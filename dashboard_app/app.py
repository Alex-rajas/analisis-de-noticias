import os
import sys
import pandas as pd
import yfinance as yf
import streamlit as st
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from dotenv import load_dotenv

# --- [IMPORTANTE] Hack de Importación ---
# Añadimos la carpeta raíz al path de Python para que
# la app (en 'dashboard_app/') pueda encontrar los módulos
# en 'core/' (como el SupabaseHandler).
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
# -------------------------------------

from core.supabase_handler import SupabaseHandler

# --- Configuración de la Página ---
st.set_page_config(
    page_title="Dashboard Quant de Sentimiento",
    page_icon="📈",
    layout="wide"
)

# --- Carga de Datos (Cacheada) ---

# Usamos @st.cache_data para que no tenga que descargar los datos
# de Supabase cada vez que movemos un slider.
@st.cache_data(ttl=600) # Cache de 10 minutos
def load_sentiment_data_from_db():
    """Conecta a Supabase y descarga TODOS los artículos analizados."""
    print("Cargando datos de sentimiento desde Supabase...")
    db_handler = SupabaseHandler()
    data = db_handler.get_analyzed_sentiment_data()
    if not data:
        st.error("No se pudieron cargar datos de Supabase. ¿El Orquestador ha funcionado?")
        return pd.DataFrame()
    
    df = pd.DataFrame(data)
    
    # NORMALIZAR FECHA (Crucial para el merge)
    # Convertir a datetime, normalizar a medianoche Y BORRAR ZONA HORARIA
    df['date'] = pd.to_datetime(df['published_at']).dt.normalize().dt.tz_localize(None)
    
    # Convertir score a numérico por si acaso
    df['sentiment_score'] = pd.to_numeric(df['sentiment_score'])
    df['relevance_score'] = pd.to_numeric(df['relevance_score'])
    
    return df

@st.cache_data(ttl=600)
def load_price_data(ticker, start_date, end_date):
    """Descarga datos de precios de yfinance Y LOS LIMPIA."""
    print(f"Descargando datos de precios para {ticker}...")
    ticker_mc = f"{ticker}.MC"
    data = yf.download([ticker_mc], start=start_date, end=end_date)
    
    if data.empty:
        st.error(f"No se pudieron descargar datos de precios para {ticker_mc}")
        return pd.DataFrame()

    # APLANAR EL MULTIINDEX (La corrección que hicimos)
    data = data[['Open', 'Close', 'Volume']]
    data.columns = data.columns.droplevel(1) 
    
    # NORMALIZAR ÍNDICE (Crucial para el merge)
    data.index = pd.to_datetime(data.index).normalize()
    data.index.name = 'date'
    
    return data

# --- Lógica de la Aplicación ---

# Cargar el .env (necesario para SupabaseHandler)
load_dotenv()

st.title("📈 Dashboard de Sentimiento de Noticias vs. Precio")
st.markdown("Visualización del impacto del sentimiento de las noticias (extraído por IA) en el precio de las acciones del IBEX 35.")

# 1. Cargar datos de sentimiento
sentiment_data = load_sentiment_data_from_db()

if not sentiment_data.empty:
    
    # 2. Sidebar de Filtros
    st.sidebar.header("Filtros del Dashboard")
    
    # Obtener lista de tickers únicos que SÍ tenemos en la DB
    available_tickers = sentiment_data['stock_ticker'].unique()
    
    # Selector de Ticker
    selected_ticker = st.sidebar.selectbox(
        "Selecciona un Ticker:",
        options=available_tickers,
        help="Solo se muestran Tickers que la IA ha encontrado en las noticias analizadas."
    )
    
    # 3. Filtrar y Cargar Datos de Precios
    df_sentiment_filtered = sentiment_data[sentiment_data['stock_ticker'] == selected_ticker].copy()
    
    # Definir rango de fechas basado en las noticias
    start_date = df_sentiment_filtered['date'].min() - pd.Timedelta(days=1)
    end_date = df_sentiment_filtered['date'].max() + pd.Timedelta(days=1)
    
    df_prices = load_price_data(selected_ticker, start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'))
    
    if not df_prices.empty:
        
        # 4. Lógica Quant (Agregación)
        df_sentiment_filtered['sentiment_weighted'] = df_sentiment_filtered['sentiment_score'] * df_sentiment_filtered['relevance_score']
        daily_sentiment = df_sentiment_filtered.groupby('date').agg(
            sentiment_sum=pd.NamedAgg(column='sentiment_weighted', aggfunc='sum'),
            news_count=pd.NamedAgg(column='url', aggfunc='count')
        )
        
        # 5. Mergear los datos
        df_merged = pd.merge(df_prices, daily_sentiment, left_index=True, right_index=True, how='left')
        df_merged['sentiment_sum'] = df_merged['sentiment_sum'].fillna(0)
        
        # 6. Crear el Gráfico (Plotly)
        st.header(f"Análisis de Sentimiento para: {selected_ticker}")
        
        fig = make_subplots(specs=[[{"secondary_y": True}]])

        # Trazo 1: Precio de Cierre (Eje Y Izquierdo)
        fig.add_trace(
            go.Scatter(
                x=df_merged.index, 
                y=df_merged['Close'], 
                name=f"Precio Cierre ({selected_ticker})",
                line=dict(color="#1f77b4") # Azul
            ),
            secondary_y=False,
        )

        # Trazo 2: Sentimiento (Eje Y Derecho)
        fig.add_trace(
            go.Bar(
                x=df_merged.index, 
                y=df_merged['sentiment_sum'], 
                name="Sentimiento Agregado (Ponderado)",
                marker=dict(color="#ff7f0e") # Naranja
            ),
            secondary_y=True,
        )
        
        # Configurar ejes
        fig.update_layout(
            title_text=f"Precio de Cierre vs. Sentimiento de Noticias ({selected_ticker})",
            xaxis_title="Fecha",
            yaxis_title="Precio de Cierre (EUR)",
            yaxis2_title="Puntuación de Sentimiento"
        )
        
        # Mostrar el gráfico
        st.plotly_chart(fig, use_container_width=True)

        # 7. Mostrar Datos Crudos (Trazabilidad)
        st.subheader("Noticias Analizadas (Datos Crudos)")
        st.markdown("Estas son las noticias que la IA ha utilizado para generar los *features* de sentimiento.")
        
        # Columnas a mostrar
        cols_to_show = ['published_at', 'title', 'source', 'sentiment_score', 'relevance_score', 'reasoning', 'url']
        st.dataframe(
            df_sentiment_filtered[cols_to_show].sort_values(by='published_at', ascending=False),
            use_container_width=True
        )

else:
    st.warning("No hay datos de sentimiento analizados en la base de datos. Ejecuta `core/orchestrator.py` primero.")