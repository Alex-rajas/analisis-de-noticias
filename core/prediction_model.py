import os
import pandas as pd
import numpy as np
import yfinance as yf
from dotenv import load_dotenv
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score

# Importar nuestro handler de Supabase
from .supabase_handler import SupabaseHandler

def load_price_data(ticker, start_date, end_date):
    """Descarga datos de precios de Yahoo Finance."""
    # Descargar usando una LISTA de tickers (aunque solo sea uno)
    # Esto fuerza a yfinance a devolver un MultiIndex predecible
    ticker_list = [f"{ticker}.MC"] 
    print(f"Descargando datos de precios para {ticker_list}...")
    data = yf.download(ticker_list, start=start_date, end=end_date)
    
    if data.empty:
        raise ValueError(f"No se pudieron descargar datos para {ticker_list}")
    
    # --- INICIO DE LA CORRECCIÓN ---
    # 1. Seleccionar solo las columnas que necesitamos (esto mantiene el MultiIndex)
    # ej. [('Open', 'BBVA.MC'), ('Close', 'BBVA.MC'), ...]
    data = data[['Open', 'Close', 'Volume']]

    # 2. APLANAR EL MULTIINDEX (EL PASO CLAVE)
    # Quita el segundo nivel de la cabecera (ej. 'BBVA.MC')
    # y deja solo el primero (ej. 'Open', 'Close', 'Volume')
    data.columns = data.columns.droplevel(1) 
    
    # 3. NORMALIZAR EL ÍNDICE (Fecha):
    data.index = pd.to_datetime(data.index).normalize()
    
    # 4. Renombrar el índice para que coincida
    data.index.name = 'date'
    # --- FIN DE LA CORRECCIÓN ---
    
    print("Datos de precios cargados y normalizados (1 Nivel).")
    return data

def load_sentiment_data(db_handler, ticker):
    """Carga datos de sentimiento de Supabase y los filtra/agrega."""
    all_sentiment_data = db_handler.get_analyzed_sentiment_data()
    if not all_sentiment_data:
        raise ValueError("No hay datos de sentimiento en Supabase.")
        
    df = pd.DataFrame(all_sentiment_data)
    
    # Convertir a datetime, normalizar Y LUEGO BORRAR LA ZONA HORARIA
    df['date'] = pd.to_datetime(df['published_at']).dt.normalize().dt.tz_localize(None)
    
    df_ticker = df[df['stock_ticker'] == ticker].copy()
    if df_ticker.empty:
        raise ValueError(f"No se encontraron datos de sentimiento para el ticker {ticker}")

    df_ticker['sentiment_weighted'] = df_ticker['sentiment_score'] * df_ticker['relevance_score']
    
    # Agrupar por 'date' y hacer que 'date' sea el ÍNDICE (Correcto)
    daily_sentiment = df_ticker.groupby('date').agg(
        sentiment_sum=pd.NamedAgg(column='sentiment_weighted', aggfunc='sum'),
        news_count=pd.NamedAgg(column='url', aggfunc='count')
    )
    
    print("Datos de sentimiento cargados y agregados (1 Nivel).")
    return daily_sentiment

def create_features_and_target(prices_df, sentiment_df):
    """
    Combina precios y sentimiento, y crea la variable objetivo (target).
    """
    # Unir usando los índices. Ahora AMBOS son simples y normalizados
    df_merged = pd.merge(prices_df, sentiment_df, left_index=True, right_index=True, how='left')

    # Rellenar días sin noticias con 0 (sin sentimiento)
    df_merged['sentiment_sum'] = df_merged['sentiment_sum'].fillna(0)
    df_merged['news_count'] = df_merged['news_count'].fillna(0)

    # 2. **TARGET (Variable Objetivo)**: ¿Subió el precio mañana?
    df_merged['future_close'] = df_merged['Close'].shift(-1)
    df_merged['target'] = (df_merged['future_close'] > df_merged['Close']).astype(int)
    
    # Eliminar la última fila (no tiene 'target' ni 'future_close')
    df_final = df_merged.dropna()

    return df_final

def train_model(df):
    """Entrena un modelo simple de ML."""
    print("Entrenando modelo...")
    
    features = ['sentiment_sum', 'news_count', 'Volume']
    X = df[features]
    y = df['target']
    
    if X.empty or y.empty:
        print("No hay suficientes datos alineados para entrenar.")
        return

    # Dividir datos (NO barajar para series temporales)
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, shuffle=False)

    if X_test.empty:
        print("El conjunto de Test está vacío. No hay suficientes datos para una división 80/20.")
        return

    model = RandomForestClassifier(n_estimators=100, random_state=42)
    model.fit(X_train, y_train)

    preds = model.predict(X_test)
    accuracy = accuracy_score(y_test, preds)
    
    print(f"\n--- RESULTADO DEL MODELO ---")
    print(f"Ticker Analizado: {TICKER_TO_ANALYZE}")
    print(f"Precisión (Accuracy) del modelo: {accuracy * 100:.2f}%")
    print(f" (Comparar con un 50% de línea base - lanzar una moneda)")

# --- Punto de Entrada Principal ---
if __name__ == "__main__":
    load_dotenv()
    
    TICKER_TO_ANALYZE = "BBVA"
    START_DATE = "2023-01-01"  
    END_DATE = "2025-11-06" # Actualizado a la fecha de hoy
    
    try:
        db_handler = SupabaseHandler()
        prices_df = load_price_data(TICKER_TO_ANALYZE, START_DATE, END_DATE)
        sentiment_df = load_sentiment_data(db_handler, TICKER_TO_ANALYZE)
        df_final = create_features_and_target(prices_df, sentiment_df)
        train_model(df_final)

    except ValueError as e:
        print(f"\nERROR: {e}")
    except Exception as e:
        print(f"\nError inesperado: {e}")