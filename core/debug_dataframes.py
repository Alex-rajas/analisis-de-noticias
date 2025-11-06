import os
import pandas as pd
import yfinance as yf
from dotenv import load_dotenv

# Importar nuestro handler de Supabase (con la importación relativa)
from .supabase_handler import SupabaseHandler

# --- Copia exacta de las funciones de 'prediction_model.py' ---

def load_price_data(ticker, start_date, end_date):
    """Descarga datos de precios de Yahoo Finance."""
    ticker_mc = f"{ticker}.MC" 
    print(f"Descargando datos de precios para {ticker_mc}...")
    data = yf.download(ticker_mc, start=start_date, end=end_date)
    if data.empty:
        raise ValueError(f"No se pudieron descargar datos para {ticker_mc}")
    
    # Esta es la versión que queremos depurar (la última que probamos)
    print("Datos de precios cargados.")
    return data

def load_sentiment_data(db_handler, ticker):
    """Carga datos de sentimiento de Supabase y los filtra/agrega."""
    print("Cargando datos de sentimiento...")
    all_sentiment_data = db_handler.get_analyzed_sentiment_data()
    if not all_sentiment_data:
        raise ValueError("No hay datos de sentimiento en Supabase.")
        
    df = pd.DataFrame(all_sentiment_data)
    df['date'] = pd.to_datetime(df['published_at']).dt.normalize()
    df_ticker = df[df['stock_ticker'] == ticker].copy()
    
    if df_ticker.empty:
        raise ValueError(f"No se encontraron datos de sentimiento para el ticker {ticker}")

    df_ticker['sentiment_weighted'] = df_ticker['sentiment_score'] * df_ticker['relevance_score']
    
    # Esta es la versión que queremos depurar (la última que probamos)
    daily_sentiment = df_ticker.groupby('date').agg(
        sentiment_sum=pd.NamedAgg(column='sentiment_weighted', aggfunc='sum'),
        news_count=pd.NamedAgg(column='url', aggfunc='count')
    )
    print("Datos de sentimiento cargados y agregados.")
    return daily_sentiment

# --- Punto de Entrada Principal ---
if __name__ == "__main__":
    load_dotenv() # Le decimos que suba un nivel para encontrar .env
    
    # --- Configuración ---
    TICKER_TO_ANALYZE = "BBVA"
    START_DATE = "2023-01-01"
    END_DATE = "2025-11-05"
    
    try:
        print("Inicializando Supabase...")
        db_handler = SupabaseHandler()
        
        # 1. Generar DataFrame de Precios
        prices_df = load_price_data(TICKER_TO_ANALYZE, START_DATE, END_DATE)
        
        # 2. Generar DataFrame de Sentimiento
        sentiment_df = load_sentiment_data(db_handler, TICKER_TO_ANALYZE)

        # 3. Exportar a CSV
        # Guardamos los archivos en la carpeta raíz para encontrarlos fácil
        prices_file = "prices_df.csv"
        sentiment_file = "sentiment_df.csv"
        
        prices_df.to_csv(prices_file)
        sentiment_df.to_csv(sentiment_file)
        
        print("\n--- ¡ÉXITO! ---")
        print(f"Archivos guardados en la carpeta raíz del proyecto:")
        print(f"1. {prices_file} (DataFrame de Precios)")
        print(f"2. {sentiment_file} (DataFrame de Sentimiento)")

    except Exception as e:
        print(f"\n--- ERROR ---")
        print(f"Ocurrió un error: {e}")