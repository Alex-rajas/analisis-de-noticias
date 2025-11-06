import os
import hashlib
import json # <--- Asegúrate de que esta importación esté
from dotenv import load_dotenv

# 1. Cargar .env ANTES de importar otros módulos que lo usen
load_dotenv()

# 2. Importar nuestros módulos
from agents.scrapers.news_agent import NewsAgent
from core.supabase_handler import SupabaseHandler

# 3. Importar los Agentes Quant (IA)
# Elige cuál quieres usar descomentando la línea apropiada
from rag_system.llm_handler import QuantAnalysisAgent # (Gemini)
# from rag_system.llm_handler import ClaudeQuantAnalysisAgent as QuantAnalysisAgent # (Claude)


# --- Configuración del Entorno ---
NEWS_RSS_URL = os.getenv("NEWS_SOURCE_URL")
NEWS_SOURCE_NAME = "FinancialNewsTest" # Puedes mover esto al .env si quieres
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
# CLAUDE_API_KEY = os.getenv("CLAUDE_API_KEY")


def run_data_collection(agent: NewsAgent, db: SupabaseHandler):
    """
    FASE 1: Recolecta nuevos artículos (RSS -> Scrape -> Storage -> DB)
    Inserta los datos base con 'sentiment_score' en NULL.
    """
    print("\n--- [FASE 1: RECOLECCIÓN DE DATOS] ---")
    if not NEWS_RSS_URL:
        print("ERROR: NEWS_SOURCE_URL no configurada en .env.")
        return

    article_links = agent.fetch_rss_links()
    total_inserted = 0

    for link_data in article_links:
        print(f"\n Procesando: {link_data['title'][:50]}...")
        
        # 1a. Scrape (Descargar y limpiar texto)
        text_content = agent.get_article_content(link_data['url'])
        if text_content is None:
            continue
        
        # 1b. Storage (Subir texto a Supabase Storage)
        file_id = hashlib.sha256(link_data['url'].encode()).hexdigest()
        storage_path = db.upload_article_text(file_id, text_content)

        if storage_path:
            # 1c. DB (Insertar metadatos en PostgreSQL)
            metadata = {
                'title': link_data['title'],
                'url': link_data['url'],
                'source': link_data['source'],
                'published_at': link_data['published_at'].isoformat(),
                # El storage_path se añade dentro de la función insert_article_metadata
            }
            
            uuid = db.insert_article_metadata(metadata, storage_path)
            if uuid:
                total_inserted += 1

    print(f"\n--- [FASE 1: COMPLETADA] ---")
    print(f"Artículos nuevos insertados (para análisis): {total_inserted}")


def run_sentiment_analysis(agent_ia: QuantAnalysisAgent, db: SupabaseHandler, limit: int = 5):
    """
    FASE 2: Analiza artículos pendientes (DB -> Storage -> IA -> DB Update)
    Busca artículos con 'sentiment_score' en NULL y los procesa.
    """
    print(f"\n--- [FASE 2: ANÁLISIS QUANT (IA)] ---")
    
    # 2a. DB (Buscar artículos que necesitan análisis)
    articles_to_process = db.get_articles_needing_analysis(limit=limit)
    
    if not articles_to_process:
        print(f"No hay artículos nuevos para analizar.")
        print(f"--- [FASE 2: COMPLETADA] ---")
        return

    total_analyzed = 0
    for article in articles_to_process:
        article_id = article['id']
        storage_path = article['storage_path']
        
        print(f"\n Analizando Artículo ID: {article_id}...")
        
        # 2b. Storage (Descargar el texto completo)
        text_content = db.download_article_text(storage_path)
        if text_content is None:
            print(f"   [ERROR] No se pudo descargar el texto. Saltando artículo {article_id}.")
            continue
            
        # 2c. IA (Llamar al Agente Quant para análisis)
        print("   Llamando a la API del LLM (Gemini/Claude)...")
        
        # Esta lógica maneja tanto el string JSON de Gemini como el dict de Claude
        analysis_output = agent_ia.analyze_article_for_quant(text_content)
        
        analysis_data = {}
        if isinstance(analysis_output, str):
            try:
                analysis_data = json.loads(analysis_output)
            except json.JSONDecodeError:
                print(f"   [IA ERROR] La API devolvió un JSON inválido: {analysis_output}")
                continue
        elif isinstance(analysis_output, dict):
            analysis_data = analysis_output
        
        if "error" in analysis_data:
            print(f"   [IA ERROR] Fallo en el análisis: {analysis_data['error']}")
            continue
            
        print(f"   [IA OK] Sentimiento: {analysis_data.get('sentiment_score')}, Ticker: {analysis_data.get('stock_ticker')}")
        
        # 2d. DB (Actualizar la fila con los features)
        db.update_article_with_analysis(article_id, analysis_data)
        total_analyzed += 1

    print(f"\n--- [FASE 2: COMPLETADA] ---")
    print(f"Artículos analizados y actualizados: {total_analyzed}")


# --- Punto de Entrada Principal ---
if __name__ == "__main__":
    
    # 1. Inicializar Handlers
    try:
        db_handler = SupabaseHandler()
        
        # Elige el agente de IA:
        ia_agent = QuantAnalysisAgent(api_key=GEMINI_API_KEY)
        # ia_agent = QuantAnalysisAgent(api_key=CLAUDE_API_KEY) # Si usas la clase de Claude

        news_agent = NewsAgent(rss_url=NEWS_RSS_URL, source_name=NEWS_SOURCE_NAME)

    except ValueError as e:
        print(f"Error de Configuración (revisa tu .env): {e}")
        exit(1)
    except Exception as e:
        print(f"Error al inicializar: {e}")
        exit(1)

    # 2. Ejecutar las Fases del Pipeline
    
    # YA HEMOS RECOLECTADO DATOS, ASÍ QUE COMENTAMOS ESTA LÍNEA POR AHORA
    # run_data_collection(news_agent, db_handler) 
    
    # EJECUTAMOS SOLO LA FASE DE ANÁLISIS
    run_sentiment_analysis(ia_agent, db_handler, limit=70) # Procesa 10 artículos pendientes

    print("\n--- [PIPELINE COMPLETADO] ---")