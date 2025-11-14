import os
import hashlib
import json
import time # (Opcional, para pausas)
from dotenv import load_dotenv

# 1. Cargar .env ANTES de importar otros módulos que lo usen
load_dotenv()

# 2. Importar nuestros módulos
from agents.scrapers.news_agent import NewsAgent
from core.supabase_handler import SupabaseHandler

# 3. Importar los Agentes Quant (IA)
from rag_system.llm_handler import QuantAnalysisAgent # (Gemini)
# from rag_system.llm_handler import ClaudeQuantAnalysisAgent as QuantAnalysisAgent # (Claude)


# --- Configuración del Entorno ---
RSS_FEEDS_STR = os.getenv("RSS_FEEDS")
SOURCE_NAMES_STR = os.getenv("SOURCE_NAMES")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
# CLAUDE_API_KEY = os.getenv("CLAUDE_API_KEY")


def run_data_collection(agent: NewsAgent, db: SupabaseHandler):
    """
    FASE 1: Recolecta nuevos artículos (RSS -> Scrape -> Storage -> DB)
    (Esta función no cambia)
    """
    
    article_links = agent.fetch_rss_links()
    total_inserted = 0

    for link_data in article_links:
        print(f"\n Procesando: {link_data['title'][:50]}...")
        
        text_content = agent.get_article_content(link_data['url'])
        if text_content is None:
            continue
        
        file_id = hashlib.sha256(link_data['url'].encode()).hexdigest()
        storage_path = db.upload_article_text(file_id, text_content)

        if storage_path:
            metadata = {
                'title': link_data['title'],
                'url': link_data['url'],
                'source': link_data['source'],
                'published_at': link_data['published_at'].isoformat(),
            }
            
            uuid = db.insert_article_metadata(metadata, storage_path)
            if uuid:
                total_inserted += 1
    
    return total_inserted


def run_sentiment_analysis(agent_ia: QuantAnalysisAgent, db: SupabaseHandler, batch_size: int = 50):
    """
    FASE 2: Analiza artículos pendientes (DB -> Storage -> IA -> DB Update)
    Se ejecuta en un bucle 'while' hasta que no queden artículos
    con 'sentiment_score' en NULL, procesando en lotes de 'batch_size'.
    """
    print(f"\n--- [FASE 2: ANÁLISIS QUANT (IA) POR LOTES DE {batch_size}] ---")
    
    total_articulos_analizados_en_esta_ejecucion = 0

    # Bucle principal: sigue hasta que no queden artículos
    while True: 
        
        # 2a. DB (Buscar el siguiente lote de artículos)
        print(f"\nBuscando siguiente lote de {batch_size} artículos para analizar...")
        articles_to_process = db.get_articles_needing_analysis(limit=batch_size)
        
        if not articles_to_process:
            # Si no devuelve artículos, la cola está vacía. Terminamos.
            print("No hay más artículos nuevos para analizar.")
            break # Sale del bucle 'while True'

        print(f"Lote encontrado. Procesando {len(articles_to_process)} artículos...")
        
        for article in articles_to_process:
            article_id = article['id']
            storage_path = article['storage_path']
            
            print(f"\n Analizando Artículo ID: {article_id}...")
            
            text_content = db.download_article_text(storage_path)
            if text_content is None:
                print(f"   [ERROR] No se pudo descargar el texto. Saltando artículo {article_id}.")
                continue
                
            print("   Llamando a la API del LLM (Gemini/Claude)...")
            
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
            
            db.update_article_with_analysis(article_id, analysis_data)
            total_articulos_analizados_en_esta_ejecucion += 1
            
            # (Opcional) Añadir una pequeña pausa para no saturar la API
            # time.sleep(0.5) # Pausa de medio segundo entre llamadas

    print(f"\n--- [FASE 2: COMPLETADA] ---")
    print(f"Total de artículos analizados en esta ejecución: {total_articulos_analizados_en_esta_ejecucion}")


# --- Punto de Entrada Principal (Refactorizado) ---
if __name__ == "__main__":
    
    # 1. Inicializar Handlers
    try:
        db_handler = SupabaseHandler()
        ia_agent = QuantAnalysisAgent(api_key=GEMINI_API_KEY)
        # ia_agent = QuantAnalysisAgent(api_key=CLAUDE_API_KEY) # Si usas la clase de Claude
    
    except ValueError as e:
        print(f"Error de Configuración (revisa tu .env): {e}")
        exit(1)
    except Exception as e:
        print(f"Error al inicializar: {e}")
        exit(1)

    # 2. Ejecutar FASE 1 (Recolección) en Bucle
    print("\n--- [INICIANDO FASE 1: RECOLECCIÓN MULTI-FUENTE] ---")
    
    if not RSS_FEEDS_STR or not SOURCE_NAMES_STR:
        print("ERROR: RSS_FEEDS o SOURCE_NAMES no están configurados en .env.")
        exit(1)
        
    feeds_list = [feed.strip() for feed in RSS_FEEDS_STR.split(',')]
    names_list = [name.strip() for name in SOURCE_NAMES_STR.split(',')]
    
    if len(feeds_list) != len(names_list):
        print("ERROR: El número de RSS_FEEDS no coincide con el número de SOURCE_NAMES en .env.")
        exit(1)

    total_articulos_insertados = 0
    
    for url, name in zip(feeds_list, names_list):
        print(f"\n--- Procesando Fuente: {name} ---")
        try:
            news_agent = NewsAgent(rss_url=url, source_name=name)
            inserted = run_data_collection(news_agent, db_handler)
            total_articulos_insertados += inserted
        except Exception as e:
            print(f"ERROR al procesar la fuente {name}: {e}")
            continue 

    print(f"\n--- [FASE 1: COMPLETADA] ---")
    print(f"Total de artículos NUEVOS insertados (de todas las fuentes): {total_articulos_insertados}")

    # 3. Ejecutar FASE 2 (Análisis)
    # Ahora llamamos a la función con el bucle 'while' interno
    # Procesará TODOS los artículos pendientes, en lotes de 50.
    #run_sentiment_analysis(ia_agent, db_handler, batch_size=50) 

    print("\n--- [PIPELINE COMPLETADO] ---")