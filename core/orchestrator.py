import os
from dotenv import load_dotenv

# Importar los módulos que acabamos de crear
from agents.scrapers.news_agent import NewsAgent 
from core.supabase_handler import SupabaseHandler 

# Cargar las variables de entorno al inicio del script
load_dotenv()

# Configuración de prueba desde .env
TEST_RSS_URL = os.getenv("NEWS_SOURCE_URL")
TEST_SOURCE_NAME = "FinancialNewsTest" # Nombre de la fuente para la DB

def run_data_collection():
    """Ejecuta el pipeline de recolección de datos: Scraping -> Storage -> DB."""
    
    if not TEST_RSS_URL:
        print("ERROR: La variable NEWS_SOURCE_URL no está configurada en el .env. Deteniendo el proceso.")
        return

    # Inicializar Agentes
    supabase_handler = SupabaseHandler()
    news_agent = NewsAgent(rss_url=TEST_RSS_URL, source_name=TEST_SOURCE_NAME)

    # 1. Obtener lista de URLs del RSS
    article_links = news_agent.fetch_rss_links()

    total_processed = 0
    total_inserted = 0
    
    # 2. Iterar sobre los enlaces y procesar cada artículo
    for link_data in article_links:
        
        print(f"\n Procesando: {link_data['title'][:50]}...")
        
        # 2a. Descargar y limpiar el contenido
        text_content = news_agent.get_article_content(link_data['url'])
        
        if text_content is None:
            continue
        
        total_processed += 1
        
        # 2b. Intentar insertar los metadatos (la inserción genera un UUID)
        # Usamos 'try-except' para generar un UUID solo si la inserción es exitosa.
        
        # La URL ya es un identificador único, la usamos como ID temporal para el storage
        # NOTA: Supabase genera el UUID real en la DB. Usamos un hash o la URL como nombre de archivo
        # Para esta implementación, usaremos el URL hash para el nombre del archivo de texto.
        temp_article_id = hashlib.sha256(link_data['url'].encode()).hexdigest()

        # 2c. Subir el texto completo al Storage
        storage_path = supabase_handler.upload_article_text(temp_article_id, text_content)

        if storage_path:
            # 2d. Insertar metadatos en PostgreSQL
            metadata = {
                'title': link_data['title'],
                'url': link_data['url'],
                'source': link_data['source'],
                'published_at': link_data['published_at'].isoformat(),
            }
            
            uuid = supabase_handler.insert_article_metadata(metadata, storage_path)
            
            if uuid:
                 total_inserted += 1

    print("\n--- RESUMEN ---")
    print(f"Artículos procesados: {total_processed}")
    print(f"Artículos insertados/actualizados: {total_inserted}")
    print("-----------------")

if __name__ == "__main__":
    # Necesitas importar 'hashlib' en el archivo real si usas el hash de URL
    import hashlib
    run_data_collection()