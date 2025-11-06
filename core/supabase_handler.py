import os
from supabase import create_client, Client
from typing import Dict, Any, Optional

# Cargar configuración desde .env
# (Hemos quitado las líneas SUPABASE_URL y SUPABASE_KEY de aquí)
STORAGE_BUCKET = os.getenv("SUPABASE_STORAGE_BUCKET", "article_texts")
TABLE_NAME = "news_articles"

class SupabaseHandler:
    
    def __init__(self):
        """Inicializa el cliente de Supabase."""
        
        # MOVEMOS LA CARGA DE VARIABLES AQUÍ DENTRO
        # Esto asegura que se ejecute DESPUÉS de que load_dotenv() haya sido llamado
        # en el script principal (orchestrator.py)
        self.supabase_url = os.getenv("SUPABASE_URL")
        self.supabase_key = os.getenv("SUPABASE_KEY")

        if not self.supabase_url or not self.supabase_key:
             raise ValueError("Las claves de Supabase no están configuradas en .env o no se cargaron a tiempo.")
        
        self.supabase: Client = create_client(self.supabase_url, self.supabase_key)
        print("-> Conexión a Supabase establecida.")

    def upload_article_text(self, article_id: str, text_content: str) -> Optional[str]:
        """Sube el texto completo del artículo al Supabase Storage."""
        file_path = f"{article_id}.txt"
        
        try:
            self.supabase.storage.from_(STORAGE_BUCKET).upload(
                file=text_content.encode('utf-8'),
                path=file_path,
                file_options={"content-type": "text/plain"}
            )
            print(f"   [STORAGE OK] Texto subido a: {file_path}")
            return file_path
        except Exception as e:
            # Manejar el caso de que el archivo ya exista (es poco probable con UUIDs)
            print(f"   [STORAGE ERROR] Fallo al subir el texto: {e}")
            return None
    # PEGA ESTAS TRES FUNCIONES DENTRO DE LA CLASE SupabaseHandler

    def get_articles_needing_analysis(self, limit: int = 10) -> list[Dict[str, Any]]:
        """
        Recupera artículos de la DB que aún no han sido analizados
        (es decir, sentiment_score es NULL).
        """
        try:
            response = self.supabase.table(TABLE_NAME)\
                .select("id, storage_path")\
                .is_("sentiment_score", "null")\
                .limit(limit)\
                .execute()
            
            if response.data:
                print(f"-> [DB SELECT] Encontrados {len(response.data)} artículos para analizar.")
                return response.data
            else:
                print("-> [DB SELECT] No hay artículos nuevos para analizar.")
                return []
                
        except Exception as e:
            print(f"-> [DB SELECT ERROR] Error al buscar artículos: {e}")
            return []

    def download_article_text(self, storage_path: str) -> Optional[str]:
        """Descarga el texto completo de un artículo desde Supabase Storage."""
        try:
            # Descarga el contenido del archivo en memoria
            file_content_bytes = self.supabase.storage.from_(STORAGE_BUCKET)\
                .download(storage_path)
            
            # Decodifica de bytes a string (UTF-8)
            file_content_str = file_content_bytes.decode('utf-8')
            print(f"   [STORAGE OK] Texto descargado desde: {storage_path}")
            return file_content_str
            
        except Exception as e:
            print(f"   [STORAGE ERROR] No se pudo descargar {storage_path}: {e}")
            return None

    def update_article_with_analysis(self, article_id: str, analysis_data: Dict[str, Any]):
        """
        Actualiza una fila en news_articles con los features generados por la IA
        (sentiment_score, stock_ticker, etc.).
        """
        try:
            response = self.supabase.table(TABLE_NAME)\
                .update(analysis_data)\
                .eq("id", article_id)\
                .execute()
            
            if response.data:
                print(f"   [DB UPDATE OK] Artículo {article_id} actualizado con análisis.")
            else:
                print(f"   [DB UPDATE WARN] No se actualizó {article_id}. ¿Quizás el ID no existe?")

        except Exception as e:
            print(f"   [DB UPDATE ERROR] Fallo al actualizar {article_id}: {e}")
    
    def get_analyzed_sentiment_data(self) -> list[dict[str, any]]:
        """
        Descarga todos los datos de sentimiento analizados para el modelo de ML.
        Filtra por artículos que SÍ tienen un sentiment_score.
        """
        try:
            response = self.supabase.table(TABLE_NAME)\
                .select("published_at, stock_ticker, sentiment_score, relevance_score, url")\
                .not_.is_("sentiment_score", "null")\
                .order("published_at", desc=False)\
                .execute()
            
            if response.data:
                print(f"-> [DB EXPORT] Descargados {len(response.data)} registros de sentimiento para el modelo.")
                return response.data
            else:
                print("-> [DB EXPORT] No se encontraron datos de sentimiento analizados.")
                return []
        except Exception as e:
            print(f"-> [DB EXPORT ERROR] Error al descargar datos: {e}")
            return []
    
    def insert_article_metadata(self, metadata: Dict[str, Any], storage_path: str) -> Optional[str]:
        """Inserta los metadatos del artículo en la tabla PostgreSQL."""
        metadata['storage_path'] = storage_path
        
        try:
            response = self.supabase.table(TABLE_NAME).insert(metadata).execute()
            
            # Supabase devuelve el registro insertado
            if response.data:
                article_uuid = response.data[0]['id']
                print(f"   [DB OK] Metadatos insertados. UUID: {article_uuid}")
                return article_uuid
            
        except Exception as e:
            # La excepción más común aquí es la violación de la restricción UNIQUE (URL duplicada)
            error_msg = str(e)
            if "duplicate key value violates unique constraint" in error_msg:
                print("   [DB SKIP] Artículo ya existe (URL duplicada). Saltando.")
                return None
            else:
                print(f"   [DB ERROR] Fallo al insertar metadatos: {error_msg}")
                return None