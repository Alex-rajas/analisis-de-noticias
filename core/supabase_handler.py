import os
from supabase import create_client, Client
from typing import Dict, Any, Optional

# (Usamos la sintaxis moderna de typing de Python 3.10)

STORAGE_BUCKET = os.getenv("SUPABASE_STORAGE_BUCKET", "article_texts")
TABLE_NAME = "news_articles"

class SupabaseHandler:
    
    def __init__(self):
        """Inicializa el cliente de Supabase."""
        
        # CORRECCIÓN: Cargar variables aquí dentro para asegurar que .env esté cargado
        self.supabase_url = os.getenv("SUPABASE_URL")
        self.supabase_key = os.getenv("SUPABASE_KEY")

        if not self.supabase_url or not self.supabase_key:
             raise ValueError("Las claves de Supabase no están configuradas en .env o no se cargaron a tiempo.")
        
        self.supabase: Client = create_client(self.supabase_url, self.supabase_key)
        print("-> Conexión a Supabase establecida.")

    def upload_article_text(self, article_id: str, text_content: str) -> str | None:
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
            # --- CORRECCIÓN BUG 409 ---
            error_str = str(e)
            
            # Si es un error de Duplicado (409), ¡es un ÉXITO!
            # Devolvemos el path para que el orquestador continúe.
            if "Duplicate" in error_str or "409" in error_str:
                print(f"   [STORAGE INFO] El texto ya existe (409 Duplicate). Usando path: {file_path}")
                return file_path # Devolvemos el path
            else:
                # Si es un error real (403, 500, etc.), es un fallo.
                print(f"   [STORAGE ERROR] Fallo al subir el texto: {error_str}")
                return None
            # --- FIN DE LA CORRECCIÓN ---

    def insert_article_metadata(self, metadata: dict[str, any], storage_path: str) -> str | None:
        """Inserta los metadatos del artículo en la tabla PostgreSQL."""
        metadata['storage_path'] = storage_path
        
        try:
            response = self.supabase.table(TABLE_NAME).insert(metadata).execute()
            
            if response.data:
                article_uuid = response.data[0]['id']
                print(f"   [DB OK] Metadatos insertados. UUID: {article_uuid}")
                return article_uuid
            
        except Exception as e:
            error_msg = str(e)
            if "duplicate key value violates unique constraint" in error_msg:
                print("   [DB SKIP] Artículo ya existe (URL duplicada). Saltando.")
                return None
            else:
                print(f"   [DB ERROR] Fallo al insertar metadatos: {error_msg}")
                return None

    # --- Funciones para Fase 2 (Análisis) y Fase 4 (Modelo) ---

    def get_articles_needing_analysis(self, limit: int = 10) -> list[dict[str, any]]:
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

    def download_article_text(self, storage_path: str) -> str | None:
        """Descarga el texto completo de un artículo desde Supabase Storage."""
        try:
            file_content_bytes = self.supabase.storage.from_(STORAGE_BUCKET)\
                .download(storage_path)
            
            file_content_str = file_content_bytes.decode('utf-8')
            print(f"   [STORAGE OK] Texto descargado desde: {storage_path}")
            return file_content_str
            
        except Exception as e:
            print(f"   [STORAGE ERROR] No se pudo descargar {storage_path}: {e}")
            return None

    def update_article_with_analysis(self, article_id: str, analysis_data: dict[str, any]):
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

    # --- Función para Fase 5 (Dashboard) ---
            
    def get_analyzed_sentiment_data(self) -> list[dict[str, any]]:
        """
        Descarga todos los datos de sentimiento analizados para el modelo de ML
        y el Dashboard.
        """
        try:
            # CORRECCIÓN: Añadidas 'title', 'source', 'reasoning' para el dashboard
            response = self.supabase.table(TABLE_NAME)\
                .select("published_at, stock_ticker, sentiment_score, relevance_score, url, title, source, reasoning")\
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