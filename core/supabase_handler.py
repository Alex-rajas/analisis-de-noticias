import os
from supabase import create_client, Client
from typing import Dict, Any, Optional

# Cargar configuración desde .env
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
STORAGE_BUCKET = os.getenv("SUPABASE_STORAGE_BUCKET", "article_texts")
TABLE_NAME = "news_articles"

class SupabaseHandler:
    
    def __init__(self):
        """Inicializa el cliente de Supabase."""
        if not SUPABASE_URL or not SUPABASE_KEY:
             raise ValueError("Las claves de Supabase no están configuradas en .env")
        self.supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
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