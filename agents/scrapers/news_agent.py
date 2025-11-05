import requests
from bs4 import BeautifulSoup
import feedparser
from typing import List, Dict, Optional
from datetime import datetime
import pytz

# Configuración (usa el mismo User-Agent definido en tu .env)
USER_AGENT = os.getenv("USER_AGENT", "Mozilla/5.0 (Custom News Agent)")

class NewsArticle:
    """Clase para estandarizar los datos extraídos de un artículo."""
    def __init__(self, title, url, source, published_at, text_content):
        self.title = title
        self.url = url
        self.source = source
        self.published_at = published_at
        self.text_content = text_content

class NewsAgent:
    
    def __init__(self, rss_url: str, source_name: str):
        self.rss_url = rss_url
        self.source_name = source_name

    def fetch_rss_links(self) -> List[Dict]:
        """Extrae titulares, URLs y fechas de un feed RSS/Atom."""
        print(f"-> Buscando nuevos artículos en {self.source_name}...")
        feed = feedparser.parse(self.rss_url)
        
        articles = []
        for entry in feed.entries:
            # Intentar estandarizar la fecha a UTC
            try:
                if entry.get('published_parsed'):
                    dt = datetime(*entry.published_parsed[:6])
                    dt = pytz.utc.localize(dt)
                else:
                    dt = pytz.utc.localize(datetime.now())
            except Exception:
                dt = pytz.utc.localize(datetime.now())

            articles.append({
                'title': entry.title,
                'url': entry.link,
                'published_at': dt,
                'source': self.source_name
            })
        print(f"-> Encontrados {len(articles)} enlaces.")
        return articles

    def get_article_content(self, url: str) -> Optional[str]:
        """Descarga el HTML y extrae el texto limpio del cuerpo del artículo."""
        try:
            headers = {'User-Agent': USER_AGENT}
            response = requests.get(url, headers=headers, timeout=15)
            response.raise_for_status() # Lanza error para códigos 4xx/5xx
            
            soup = BeautifulSoup(response.content, 'lxml')
            
            # **ESTRATEGIA DE LIMPIEZA:**
            # Esto es la parte más dependiente de la web. Aquí asumimos que
            # el contenido principal está dentro de ciertas etiquetas comunes
            # de artículos (ej. <article>, <div> con clase 'body-content').
            
            # Implementación genérica: Extraer todos los párrafos (p) y textos
            content_tags = soup.find_all(['p', 'h1', 'h2'])
            text_content = "\n".join([tag.get_text(separator=' ', strip=True) for tag in content_tags])
            
            # Limpieza básica: Asegurar que el texto tiene un mínimo de longitud
            if len(text_content) < 200:
                 print(f"   [ADVERTENCIA] Texto muy corto. Posible fallo en el scraping de: {url}")
                 return None

            print(f"   [OK] Texto extraído con {len(text_content)} caracteres.")
            return text_content
            
        except requests.exceptions.RequestException as e:
            print(f"   [ERROR] Fallo al descargar {url}: {e}")
            return None