import os
import requests
import time
from bs4 import BeautifulSoup
import feedparser
from typing import List, Dict, Optional
from datetime import datetime
import pytz

# --- Importaciones de Selenium ---
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options

USER_AGENT = os.getenv("USER_AGENT", "Mozilla/5.0 (Custom News Agent)")
DRIVER_PATH = os.path.abspath('chromedriver.exe') 

# Lista de fuentes que REQUIEREN un navegador (Selenium)
SELENIUM_SOURCES = ["Bloomberg", "ABC", "ElEconomista", "ElMundoFinanciero"]

class NewsArticle:
    # ... (Esta clase no cambia) ...
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
        self._selenium_driver = None 

    @property
    def selenium_driver(self):
        """Inicializa el driver de Selenium solo cuando se necesita."""
        if self._selenium_driver is None:
            print(f"   [Selenium] Inicializando navegador virtual para {self.source_name}...")
            service = Service(executable_path=DRIVER_PATH)
            options = Options()
            options.add_argument("--headless")
            options.add_argument("--disable-gpu")
            options.add_argument(f"--user-agent={USER_AGENT}")
            options.add_argument("--log-level=3")
            options.add_argument('--ignore-certificate-errors')
            options.add_argument('--disable-extensions')
            options.add_argument('--no-sandbox')
            options.add_argument('--disable-dev-shm-usage')
            
            try:
                self._selenium_driver = webdriver.Chrome(service=service, options=options)
            except Exception as e:
                print(f"   [SELENIUM ERROR] ¿Has descargado 'chromedriver.exe' y lo has puesto en la raíz?")
                print(f"   Error: {e}")
                raise e
        return self._selenium_driver

    def close_driver(self):
        """Método explícito para cerrar el driver."""
        if self._selenium_driver:
            print(f"   [Selenium] Cerrando navegador virtual para {self.source_name}.")
            self._selenium_driver.quit()
            self._selenium_driver = None

    def fetch_rss_links(self) -> list[dict[str, any]]:
        # ... (Esta función no cambia) ...
        print(f"-> Buscando nuevos artículos en {self.source_name}...")
        try:
            feed = feedparser.parse(self.rss_url)
            articles = []
            for entry in feed.entries:
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
        except Exception as e:
            print(f"   [ERROR RSS] No se pudo leer el feed para {self.source_name}: {e}")
            return []

    def _get_content_with_selenium(self, url: str) -> Optional[str]:
        # ... (Esta función no cambia) ...
        try:
            print(f"   [Selenium] Navegando a {url}...")
            self.selenium_driver.get(url)
            time.sleep(3) 
            html_content = self.selenium_driver.page_source
            soup = BeautifulSoup(html_content, 'lxml')
            return self._extract_text_from_html(soup)
        except Exception as e:
            print(f"   [SELENIUM ERROR] Fallo al descargar {url}: {e}")
            return None

    def _extract_text_from_html(self, soup: BeautifulSoup) -> str:
        """Lógica de extracción de texto, especializada por fuente (Selectores Finales)."""
        main_content = None
        selector = "Genérico (fallback)"
        
        try:
            if self.source_name == "CincoDías":
                selector = "div.article-body"
                main_content = soup.find("div", class_="article-body")
            
            elif self.source_name == "ElEconomista":
                selector = "div[itemprop='articleBody']" 
                main_content = soup.find("div", itemprop="articleBody")

            elif self.source_name == "ABC":
                # --- ¡AQUÍ ESTÁ TU CAMBIO! ---
                # Probamos el selector que encontraste.
                selector = "div[data-voc-component='voc-d']" 
                main_content = soup.find("div", attrs={"data-voc-component": "voc-d"})
                # --- FIN DEL CAMBIO ---

            elif self.source_name == "ElMundoFinanciero":
                selector = "div.td-post-content" 
                main_content = soup.find("div", class_="td-post-content")
            
            elif self.source_name == "Bloomberg":
                selector = "div[class*='body-content']" 
                main_content = soup.find("div", class_=lambda x: x and 'body-content' in x)
            
            # (Expansión funciona bien con el genérico)
            # --- Fin de Selectores ---

        except Exception as e:
            print(f"   [Selector WARN] Falló el selector especializado '{selector}': {e}")
            main_content = None

        if main_content is None:
            main_content = soup.find("body")
            if main_content is None:
                return ""
        
        paragraphs = main_content.find_all(['p', 'h1', 'h2'])
        text_content = "\n".join([p.get_text(separator=' ', strip=True) for p in paragraphs])
        
        return text_content

    def get_article_content(self, url: str) -> str | None:
        # ... (Esta función no cambia) ...
        if self.source_name in SELENIUM_SOURCES:
            return self._get_content_with_selenium(url)
        
        try:
            headers = {'User-Agent': USER_AGENT}
            response = requests.get(url, headers=headers, timeout=15)
            response.raise_for_status() 
            soup = BeautifulSoup(response.content, 'lxml')
            text_content = self._extract_text_from_html(soup)
            
            if len(text_content) < 250: 
                 print(f"   [ADVERTENCIA] Texto muy corto o no encontrado. Descartando: {url}")
                 return None

            print(f"   [OK] Texto extraído con {len(text_content)} caracteres.")
            return text_content
            
        except requests.exceptions.RequestException as e:
            # ... (Manejo de errores no cambia) ...
            if e.response and e.response.status_code == 404:
                print(f"   [ERROR 404] Artículo no encontrado: {url}")
            else:
                print(f"   [ERROR HTTP] Fallo al descargar {url}: {e}")
            return None
        except Exception as e:
            print(f"   [ERROR DESCONOCIDO] Fallo en el scraping de {url}: {e}")
            return None