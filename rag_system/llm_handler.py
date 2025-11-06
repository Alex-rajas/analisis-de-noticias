import os
import json
from pydantic import BaseModel, Field
from typing import List

# --- Importaciones de Gemini ---
from google import genai
from google.genai import types

# --- Importaciones de Claude ---
from anthropic import Anthropic, APIStatusError

# ==============================================================================
# 1. ESQUEMA DE DATOS (PYDANTIC)
# Define la estructura JSON que la IA DEBE devolver.
# ==============================================================================

class FinancialSentimentAnalysis(BaseModel):
    """
    Estructura para el análisis de sentimiento y relevancia de una noticia
    financiera para un modelo de predicción bursátil.
    """
    
    stock_ticker: str = Field(
        description="Ticker bursátil principal (ej. SAN, BBVA, IBE) o 'IBEX35' si es macro."
    )
    
    sentiment_score: float = Field(
        description="Puntuación numérica del sentimiento entre -1.0 (Muy Negativo) y +1.0 (Muy Positivo)."
    )
    
    relevance_score: int = Field(
        description="Puntuación de impacto en el precio: 1 (Bajo), 2 (Medio), 3 (Alto). Noticias sobre resultados son 3."
    )
    
    topic_category: str = Field(
        description="Categoría temática: 'Earnings' (Resultados), 'M&A' (Fusiones/Adquisiciones), 'Regulation' (Regulación/Política), 'Macro' (Economía General), 'Product' (Lanzamiento/Innovación), 'Other'."
    )
    
    reasoning: str = Field(
        description="Justificación concisa del análisis de sentimiento y la relevancia."
    )
    
    secondary_tickers: List[str] = Field(
        default=[],
        description="Lista de otros Tickers mencionados que podrían verse afectados (ej. la competencia)."
    )


# ==============================================================================
# 2. AGENTE DE ANÁLISIS DE GEMINI
# ==============================================================================

class QuantAnalysisAgent:
    
    def __init__(self, api_key: str):
        """Inicializa el cliente de Gemini y define el modelo."""
        try:
            self.client = genai.Client(api_key=api_key)
        except Exception as e:
            raise ValueError(f"Error al inicializar el cliente de Gemini. ¿API Key válida? {e}")
            
        # Usamos Flash por su velocidad y bajo coste, ideal para el alto volumen
        self.model_name = 'gemini-2.5-flash' 
        
        # El Prompt de Sistema (System Instruction)
        self.system_prompt = (
            "Eres un analista cuantitativo (Quant) de alto nivel, experto en Finanzas Bursátiles Españolas y en el IBEX 35. "
            "Tu única tarea es analizar artículos de noticias financieras y transformarlos en métricas de sentimiento "
            "y relevancia para alimentar un modelo de predicción de series temporales. "
            "Tu análisis debe ser objetivo, rápido y centrado en el impacto en el precio de las acciones. "
            "Debes identificar los Tickers bursátiles con precisión. "
            "Debes devolver **SOLO** el objeto JSON que cumpla el esquema Pydantic proporcionado."
        )

    def analyze_article_for_quant(self, article_text: str) -> str: # Devuelve un JSON string
        """
        Envía el texto del artículo a Gemini y lo obliga a devolver el JSON
        de análisis estructurado.
        """
        
        # Configuración para forzar la salida JSON estructurada
        config = types.GenerateContentConfig(
            system_instruction=self.system_prompt,
            response_mime_type="application/json",
            response_schema=FinancialSentimentAnalysis,
        )
        
        # El Prompt de Usuario (instrucción de ejecución)
        user_prompt = f"""
        Analiza el siguiente artículo financiero. Identifica el Ticker principal afectado, 
        genera una puntuación de sentimiento entre -1.0 y 1.0, y una puntuación de 
        relevancia entre 1 y 3. 
        
        Artículo: 
        ---
        {article_text}
        ---
        """
        
        try:
            response = self.client.models.generate_content(
                model=self.model_name,
                contents=[user_prompt],
                config=config
            )
            
            # Gemini devuelve un JSON string en response.text
            return response.text 
            
        except Exception as e:
            print(f"Error al llamar a la API de Gemini: {e}")
            return json.dumps({"error": str(e), "stock_ticker": "ERROR"})


# ==============================================================================
# 3. AGENTE DE ANÁLISIS DE CLAUDE
# ==============================================================================

class ClaudeQuantAnalysisAgent:
    
    def __init__(self, api_key: str):
        """Inicializa el cliente de Claude con la clave de API."""
        try:
            self.client = Anthropic(api_key=api_key)
        except Exception as e:
            raise ValueError(f"Error al inicializar el cliente de Claude. ¿API Key válida? {e}")
            
        self.model_name = 'claude-3-haiku-20240307' 
        
        # El Prompt de Sistema (System Instruction)
        self.system_prompt = (
            "Eres un analista cuantitativo (Quant) de alto nivel, experto en Finanzas Bursátiles Españolas y en el IBEX 35. "
            "Tu única tarea es analizar artículos de noticias financieras y transformarlos en métricas de sentimiento "
            "y relevancia para alimentar un modelo de predicción de series temporales. "
            "Debes utilizar la Herramienta (Tool) 'FinancialSentimentAnalysis' para devolver **SOLO** el objeto JSON estructurado."
        )

    def analyze_article_for_quant(self, article_text: str) -> dict: # Devuelve un dict
        """
        Envía el texto del artículo a Claude y lo obliga a devolver el JSON
        de análisis estructurado mediante el uso de la Herramienta.
        """
        
        # 1. Definir la herramienta usando el schema de Pydantic
        tool_schema = FinancialSentimentAnalysis.model_json_schema()

        # 2. El Prompt de Usuario (la instrucción de ejecución)
        user_prompt = f"""
        Analiza el siguiente artículo financiero. Identifica el Ticker principal afectado, 
        genera una puntuación de sentimiento entre -1.0 y 1.0, y una puntuación de 
        relevancia entre 1 y 3. 
        
        Artículo: 
        ---
        {article_text}
        ---
        """
        
        try:
            # 3. Llamada a la API de Claude
            response = self.client.messages.create(
                model=self.model_name,
                system=self.system_prompt,
                messages=[{"role": "user", "content": user_prompt}],
                tools=[{"name": "FinancialSentimentAnalysis", "description": "Analiza el artículo y devuelve métricas predictivas.", "input_schema": tool_schema}],
                max_tokens=4096,
            )
            
            # 4. Procesar la respuesta (esperamos un call a la herramienta)
            if response.tool_calls:
                # El resultado estructurado está en el primer tool_call
                tool_output = response.tool_calls[0].input
                # El output es un diccionario, lo devolvemos
                return tool_output
            else:
                return {"error": "Claude no generó un Tool Call estructurado.", "stock_ticker": "ERROR"}

        except APIStatusError as e:
            print(f"Error de API de Claude: {e.status_code} - {e.response.text}")
            return {"error": f"API Error: {e.status_code}", "stock_ticker": "ERROR"}
        except Exception as e:
            print(f"Error desconocido: {e}")
            return {"error": str(e), "stock_ticker": "ERROR"}