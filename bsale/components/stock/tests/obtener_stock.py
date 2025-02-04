import requests
import json
import os
from dotenv import load_dotenv

# Cargar variables de entorno desde .env
load_dotenv()
API_TOKEN = os.getenv("BSALE_ACCESS_TOKEN")

# Configuración de la API
BASE_URL = "https://api.bsale.io/v1/stocks.json"

def obtener_stock_por_codigo(variant_code):
    """
    Obtiene el stock de una variante específica desde la API de Bsale usando su código (SKU).
    """
    headers = {
        'Content-Type': 'application/json',
        'access_token': API_TOKEN
    }
    params = {
        'code': variant_code  # Filtrar por código de variante
    }

    try:
        print(f"Consultando stock para la variante: {variant_code}...")
        response = requests.get(BASE_URL, headers=headers, params=params, timeout=10)
        response.raise_for_status()  # Verifica si hay errores HTTP
        data = response.json()

        if "items" in data and data["items"]:
            print(json.dumps(data["items"], indent=4, ensure_ascii=False))  # Mostrar datos en formato legible
            return data["items"]
        else:
            print(f"No se encontró stock para la variante {variant_code}.")
            return None

    except requests.exceptions.HTTPError as e:
        print(f"Error HTTP al consultar la API: {e}")
    except requests.exceptions.RequestException as e:
        print(f"Error en la conexión con la API: {e}")

    return None

# Prueba con el variant code "2260AZMAXXL"
variant_code_prueba = "2260AZMAXXL"
stock_data = obtener_stock_por_codigo(variant_code_prueba)


