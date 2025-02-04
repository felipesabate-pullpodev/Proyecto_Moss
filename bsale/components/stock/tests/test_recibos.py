import os
import requests
import json
from dotenv import load_dotenv

# Cargar variables de entorno desde .env
load_dotenv()

# Token de acceso de Bsale
API_TOKEN = os.getenv("BSALE_ACCESS_TOKEN")

# Configuración de la API
BASE_URL = "https://api.bsale.io/v1/stocks/receptions.json"

def obtener_recepcion_stock():
    """
    Obtiene una única recepción de stock con detalles y oficina expandida.
    """
    headers = {'access_token': API_TOKEN}
    params = {'expand': '[office,details]', 'limit': 1}  # Solo una recepción para prueba

    try:
        print("Consultando una recepción de stock...")
        response = requests.get(BASE_URL, headers=headers, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()

        if "items" in data and data["items"]:
            return data["items"][0]  # Retornamos solo el primer elemento
        else:
            print("No se encontraron recepciones de stock.")
            return None

    except requests.exceptions.RequestException as e:
        print(f"Error en la conexión con la API: {e}")
        return None

# Ejecutar la prueba
recepcion = obtener_recepcion_stock()

# Verificar el resultado
if recepcion:
    print(json.dumps(recepcion, indent=4, ensure_ascii=False))  # Mostrar el resultado estructurado
else:
    print("No se pudo obtener la recepción de stock.")
