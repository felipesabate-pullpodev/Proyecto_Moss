import os
import requests
import pandas as pd
from dotenv import load_dotenv
from google.cloud import bigquery

# Cargar variables de entorno desde .env
load_dotenv()

# Token de acceso de Bsale
API_TOKEN = os.getenv("BSALE_ACCESS_TOKEN")

# Configurar credenciales de BigQuery
BIGQUERY_KEY_PATH = os.getenv("BIGQUERY_KEY_PATH")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = BIGQUERY_KEY_PATH

# Configuración de BigQuery
BQ_PROJECT_ID = os.getenv("BIGQUERY_PROJECT_ID")
BQ_DATASET = os.getenv("BIGQUERY_DATASET")
BQ_TABLE = "bsale_stock_actual"  # Nombre de la tabla para stock

# Configuración de la API
BASE_URL = "https://api.bsale.io/v1/stocks.json"

def obtener_variant_code(variant_id):
    """
    Obtiene el código de una variante específica desde la API de Bsale.
    """
    variant_url = f"https://api.bsale.io/v1/variants/{variant_id}.json"
    headers = {'access_token': API_TOKEN}
    
    try:
        response = requests.get(variant_url, headers=headers, timeout=10)
        response.raise_for_status()
        variant_data = response.json()
        return variant_data.get("code", None)
    except requests.exceptions.RequestException as e:
        print(f"Error al obtener el código de la variante {variant_id}: {e}")
        return None

def obtener_stock_por_codigo(variant_code):
    """
    Obtiene el stock de una variante específica desde la API de Bsale usando su código (SKU).
    """
    headers = {
        'Content-Type': 'application/json',
        'access_token': API_TOKEN
    }
    params = {'code': variant_code}

    try:
        print(f"Consultando stock para la variante: {variant_code}...")
        response = requests.get(BASE_URL, headers=headers, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()

        if "items" in data and data["items"]:
            return data["items"]
        else:
            print(f"No se encontró stock para la variante {variant_code}.")
            return []

    except requests.exceptions.RequestException as e:
        print(f"Error en la conexión con la API: {e}")
        return []

def transformar_a_dataframe(stock_data):
    """
    Convierte la respuesta JSON de Bsale en un DataFrame de Pandas.
    Incluye el variant_code.
    """
    registros = []
    for item in stock_data:
        variant_id = item["variant"]["id"]
        variant_code = obtener_variant_code(variant_id)  # Obtener el código de la variante
        
        registros.append({
            "stock_id": item["id"],
            "variant_id": variant_id,
            "variant_code": variant_code,  # Agregar el código de la variante
            "office_id": item["office"]["id"],
            "quantity": item["quantity"],
            "quantity_reserved": item["quantityReserved"],
            "quantity_available": item["quantityAvailable"]
        })

    return pd.DataFrame(registros)

def cargar_a_bigquery(df):
    """
    Carga los datos a BigQuery en la tabla especificada.
    """
    client = bigquery.Client(project=BQ_PROJECT_ID)
    table_id = f"{BQ_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}"

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND  # Añade datos sin borrar lo anterior
    )

    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()
    print(f"Datos cargados en {table_id} con {len(df)} registros.")

# Prueba con el variant code "2260AZMAXXL"
variant_code_prueba = "2260AZMAXXL"
stock_data = obtener_stock_por_codigo(variant_code_prueba)

if stock_data:
    df_stock = transformar_a_dataframe(stock_data)
    cargar_a_bigquery(df_stock)
