import os
import requests
import json
import logging
import pandas as pd
from dotenv import load_dotenv
from google.cloud import bigquery

# Cargar variables de entorno desde .env
load_dotenv()

# Configurar el logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Token de acceso
ACCESS_TOKEN = os.getenv("BSALE_ACCESS_TOKEN")

# Configuración de BigQuery
BIGQUERY_PROJECT_ID = os.getenv("BIGQUERY_PROJECT_ID")
BIGQUERY_DATASET = os.getenv("BIGQUERY_DATASET")
BIGQUERY_KEY_PATH = os.getenv("BIGQUERY_KEY_PATH")

# Dimensiones a extraer y sus endpoints
DIMENSIONS = {
    "dim_offices": "https://api.bsale.cl/v1/offices.json",
    "dim_users": "https://api.bsale.cl/v1/users.json",
    "dim_references": "https://api.bsale.cl/v1/documents/references.json",
    "dim_document_taxes": "https://api.bsale.cl/v1/documents/document_taxes.json",
    "dim_sellers": "https://api.bsale.cl/v1/documents/sellers.json",
    "dim_document_type": "https://api.bsale.io/v1/document_types.json"
}

def fetch_data(url):
    """ Descarga datos desde un endpoint de Bsale."""
    headers = {'Content-Type': 'application/json', 'access_token': ACCESS_TOKEN}
    session = requests.Session()
    all_items = []
    offset = 0
    limit = 50  # Máximo permitido por Bsale

    while True:
        try:
            response = session.get(f"{url}?limit={limit}&offset={offset}", headers=headers, timeout=10)
            response.raise_for_status()
            data = response.json()

            items = data.get('items', [])
            all_items.extend(items)

            if len(items) < limit:
                break  # No hay más datos

            offset += limit  # Avanzar a la siguiente página
        except requests.exceptions.RequestException as e:
            logger.error(f"Error al obtener datos de {url}: {e}")
            break

    return all_items

def normalize_data(data):
    """ Convierte valores que son diccionarios en formato JSON string."""
    for item in data:
        for key, value in item.items():
            if isinstance(value, dict) or isinstance(value, list):
                item[key] = json.dumps(value, ensure_ascii=False)
    return data

def load_to_bigquery(df, table_name):
    """ Carga un DataFrame a BigQuery."""
    if df.empty:
        logger.info(f"⚠️ No hay datos nuevos para cargar en {table_name}.")
        return

    try:
        client = bigquery.Client.from_service_account_json(BIGQUERY_KEY_PATH, project=BIGQUERY_PROJECT_ID)
        table_id = f"{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET}.{table_name}"

        job_config = bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE)
        df.drop_duplicates(inplace=True)
        load_job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        load_job.result()

        logger.info(f"✅ Cargados {len(df)} registros a {table_id}.")
    except Exception as e:
        logger.error(f"❌ Error al cargar datos en BigQuery: {e}")

def extract_dimensions():
    """ Extrae y carga todas las dimensiones."""
    for dimension, url in DIMENSIONS.items():
        logger.info(f"⏳ Extrayendo {dimension}...")
        data = fetch_data(url)

        if not data:
            logger.warning(f"⚠️ No se encontraron datos en {dimension}.")
            continue

        df = pd.DataFrame(normalize_data(data))
        load_to_bigquery(df, dimension)

if __name__ == "__main__":
    extract_dimensions()
