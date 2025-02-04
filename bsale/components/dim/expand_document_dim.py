import os
import json
import logging
import requests
import pandas as pd
from google.cloud import bigquery
from dotenv import load_dotenv

# Cargar variables de entorno desde .env
load_dotenv()

# Configurar el logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuración de BigQuery
BIGQUERY_PROJECT_ID = os.getenv("BIGQUERY_PROJECT_ID")
BIGQUERY_DATASET = os.getenv("BIGQUERY_DATASET")
BIGQUERY_KEY_PATH = os.getenv("BIGQUERY_KEY_PATH")
ACCESS_TOKEN = os.getenv("BSALE_ACCESS_TOKEN")

# Tabla origen y destino
SOURCE_TABLE = f"{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET}.bsale_documents"
DESTINATION_TABLE = f"{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET}.dim_references"


def fetch_document_ids():
    """ Obtiene los IDs de los documentos desde BigQuery. """
    try:
        client = bigquery.Client.from_service_account_json(BIGQUERY_KEY_PATH, project=BIGQUERY_PROJECT_ID)
        query = f"SELECT id FROM `{SOURCE_TABLE}`"
        df = client.query(query).to_dataframe()
        return df["id"].tolist()
    except Exception as e:
        logger.error(f"❌ Error al obtener documentos desde BigQuery: {e}")
        return []

def fetch_references(document_id):
    """ Obtiene las referencias de un documento desde Bsale. """
    url = f"https://api.bsale.cl/v1/documents/{document_id}/references.json"
    headers = {'Content-Type': 'application/json', 'access_token': ACCESS_TOKEN}
    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        data = response.json()
        return data.get("items", [])
    except requests.exceptions.RequestException as e:
        logger.error(f"Error al obtener referencias del documento {document_id}: {e}")
        return []

def process_references():
    """ Obtiene y guarda referencias de documentos en BigQuery. """
    document_ids = fetch_document_ids()
    if not document_ids:
        logger.warning("⚠️ No se encontraron documentos para procesar referencias.")
        return
    
    all_references = []
    for doc_id in document_ids:
        references = fetch_references(doc_id)
        for ref in references:
            ref["document_id"] = doc_id  # Relacionar con el documento original
            all_references.append(ref)
    
    if all_references:
        df_references = pd.DataFrame(all_references)
        load_to_bigquery(df_references)
    else:
        logger.info("⚠️ No se encontraron referencias para cargar.")

def load_to_bigquery(df):
    """ Carga un DataFrame en una tabla de BigQuery. """
    if df.empty:
        logger.info("⚠️ No hay datos nuevos para cargar en dim_references.")
        return
    
    try:
        client = bigquery.Client.from_service_account_json(BIGQUERY_KEY_PATH, project=BIGQUERY_PROJECT_ID)
        job_config = bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE)
        df.drop_duplicates(inplace=True)
        load_job = client.load_table_from_dataframe(df, DESTINATION_TABLE, job_config=job_config)
        load_job.result()
        
        logger.info(f"✅ Cargados {len(df)} registros a {DESTINATION_TABLE}.")
    except Exception as e:
        logger.error(f"❌ Error al cargar datos en BigQuery: {e}")

if __name__ == "__main__":
    process_references()
