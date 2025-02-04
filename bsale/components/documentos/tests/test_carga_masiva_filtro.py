import os
import requests
import json
import time
import logging
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
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
BIGQUERY_TABLE = os.getenv("BIGQUERY_TABLE")
BIGQUERY_KEY_PATH = os.getenv("BIGQUERY_KEY_PATH")

# Tamaño del batch para enviar datos a BigQuery
BATCH_SIZE = 20000  

def fetch_existing_ids_from_bigquery():
    """
    Obtiene los IDs de los documentos ya existentes en BigQuery para evitar duplicados.
    """
    try:
        client = bigquery.Client.from_service_account_json(BIGQUERY_KEY_PATH, project=BIGQUERY_PROJECT_ID)
        table_id = f"{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET}.{BIGQUERY_TABLE}"

        query = f"SELECT id FROM `{table_id}`"
        result = client.query(query).result()

        existing_ids = {row.id for row in result}  # Convertimos la consulta en un conjunto de IDs
        logger.info(f"🔍 Se encontraron {len(existing_ids)} documentos ya existentes en BigQuery.")
        return existing_ids
    except Exception as e:
        logger.error(f"❌ Error al consultar BigQuery para verificar duplicados: {e}")
        return set()

def load_to_bigquery_masivo(df):
    """
    Carga el DataFrame `df` en una tabla de BigQuery sin duplicados.
    """
    if df.empty:
        logger.info("⚠️ No hay datos nuevos para cargar en BigQuery.")
        return

    try:
        client = bigquery.Client.from_service_account_json(BIGQUERY_KEY_PATH, project=BIGQUERY_PROJECT_ID)
        table_id = f"{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET}.{BIGQUERY_TABLE}"

        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,  # Anexar datos
            autodetect=True  # Inferir esquema automáticamente
        )

        # Eliminar duplicados antes de cargar
        df.drop_duplicates(subset=["id"], inplace=True)

        load_job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        load_job.result()  # Espera a que termine la carga

        logger.info(f"✅ Cargados {len(df)} registros nuevos a BigQuery en la tabla {table_id}.")
    except Exception as e:
        logger.error(f"❌ Error al cargar datos en BigQuery: {e}")

def get_document_intervals(cursorlength=500):
    """
    Obtiene los intervalos de documentos desde el endpoint.
    """
    headers = {
        'Content-Type': 'application/json',
        'access_token': ACCESS_TOKEN,
        'target': 'beta'
    }
    url = f'https://api.bsale.cl/v1/documents_interval.json?cursorlength={cursorlength}'

    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        data = response.json()
        intervals = data.get('items', [])
        logger.info(f"Se obtuvieron {len(intervals)} intervalos de documentos.")
        return intervals
    except requests.exceptions.RequestException as e:
        logger.error(f"Error al obtener los intervalos de documentos: {e}")
        return []

def process_document(document_data):
    """
    Procesa un documento ya expandido, listo para cargar en BigQuery.
    """
    try:
        final_structure = {
            "id": document_data.get("id"),
            "emissionDate": document_data.get("emissionDate"),
            "expirationDate": document_data.get("expirationDate"),
            "generationDate": document_data.get("generationDate"),
            "number": document_data.get("number"),
            "totalAmount": document_data.get("totalAmount"),
            "netAmount": document_data.get("netAmount"),
            "taxAmount": document_data.get("taxAmount"),
            "state": document_data.get("state"),
            "document_type": json.dumps(document_data.get("document_type", {}), ensure_ascii=False),
            "client": json.dumps(document_data.get("client", {}), ensure_ascii=False),
            "office": json.dumps(document_data.get("office", {}), ensure_ascii=False),
            "user": json.dumps(document_data.get("user", {}), ensure_ascii=False),
            "references": json.dumps(document_data.get("references", []), ensure_ascii=False),
            "document_taxes": json.dumps(document_data.get("document_taxes", []), ensure_ascii=False),
            "details": json.dumps(document_data.get("details", []), ensure_ascii=False),
            "sellers": json.dumps(document_data.get("sellers", []), ensure_ascii=False),
            "payments": json.dumps(document_data.get("payments", []), ensure_ascii=False),
        }
        return final_structure
    except Exception as e:
        logger.error(f"❌ Error al procesar el documento ID {document_data.get('id')}: {e}")
        return None

def extract_data_with_expand(start_interval=0):
    headers_documents = {
        'Content-Type': 'application/json',
        'access_token': ACCESS_TOKEN,
        'target': 'beta'
    }

    intervals = get_document_intervals(cursorlength=500)
    if not intervals:
        logger.error("No se pudieron obtener los intervalos de documentos.")
        return

    # Obtener IDs existentes en BigQuery
    existing_ids = fetch_existing_ids_from_bigquery()

    total_intervals = len(intervals) - 1
    interval_counter = start_interval
    buffer = []
    start_time = time.time()

    with requests.Session() as session:
        for i in range(start_interval, len(intervals) - 1):
            interval_counter += 1
            firstid = intervals[i]['id']
            lastid = intervals[i + 1]['id'] - 1

            logger.info(f"Procesando intervalo {interval_counter}/{total_intervals}: IDs del {firstid} al {lastid}.")

            url = (
                f'https://api.bsale.cl/v1/documents.json'
                f'?firstid={firstid}&lastid={lastid}&order=none&limit=500'
                f'&expand=document_type,client,office,user,details,references,document_taxes,sellers,payments'
            )

            try:
                response = session.get(url, headers=headers_documents, timeout=30)
                response.raise_for_status()
                documents = response.json().get('items', [])

                # Filtrar documentos que ya existen en BigQuery
                new_documents = [doc for doc in documents if doc.get("id") not in existing_ids]

                logger.info(f"📄 Procesando {len(new_documents)} documentos nuevos.")

                for document in new_documents:
                    processed_document = process_document(document)
                    if processed_document:
                        buffer.append(processed_document)

                    # Si llegamos al batch_size, cargamos en BigQuery
                    if len(buffer) >= BATCH_SIZE:
                        df = pd.DataFrame(buffer)
                        load_to_bigquery_masivo(df)
                        buffer = []

            except requests.exceptions.RequestException as e:
                logger.error(f"Error al obtener documentos del intervalo {firstid}-{lastid}: {e}")
                continue

        # Cargar los últimos datos restantes
        if buffer:
            df = pd.DataFrame(buffer)
            load_to_bigquery_masivo(df)

    logger.info(f"✅ Proceso completado en {time.time() - start_time:.2f} segundos.")

if __name__ == "__main__":
    extract_data_with_expand(start_interval=0)
