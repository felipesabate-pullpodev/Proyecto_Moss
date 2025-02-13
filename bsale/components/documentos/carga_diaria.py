import os
import requests
import json
import sys
import time
import logging
import pandas as pd
from dotenv import load_dotenv
from google.cloud import bigquery
from datetime import datetime, timedelta
import pytz  # 📌 Para manejar la zona horaria de Chile

# Cargar variables de entorno desde .env
load_dotenv()

# 📌 Configurar logging para que use solo STDOUT y evitar que Prefect lo marque como error
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)]  # 🔹 Enviar logs solo a STDOUT
)

logger = logging.getLogger(__name__)

# Token de acceso
ACCESS_TOKEN = os.getenv("BSALE_ACCESS_TOKEN")

# Configuración de BigQuery
BIGQUERY_PROJECT_ID = os.getenv("BIGQUERY_PROJECT_ID")
BIGQUERY_DATASET = os.getenv("BIGQUERY_DATASET")
BIGQUERY_TABLE = os.getenv("BIGQUERY_TABLE")
BIGQUERY_KEY_PATH = r"C:\Users\Felip\Desktop\PULLPO\Repositorio Demos\ELT&V\Proyecto_Moss\env\moss-448416-4aea07bf8473.json"

# Tamaño del batch para enviar datos a BigQuery
BATCH_SIZE = 500  

# 📌 Zona horaria de Chile
CHILE_TZ = pytz.timezone("America/Santiago")

def fetch_existing_ids_from_bigquery():
    """
    Obtiene los IDs de los documentos ya existentes en BigQuery para evitar duplicados.
    """
    try:
        client = bigquery.Client.from_service_account_json(BIGQUERY_KEY_PATH, project=BIGQUERY_PROJECT_ID)
        table_id = f"{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET}.{BIGQUERY_TABLE}"

        query = f"SELECT id FROM `{table_id}`"
        result = client.query(query).result()

        existing_ids = {row.id for row in result}  # Convierte la consulta en un conjunto de IDs
        logger.info(f" Se encontraron {len(existing_ids)} documentos ya existentes en BigQuery.")
        return existing_ids
    except Exception as e:
        logger.error(f" Error al consultar BigQuery para verificar duplicados: {e}")
        return set()

def load_to_bigquery(df):
    """
    Carga un DataFrame a BigQuery en lotes para evitar duplicados.
    """
    if df.empty:
        logger.info(" No hay datos nuevos para cargar en BigQuery.")
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

        logger.info(f" Cargados {len(df)} registros nuevos a BigQuery en la tabla {table_id}.")
    except Exception as e:
        logger.error(f" Error al cargar datos en BigQuery: {e}")

def fetch_all_pages(base_url, headers):
    """
    Descarga datos paginados desde Bsale usando expand y offset en el endpoint de documents.
    """
    session = requests.Session()
    all_items = []
    offset = 0
    limit = 50  # Bsale impone un límite máximo de 50

    while True:
        try:
            url = f"{base_url}&offset={offset}&limit={limit}"
            response = session.get(url, headers=headers, timeout=10)
            response.raise_for_status()
            data = response.json()

            items = data.get('items', [])
            all_items.extend(items)

            logger.info(f" Procesado offset {offset}: {len(items)} documentos encontrados.")

            if len(items) < limit:
                break  # Si no hay más documentos, salimos del loop

            offset += limit  # Avanzar al siguiente conjunto de documentos
        except requests.exceptions.RequestException as e:
            logger.error(f"Error al obtener datos: {e}")
            break

    return all_items

def process_document(document_data):
    """
    Procesa un documento de Bsale y lo estructura para BigQuery.
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

def extract_data(days_back=3):
    """
    Extrae documentos desde Bsale en un rango de fechas determinado usando expand.
    """
    # 📌 Obtener la fecha actual en Chile
    now_chile = datetime.now(CHILE_TZ)
    
    # 📌 Calcular el rango de fechas en la zona horaria de Chile
    start_date_chile = now_chile - timedelta(days=days_back + 1)
    end_date_chile = now_chile

    # 📌 Convertir las fechas a timestamps en UTC
    start_date_utc = int(start_date_chile.astimezone(pytz.utc).timestamp())
    end_date_utc = int(end_date_chile.astimezone(pytz.utc).timestamp())

    logger.info(f" Iniciando extracción de documentos desde {start_date_chile} hasta {end_date_chile} (Hora Chile)")
    logger.info(f" Convertido a timestamps UTC: {start_date_utc} - {end_date_utc}")

    headers = {
        'Content-Type': 'application/json',
        'access_token': ACCESS_TOKEN
    }

    base_url = (
        "https://api.bsale.cl/v1/documents.json"
        f"?emissiondaterange=[{start_date_utc},{end_date_utc}]"
        "&expand=document_type,client,office,user,details,references,document_taxes,sellers,payments"
    )

    # 🔍 Obtener IDs existentes en BigQuery antes de cargar
    existing_ids = fetch_existing_ids_from_bigquery()

    all_documents = fetch_all_pages(base_url, headers)
    
    buffer = []
    new_documents = 0

    for doc in all_documents:
        if doc.get("id") in existing_ids:
            continue  # Si el documento ya existe en BigQuery, lo saltamos

        processed_doc = process_document(doc)
        if processed_doc:
            buffer.append(processed_doc)
            new_documents += 1

        if len(buffer) >= BATCH_SIZE:
            df = pd.DataFrame(buffer)
            load_to_bigquery(df)
            buffer = []

    if buffer:
        df = pd.DataFrame(buffer)
        load_to_bigquery(df)

    logger.info(f" Extracción y carga completada. Total documentos procesados: {new_documents}")

if __name__ == "__main__":
    extract_data(days_back=3)
