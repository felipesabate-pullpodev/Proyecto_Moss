import os
import requests
import json
import time
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

# Función para cargar datos a BigQuery con WRITE_TRUNCATE (se borra y se recarga la tabla completa)
def load_to_bigquery(df):
    try:
        key_path = os.getenv("BIGQUERY_KEY_PATH")
        project_id = os.getenv("BIGQUERY_PROJECT_ID")
        dataset_id = os.getenv("BIGQUERY_DATASET")
        table_name = "bsale_stock_actual"  # Nombre de la tabla para stock

        if not all([key_path, project_id, dataset_id, table_name]):
            logger.error("Faltan variables de entorno para BigQuery. Verifica tu archivo .env.")
            return

        client = bigquery.Client.from_service_account_json(key_path, project=project_id)
        table_id = f"{project_id}.{dataset_id}.{table_name}"

        # Definir esquema de la tabla
        schema = [
            bigquery.SchemaField("id", "INTEGER"),
            bigquery.SchemaField("quantity", "FLOAT"),
            bigquery.SchemaField("quantityReserved", "FLOAT"),
            bigquery.SchemaField("quantityAvailable", "FLOAT"),
            bigquery.SchemaField("variant", "STRING"),
            bigquery.SchemaField("office", "STRING"),
        ]

        job_config = bigquery.LoadJobConfig(
            schema=schema,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
        )

        load_job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        load_job.result()

        logger.info(f"Se cargaron {len(df)} registros a BigQuery en la tabla {table_id} sin duplicados.")
    except Exception as e:
        logger.error(f"Error al cargar datos en BigQuery: {e}")

# Función para obtener los intervalos de stock desde BSALE
def get_stock_intervals(cursorlength=500):
    headers = {
        'Content-Type': 'application/json',
        'access_token': ACCESS_TOKEN,
        'target': 'beta'
    }
    url = f'https://api.bsale.cl/v1/stocks_interval.json?cursorlength={cursorlength}'

    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        data = response.json()
        intervals = data.get('items', [])
        logger.info(f"Se obtuvieron {len(intervals)} intervalos de stock.")
        return intervals
    except requests.exceptions.RequestException as e:
        logger.error(f"Error al obtener intervalos de stock: {e}")
        return []

# Función para obtener detalles del stock en un rango de IDs
def fetch_all_stocks(firstid, lastid):
    headers = {
        'Content-Type': 'application/json',
        'access_token': ACCESS_TOKEN,
        'target': 'beta'
    }
    url = (f'https://api.bsale.cl/v1/stocks.json?firstid={firstid}&lastid={lastid}&order=none&limit=500&'
           f'expand=variant,office')

    all_items = []
    with requests.Session() as session:
        while url:
            response = session.get(url, headers=headers, timeout=30)
            response.raise_for_status()
            data = response.json()
            all_items.extend(data.get('items', []))
            url = data.get('next')

    return all_items

# Función para procesar cada registro de stock
def process_stock(stock_data):
    try:
        processed_stock = {
            "id": stock_data.get("id"),
            "quantity": stock_data.get("quantity"),
            "quantityReserved": stock_data.get("quantityReserved"),
            "quantityAvailable": stock_data.get("quantityAvailable"),
            "variant": json.dumps(stock_data.get("variant", {}), ensure_ascii=False),
            "office": json.dumps(stock_data.get("office", {}), ensure_ascii=False)
        }
        return processed_stock
    except Exception as e:
        logger.error(f"Error al procesar stock ID {stock_data.get('id')}: {e}")
        return None

# Extraer datos de stock y cargarlos en una única operación
def extract_stock_data(start_interval=0):
    intervals = get_stock_intervals(cursorlength=500)
    if not intervals:
        logger.error("No se pudieron obtener intervalos de stock.")
        return

    total_intervals = len(intervals) - 1
    all_stock_buffer = []  # Buffer para acumular todos los registros de stock

    start_time = time.time()

    for i in range(start_interval, total_intervals):
        firstid = intervals[i]['id']
        lastid = intervals[i + 1]['id'] - 1

        logger.info(f"Procesando intervalo {i + 1}/{total_intervals}: IDs del {firstid} al {lastid}.")
        try:
            stocks = fetch_all_stocks(firstid, lastid)
            logger.info(f"Procesando {len(stocks)} registros de stock.")

            for stock in stocks:
                processed_stock = process_stock(stock)
                if processed_stock:
                    all_stock_buffer.append(processed_stock)
        except requests.exceptions.RequestException as e:
            logger.error(f"Error al obtener stocks del intervalo {firstid}-{lastid}: {e}")
            continue

    if all_stock_buffer:
        df = pd.DataFrame(all_stock_buffer)
        load_to_bigquery(df)
        logger.info(f"Cargados {len(df)} registros totales a BigQuery.")

    total_elapsed_time = time.time() - start_time
    logger.info(f"Proceso completado en {total_elapsed_time:.2f} segundos.")

if __name__ == "__main__":
    extract_stock_data(start_interval=0)
