import os
import requests
import json
import time
import logging
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv
from google.cloud import bigquery
import random

# Cargar variables de entorno desde .env
load_dotenv()

# Configurar el logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Token de acceso
ACCESS_TOKEN = os.getenv("BSALE_ACCESS_TOKEN")

# Función para cargar masivamente el DataFrame a BigQuery
def load_to_bigquery_masivo(df):
    """
    Carga el DataFrame `df` en una tabla de BigQuery.
    Ajusta el project, dataset y table_name según tu configuración.
    """
    try:
        # Obtener variables de entorno
        key_path = os.getenv("BIGQUERY_KEY_PATH")
        project_id = os.getenv("BIGQUERY_PROJECT_ID")
        dataset_id = os.getenv("BIGQUERY_DATASET")
        table_name = os.getenv("BIGQUERY_TABLE")

        # Depuración: Verificar si las variables están cargadas
        if not key_path:
            logger.error("BIGQUERY_KEY_PATH no está definido en las variables de entorno.")
        if not project_id:
            logger.error("BIGQUERY_PROJECT_ID no está definido en las variables de entorno.")
        if not dataset_id:
            logger.error("BIGQUERY_DATASET no está definido en las variables de entorno.")
        if not table_name:
            logger.error("BIGQUERY_TABLE no está definido en las variables de entorno.")

        if not all([key_path, project_id, dataset_id, table_name]):
            logger.error("Faltan variables de entorno para BigQuery. Verifica tu archivo .env.")
            return

        # Crear el cliente de BigQuery usando el archivo de credenciales
        client = bigquery.Client.from_service_account_json(key_path, project=project_id)

        # Define el ID completo de tu tabla: project.dataset.table
        table_id = f"{project_id}.{dataset_id}.{table_name}"

        # Configura el modo de carga: WRITE_APPEND para anexar datos
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            autodetect=True  # Infiriendo el esquema automáticamente
        )

        # Lanza el job de carga
        load_job = client.load_table_from_dataframe(
            df,
            table_id,
            job_config=job_config
        )
        load_job.result()  # Espera a que termine la carga

        logger.info(f"Se cargaron {len(df)} registros a BigQuery en la tabla {table_id}.")
    except Exception as e:
        logger.error(f"Error al cargar datos en BigQuery: {e}")

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
    max_retries = 5

    with requests.Session() as session:
        for attempt in range(max_retries):
            try:
                response = session.get(url, headers=headers, timeout=10)
                response.raise_for_status()
                data = response.json()
                intervals = data.get('items', [])
                logger.info(f"Se obtuvieron {len(intervals)} intervalos de documentos.")
                return intervals
            except requests.exceptions.HTTPError as e:
                if response.status_code == 429:
                    retry_after = int(response.headers.get('Retry-After', 60))
                    retry_delay = retry_after * (2 ** attempt)
                    logger.warning(f"Límite de tasa alcanzado al acceder a {url}, esperando {retry_delay} segundos...")
                    time.sleep(retry_delay)
                    continue
                else:
                    logger.error(f"Error HTTP al obtener los intervalos de documentos: {e}")
                    return []
            except requests.exceptions.RequestException as e:
                logger.error(f"Error al obtener los intervalos de documentos: {e}")
                return []
    logger.error(f"No se pudo obtener los intervalos después de {max_retries} intentos.")
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
        logger.error(f"Error al procesar el documento ID {document_data.get('id')}: {e}")
        return None

def fetch_all_pages(url, headers, session):
    all_items = []
    while url:
        response = session.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        data = response.json()
        all_items.extend(data.get('items', []))
        url = data.get('next')
    return all_items

def expand_document_details(document, headers, session):
    """
    Este método expande la paginación de 'details' si viniera con estructura paginada.
    """
    details_field = document.get("details", [])
    if isinstance(details_field, dict):  # Significa que es un objeto con 'items' y 'next'
        all_details = details_field.get('items', [])
        next_url = details_field.get('next')
        while next_url:
            more_details = fetch_all_pages(next_url, headers, session)
            all_details.extend(more_details)
            # La respuesta de fetch_all_pages ya se encarga de iterar 'next'
            # así que salimos del while
            next_url = None
        document['details'] = all_details
    return document

def extract_data_with_expand(start_interval=0):
    """
    Usa el 'Enfoque A':
      - intervals[i]['id']  => se considera inclusivo como lastid
      - En cada vuelta, el rango es [firstid, lastid]
      - Luego, firstid = lastid + 1 para evitar duplicados en la siguiente iteración.
    """
    headers_documents = {
        'Content-Type': 'application/json',
        'access_token': ACCESS_TOKEN,
        'target': 'beta'
    }

    # 1) Obtener intervalos
    intervals = get_document_intervals(cursorlength=500)
    if not intervals:
        logger.error("No se pudieron obtener los intervalos de documentos.")
        return

    # Asegurarnos de que estén ordenados por 'id'
    intervals.sort(key=lambda x: x["id"])

    # Validación simple
    if start_interval >= len(intervals):
        logger.error(f"El índice de inicio {start_interval} supera la cantidad de intervalos ({len(intervals)}).")
        return

    # 2) Preparar variables de control
    buffer = []
    batch_size = 20000
    start_time = time.time()

    # 3) Iniciar el firstid en el intervalo de comienzo
    firstid = intervals[start_interval]['id']
    total_intervals = len(intervals) - (start_interval + 1)

    with requests.Session() as session:
        # 4) Iteramos desde el siguiente al 'start_interval' hasta el final
        interval_counter = 0
        for i in range(start_interval + 1, len(intervals)):
            interval_counter += 1
            lastid = intervals[i]['id']

            # Si lastid < firstid, no se hace nada
            if lastid < firstid:
                logger.warning(f"Intervalo inválido: lastid={lastid} < firstid={firstid}. Se omite.")
                continue

            logger.info(f"Procesando intervalo {interval_counter}/{total_intervals}: IDs del {firstid} al {lastid}.")

            # Reintentos (p.ej. 5) en caso de 429 o errores de red
            for attempt in range(5):
                try:
                    url = (f"https://api.bsale.cl/v1/documents.json"
                           f"?firstid={firstid}&lastid={lastid}&order=none&limit=500"
                           f"&expand=document_type,client,office,user,details,references,document_taxes,sellers,payments")

                    # Descargar todos los documentos de este rango
                    documents = fetch_all_pages(url, headers_documents, session)
                    logger.info(f"Procesando {len(documents)} documentos en el rango {firstid}-{lastid}.")

                    # Expandir detalles si están paginados
                    docs_with_incomplete_details = []
                    for doc in documents:
                        details = doc.get("details")
                        if isinstance(details, dict) and details.get('next'):
                            docs_with_incomplete_details.append(doc)

                    for doc in docs_with_incomplete_details:
                        doc = expand_document_details(doc, headers_documents, session)

                    # Procesar y acumular
                    for document in documents:
                        processed_document = process_document(document)
                        if processed_document:
                            buffer.append(processed_document)
                            # Cargar en lotes de batch_size
                            if len(buffer) >= batch_size:
                                df = pd.DataFrame(buffer)
                                load_to_bigquery_masivo(df)
                                logger.info(f"Cargados {len(df)} registros a BigQuery.")
                                buffer.clear()

                    # Si llegamos aquí sin excepciones, salimos del for attempt
                    break

                except requests.exceptions.HTTPError as e:
                    # Manejo especial de error 429 (rate limit)
                    if e.response is not None and e.response.status_code == 429:
                        retry_after = int(e.response.headers.get('Retry-After', 60))
                        retry_delay = retry_after * (2 ** attempt)
                        logger.warning(f"Límite de tasa alcanzado (429). Esperando {retry_delay} seg...")
                        time.sleep(retry_delay)
                        continue
                    else:
                        logger.error(f"Error HTTP en intervalo {firstid}-{lastid}: {e}")
                        break
                except requests.exceptions.RequestException as e:
                    logger.error(f"Error de red en intervalo {firstid}-{lastid}: {e}")
                    break
            else:
                logger.error(f"No se pudo obtener documentos del intervalo {firstid}-{lastid} tras 5 intentos.")

            # Actualizamos el firstid para el siguiente intervalo
            firstid = lastid + 1

            # Cálculo de tiempos (opcional)
            elapsed_time = time.time() - start_time
            intervals_left = total_intervals - interval_counter
            if interval_counter > 0:
                estimated_total_time = (elapsed_time / interval_counter) * total_intervals
                estimated_time_left = estimated_total_time - elapsed_time
            else:
                estimated_time_left = 0

            logger.info(f"Tiempo transcurrido: {elapsed_time:.2f}s, estimado restante: {estimated_time_left:.2f}s.")
            logger.info(f"Documentos en buffer tras intervalo {interval_counter}: {len(buffer)}")

        # Si al final quedan documentos en el buffer, cargarlos
        if buffer:
            df = pd.DataFrame(buffer)
            load_to_bigquery_masivo(df)
            logger.info(f"Cargados {len(df)} registros restantes a BigQuery.")
            buffer.clear()

    total_elapsed_time = time.time() - start_time
    logger.info(f"Proceso completado en {total_elapsed_time:.2f} segundos.")

if __name__ == "__main__":
    extract_data_with_expand(start_interval=0)
