import os
import requests
import json
import sys
import logging
import time
import pandas as pd
from datetime import datetime, timedelta
import pytz

from dotenv import load_dotenv
from google.cloud import bigquery

# -----------------------------------------------------------------------------
# 1) Configuraciones Generales
# -----------------------------------------------------------------------------
load_dotenv()  # Carga variables de entorno .env si existe

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# Variables de entorno / Credenciales
AD_ACCOUNT_ID = os.getenv("FB_AD_ACCOUNT_ID")     
ACCESS_TOKEN = os.getenv("FB_ACCESS_TOKEN")       
BIGQUERY_PROJECT_ID = os.getenv("BIGQUERY_PROJECT_ID")
BIGQUERY_DATASET = os.getenv("BIGQUERY_DATASET")
BIGQUERY_TABLE = 'test_meta'
BIGQUERY_KEY_PATH = os.getenv("BIGQUERY_KEY_PATH")

# Niveles de batch
BATCH_SIZE = 500        # batch interno: cada 500 registros subimos a BigQuery
MAX_RECORDS = 20000     # batch global: cada 20,000 registros, forzamos otra subida + reset

CHILE_TZ = pytz.timezone("America/Santiago")

# -----------------------------------------------------------------------------
# 2) Funciones de BigQuery
# -----------------------------------------------------------------------------
def fetch_existing_ids_from_bigquery():
    """
    Obtiene los IDs existentes en BigQuery para evitar duplicados.
    """
    try:
        client = bigquery.Client.from_service_account_json(
            BIGQUERY_KEY_PATH,
            project=BIGQUERY_PROJECT_ID
        )
        table_id = f"{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET}.{BIGQUERY_TABLE}"

        query = f"SELECT id FROM `{table_id}`"
        result = client.query(query).result()

        existing_ids = {row.id for row in result}
        logger.info(f" Se encontraron {len(existing_ids)} registros existentes en BigQuery.")
        return existing_ids
    except Exception as e:
        logger.error(f" Error al consultar BigQuery: {e}")
        return set()

def load_to_bigquery(df):
    """
    Carga un DataFrame a BigQuery, eliminando duplicados y anexando datos.
    """
    if df.empty:
        logger.info(" No hay datos nuevos para cargar en BigQuery.")
        return

    try:
        client = bigquery.Client.from_service_account_json(
            BIGQUERY_KEY_PATH,
            project=BIGQUERY_PROJECT_ID
        )
        table_id = f"{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET}.{BIGQUERY_TABLE}"

        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            autodetect=True
            # create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
        )

        df.drop_duplicates(subset=["id"], inplace=True)

        load_job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        load_job.result()  # Esperar la carga

        logger.info(f" Cargados {len(df)} registros nuevos a BigQuery en {table_id}.")
    except Exception as e:
        logger.error(f" Error al cargar datos en BigQuery: {e}")

# -----------------------------------------------------------------------------
# 3) Función para obtener TODOS los anuncios y su status de la cuenta
#    (timeout y reintentos ante Timeout)
# -----------------------------------------------------------------------------
def fetch_all_ads_status():
    """
    Devuelve un diccionario con la forma:
      {
        '1234567890': {'status': 'ACTIVE', 'effective_status': 'ACTIVE'},
        ...
      }
    Maneja paginación y timeout con reintentos.
    """
    ads_status_map = {}
    session = requests.Session()
    url = (
        f"https://graph.facebook.com/v16.0/act_{AD_ACCOUNT_ID}/ads"
        f"?fields=id,name,status,effective_status"
        f"&access_token={ACCESS_TOKEN}"
    )

    while True:
        try:
            response = session.get(url, timeout=60)
            data = response.json()

            if "error" in data:
                logger.error(f" Ocurrió un error al obtener Ads: {data['error']}")
                break

            ads_data = data.get("data", [])
            for ad in ads_data:
                ad_id = ad.get("id", None)
                if ad_id:
                    ads_status_map[ad_id] = {
                        "status": ad.get("status", "unknown"),
                        "effective_status": ad.get("effective_status", "unknown")
                    }

            paging = data.get("paging", {})
            next_page = paging.get("next")

            if not next_page:
                logger.info(" No hay más páginas en /ads.")
                break
            else:
                url = next_page
                logger.info(f" Obtenidos {len(ads_data)} anuncios, avanzando a la siguiente página...")

        except requests.exceptions.Timeout:
            logger.warning("Se agotó el tiempo de espera (Timeout) en /ads. Reintentando en 30 seg...")
            time.sleep(30)
            continue

        except requests.exceptions.RequestException as e:
            logger.error(f" Error al obtener datos de Ads: {e}")
            break

    logger.info(f" Total de anuncios obtenidos: {len(ads_status_map)}")
    return ads_status_map

# -----------------------------------------------------------------------------
# 4) Paginación en la API de Insights (nivel=ad) con timeout y reintentos
# -----------------------------------------------------------------------------
def fetch_all_insights(base_url):
    """
    Llama a la API con paginación, maneja timeouts e imprime logs.
    Retorna una lista con todos los 'data' combinados.
    """
    all_data = []
    session = requests.Session()
    url = base_url

    while True:
        try:
            response = session.get(url, timeout=60)
            data = response.json()

            if "error" in data:
                logger.error(f" Ocurrió un error en la API de Meta: {data['error']}")
                break

            insights = data.get("data", [])
            all_data.extend(insights)
            logger.info(f" Recibidos {len(insights)} registros en esta página.")

            paging = data.get("paging", {})
            next_page = paging.get("next")

            if not next_page:
                logger.info(" No hay más páginas de Insights.")
                break
            else:
                url = next_page
                logger.info(" Avanzando a la siguiente página...")

        except requests.exceptions.Timeout:
            logger.warning("Se agotó el tiempo de espera (Timeout) en Insights. Reintentando en 30 seg...")
            time.sleep(30)
            continue

        except requests.exceptions.RequestException as e:
            logger.error(f" Error al obtener datos de Insights: {e}")
            break

    return all_data

# -----------------------------------------------------------------------------
# 5) Procesar cada fila de Insights (crear 'id' único) + status
# -----------------------------------------------------------------------------
def process_insight(insight, ads_status_map):
    """
    Procesa cada registro 'insight' y retorna un dict con columnas planas,
    dejando 'actions' y 'action_values' como JSON anidados.
    """
    try:
        ad_id = insight.get("ad_id", "unknown")
        date_start = insight.get("date_start", "unknown")
        age = insight.get("age", "unknown")
        gender = insight.get("gender", "unknown")

        unique_id = f"{ad_id}_{date_start}_{age}_{gender}"

        # Obtener status y effective_status del anuncio
        ad_status_info = ads_status_map.get(ad_id, {})
        status = ad_status_info.get("status", "unknown")
        effective_status = ad_status_info.get("effective_status", "unknown")

        # Definir tipos de acción que se consideran como compra
        purchase_types = {
            "purchase",
            "onsite_web_purchase",
            "onsite_web_app_purchase",
            "offsite_conversion.fb_pixel_purchase",
            "omni_purchase",
            "web_in_store_purchase"
        }

        # (1) Acciones anidadas como JSON
        actions = insight.get("actions", [])
        actions_json = json.dumps(actions)  # Guarda la lista completa como string JSON

        # Extraer el número de compras (eventos) desde 'actions'
        purchases = 0
        for action in actions:
            if action.get("action_type") in purchase_types:
                purchases += int(action.get("value", 0))

        # (2) Action_values anidados como JSON
        action_values = insight.get("action_values", [])
        action_values_json = json.dumps(action_values)  # Guarda la lista completa como string JSON

        # Extraer el valor monetario total de las compras
        purchase_value = 0.0
        for av in action_values:
            if av.get("action_type") in purchase_types:
                purchase_value += float(av.get("value", 0))

        return {
            "id": unique_id,
            "ad_id": ad_id,
            "ad_name": insight.get("ad_name"),
            "adset_id": insight.get("adset_id"),
            "adset_name": insight.get("adset_name"),
            "campaign_id": insight.get("campaign_id"),
            "campaign_name": insight.get("campaign_name"),
            "impressions": insight.get("impressions"),
            "clicks": insight.get("clicks"),
            "ctr": insight.get("ctr"),
            "spend": insight.get("spend"),
            "age": age,
            "gender": gender,
            "date_start": date_start,
            "date_stop": insight.get("date_stop", "unknown"),
            "actions": actions_json,            # Guarda la lista original como JSON
            "action_values": action_values_json, # Guarda la lista original como JSON
            "purchases": purchases,             # Número de eventos de compra
            "purchase_value": purchase_value,    # Monto total de compras
            "status": status,
            "effective_status": effective_status
        }

    except Exception as e:
        logger.error(f"Error al procesar la fila de Insights: {e}")
        return None

# -----------------------------------------------------------------------------
# 6) Función principal de extracción + carga (con batch interno y global)
# -----------------------------------------------------------------------------
def extract_insights_meta(days_back=1):
    """
    Obtiene Insights de los últimos 'days_back' días a nivel de anuncio,
    usando 2 niveles de batch:
      - BATCH_SIZE=500 (subidas parciales a BQ)
      - MAX_RECORDS=20000 (cada 20k procesados, sube a BQ y reinicia conteo)
    Maneja timeouts y reintentos.
    """
    now_chile = datetime.now(CHILE_TZ)
    start_date_chile = now_chile - timedelta(days=days_back)
    end_date_chile = now_chile

    since_str = start_date_chile.strftime("%Y-%m-%d")
    until_str = end_date_chile.strftime("%Y-%m-%d")

    logger.info(f" Extrayendo Insights (ad level) desde {since_str} hasta {until_str}.")

    # 1) Info de status de anuncios
    ads_status_map = fetch_all_ads_status()

    # 2) URL base (incluimos action_values para obtener el valor monetario también)
    base_url = (
        f"https://graph.facebook.com/v16.0/act_{AD_ACCOUNT_ID}/insights"
        f"?level=ad"
        f"&fields=ad_id,ad_name,adset_id,adset_name,campaign_id,campaign_name,"
        f"impressions,clicks,ctr,spend,actions,action_values"
        f"&breakdowns=age,gender"
        f"&action_breakdowns=action_type"
        f"&time_increment=1"
        f"&time_range={{'since':'{since_str}','until':'{until_str}'}}"
        f"&access_token={ACCESS_TOKEN}"
    )

    # 3) IDs existentes
    existing_ids = fetch_existing_ids_from_bigquery()

    # 4) Paginar para obtener todos los insights
    all_insights = fetch_all_insights(base_url)

    # 5) Procesar y cargar con batch interno y global
    buffer = []
    new_records = 0
    count_total = 0  # Contador global de registros

    for insight in all_insights:
        record = process_insight(insight, ads_status_map)
        if not record:
            continue

        # Verificar duplicado
        if record["id"] in existing_ids:
            continue

        buffer.append(record)
        new_records += 1
        count_total += 1

        # Batch interno (cada 500)
        if len(buffer) >= BATCH_SIZE:
            df = pd.DataFrame(buffer)
            load_to_bigquery(df)
            buffer = []

        # Batch global (cada 20,000)
        if count_total >= MAX_RECORDS:
            if buffer:
                df = pd.DataFrame(buffer)
                load_to_bigquery(df)
                buffer = []
            logger.info(f" Alcanzado el límite global de {MAX_RECORDS} registros. Reseteando contador.")
            count_total = 0

    # Al finalizar, subimos lo que quede en buffer
    if buffer:
        df = pd.DataFrame(buffer)
        load_to_bigquery(df)

    logger.info(f" Finalizado. Se procesaron {new_records} registros nuevos.")

# -----------------------------------------------------------------------------
# 7) Main
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    start_time = datetime.now()

    extract_insights_meta(days_back=1)

    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    logger.info(f" Ejecución completa en {duration} segundos.")
