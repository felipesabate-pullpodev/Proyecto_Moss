import os
import requests
import json
import logging
import time
import pandas as pd
from dotenv import load_dotenv
from google.cloud import bigquery

# -------------------------------------------------------------------
# CONFIGURACIONES GENERALES
# -------------------------------------------------------------------
load_dotenv()
ACCESS_TOKEN = os.getenv("FB_ACCESS_TOKEN")
AD_ACCOUNT_ID = os.getenv("FB_AD_ACCOUNT_ID")
BIGQUERY_PROJECT_ID = os.getenv("BIGQUERY_PROJECT_ID")
BIGQUERY_DATASET = os.getenv("BIGQUERY_DATASET")
BIGQUERY_TABLE = "meta_dim_creative"
BIGQUERY_KEY_PATH = os.getenv("BIGQUERY_KEY_PATH")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# -------------------------------------------------------------------
# FUNCIONES PARA EXTRAER DATOS
# -------------------------------------------------------------------
def fetch_all_ad_creatives():
    """
    Obtiene todos los Ad Creatives asociados a los anuncios de la cuenta.
    Maneja paginación y espera si se alcanza el rate limit.
    """
    session = requests.Session()
    url = (
        f"https://graph.facebook.com/v16.0/act_{AD_ACCOUNT_ID}/ads"
        f"?fields=id,creative"
        f"&access_token={ACCESS_TOKEN}"
    )

    ad_creatives = []
    
    while url:
        try:
            response = session.get(url, timeout=60)
            data = response.json()

            if "error" in data:
                error_code = data["error"].get("code", 0)
                
                if error_code == 80004:  # Rate limit alcanzado
                    logger.warning("Rate limit alcanzado en /ads. Esperando 60 seg antes de continuar...")
                    time.sleep(60)  # Espera antes de reintentar
                    continue  # Reintentar la misma solicitud
                
                logger.error(f"Error en API: {data['error']}")
                break

            for ad in data.get("data", []):
                ad_id = ad.get("id")
                ad_creative_id = ad.get("creative", {}).get("id")
                if ad_id and ad_creative_id:
                    ad_creatives.append({"ad_id": ad_id, "ad_creative_id": ad_creative_id})

            url = data.get("paging", {}).get("next")

            # Espera 0.5 segundos entre cada solicitud para evitar rate limits
            time.sleep(0.5)

        except requests.exceptions.RequestException as e:
            logger.error(f"Error de conexión: {e}")
            break

    logger.info(f"Se encontraron {len(ad_creatives)} Ad Creatives.")
    return ad_creatives


def fetch_ad_creative_details(ad_creative_id, retries=3, wait_time=60):
    """
    Obtiene información detallada de un Ad Creative.
    Si hay error de rate limit, espera y reintenta.
    """
    url = (
        f"https://graph.facebook.com/v16.0/{ad_creative_id}"
        f"?fields=id,name,title,body,image_url,thumbnail_url,video_id,object_story_id,status,call_to_action_type,object_type,template_url,object_story_spec"
        f"&access_token={ACCESS_TOKEN}"
    )

    for attempt in range(retries):
        response = requests.get(url, timeout=60)
        data = response.json()

        if "error" in data:
            error_code = data["error"].get("code", 0)

            # Si el error es por rate limit, espera y reintenta
            if error_code == 80004:
                logger.warning(f"Rate limit alcanzado. Esperando {wait_time} seg antes de reintentar...")
                time.sleep(wait_time)
                continue  # Reintentar la solicitud

            logger.error(f"Error obteniendo Creative ID {ad_creative_id}: {data['error']}")
            return None  # Si es otro error, no reintentar

        return {
            "ad_creative_id": ad_creative_id,
            "name": data.get("name"),
            "body": data.get("body"),
            "title": data.get("title"),
            "image_url": data.get("image_url"),
            "thumbnail_url": data.get("thumbnail_url"),
            "video_id": data.get("video_id"),
            "object_story_id": data.get("object_story_id"),
            "status": data.get("status"),  # Campo status agregado
            "call_to_action_type": data.get("call_to_action_type"),
            "object_type": data.get("object_type"),
            "template_url": data.get("template_url"),
            "object_story_spec": json.dumps(data.get("object_story_spec", {}))
        }

    logger.error(f"Falló después de {retries} intentos para {ad_creative_id}")
    return None

# -------------------------------------------------------------------
# CARGAR DATOS A BIGQUERY
# -------------------------------------------------------------------
def load_to_bigquery(df):
    """
    Carga un DataFrame a BigQuery en la tabla dim_creative
    """
    if df.empty:
        logger.info("No hay datos nuevos para cargar en BigQuery.")
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
        )

        df.drop_duplicates(subset=["ad_creative_id"], inplace=True)

        load_job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        load_job.result()  # Esperar la carga

        logger.info(f" Cargados {len(df)} registros nuevos a BigQuery en {table_id}.")
    except Exception as e:
        logger.error(f" Error al cargar datos en BigQuery: {e}")


# -------------------------------------------------------------------
# PROCESO PRINCIPAL
# -------------------------------------------------------------------
def extract_creatives_meta():
    """
    Obtiene todos los Ad Creatives, extrae detalles y los carga en BigQuery.
    """
    logger.info("Obteniendo todos los Ad Creatives de la cuenta...")
    ad_creatives = fetch_all_ad_creatives()

    creative_data = []
    for ad in ad_creatives:
        creative_info = fetch_ad_creative_details(ad["ad_creative_id"])
        if creative_info:
            creative_info["ad_id"] = ad["ad_id"]
            creative_data.append(creative_info)

            # Espera 0.5 segundos entre cada solicitud para evitar rate limits
            time.sleep(0.5)

    if creative_data:
        df = pd.DataFrame(creative_data)
        load_to_bigquery(df)

    logger.info("Proceso de extracción y carga de Ad Creatives completado.")


# -------------------------------------------------------------------
# EJECUCIÓN
# -------------------------------------------------------------------
if __name__ == "__main__":
    extract_creatives_meta()