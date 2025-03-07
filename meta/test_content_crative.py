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
# LISTA COMPLETA DE CAMPOS DISPONIBLES EN AD CREATIVE
# -------------------------------------------------------------------
ALL_FIELDS = [
    "id", "account_id", "actor_id", "ad_disclaimer_spec", "adlabels", "applink_treatment",
    "asset_feed_spec", "authorization_category", "body", "branded_content",
    "branded_content_sponsor_page_id", "bundle_folder_id", "call_to_action_type",
    "categorization_criteria", "category_media_source", "collaborative_ads_lsb_image_bank_id",
    "contextual_multi_ads", "creative_sourcing_spec", "degrees_of_freedom_spec",
    "destination_set_id", "dynamic_ad_voice", "effective_authorization_category",
    "effective_instagram_media_id", "effective_object_story_id", "enable_direct_install",
    "enable_launch_instant_app", "facebook_branded_content", "image_crops", "image_hash",
    "image_url", "instagram_permalink_url", "instagram_user_id", "interactive_components_spec",
    "link_destination_display_url", "link_og_id", "link_url", "messenger_sponsored_message",
    "name", "object_id", "object_store_url", "object_story_id", "object_story_spec",
    "object_type", "object_url", "page_welcome_message", "photo_album_source_object_story_id",
    "place_page_set_id", "platform_customizations", "playable_asset_id", "portrait_customizations",
    "product_data", "product_set_id", "recommender_settings", "referral_id",
    "source_instagram_media_id", "status", "template_url", "template_url_spec",
    "thumbnail_id", "thumbnail_url", "title", "url_tags", "use_page_actor_override",
    "video_id"
]

# -------------------------------------------------------------------
# EXTRAER TODOS LOS AD CREATIVES
# -------------------------------------------------------------------
def fetch_all_ad_creatives():
    """
    Obtiene todos los Ad Creatives de la cuenta publicitaria.
    Maneja paginación y espera si se alcanza el rate limit.
    """
    session = requests.Session()
    url = (
        f"https://graph.facebook.com/v16.0/act_{AD_ACCOUNT_ID}/adcreatives"
        f"?fields={','.join(ALL_FIELDS)}"
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
                    logger.warning("Rate limit alcanzado en /adcreatives. Esperando 60 seg antes de continuar...")
                    time.sleep(60)
                    continue  # Reintentar

                logger.error(f"Error en API: {data['error']}")
                break

            ad_creatives.extend(data.get("data", []))

            url = data.get("paging", {}).get("next")
            time.sleep(0.5)

        except requests.exceptions.RequestException as e:
            logger.error(f"Error de conexión: {e}")
            break

    logger.info(f"Se encontraron {len(ad_creatives)} Ad Creatives.")
    return ad_creatives

# -------------------------------------------------------------------
# CARGAR DATOS A BIGQUERY
# -------------------------------------------------------------------
def load_to_bigquery(df):
    """
    Carga un DataFrame con todos los Ad Creatives a BigQuery.
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

        df.drop_duplicates(subset=["id"], inplace=True)

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
    Extrae todos los Ad Creatives y los carga en BigQuery.
    """
    logger.info("Obteniendo todos los Ad Creatives de la cuenta...")
    ad_creatives = fetch_all_ad_creatives()

    if ad_creatives:
        df = pd.DataFrame(ad_creatives)
        load_to_bigquery(df)

    logger.info("Proceso de extracción y carga de Ad Creatives completado.")

# -------------------------------------------------------------------
# EJECUCIÓN
# -------------------------------------------------------------------
if __name__ == "__main__":
    extract_creatives_meta()
