import os
import requests
import json
import sys
import logging
import time
import pandas as pd
from datetime import datetime, timedelta
import pytz
from ratelimit import limits, sleep_and_retry  # Para el rate limiter

from dotenv import load_dotenv
from google.cloud import bigquery

# -----------------------------------------------------------------------------
# Configuraciones para Rate Limiting
# -----------------------------------------------------------------------------
CALLS = 50  # Número máximo de llamadas permitidas (reducido de 100 a 50)
PERIOD = 60  # Periodo en segundos

@sleep_and_retry
@limits(calls=CALLS, period=PERIOD)
def rate_limited_get(url, timeout=60):
    """Función que envuelve requests.get aplicando un límite de llamadas."""
    return requests.get(url, timeout=timeout)

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
# Variables de entorno / Credenciales
AD_ACCOUNT_ID = os.getenv("FB_AD_ACCOUNT_ID")     
ACCESS_TOKEN = os.getenv("FB_ACCESS_TOKEN")       
BIGQUERY_PROJECT_ID = os.getenv("BIGQUERY_PROJECT_ID")
BIGQUERY_DATASET = os.getenv("BIGQUERY_DATASET")
BIGQUERY_TABLE = 'meta_insights'
STAGING_TABLE = 'meta_insights_staging'   # Tabla de staging para el MERGE
BIGQUERY_KEY_PATH = os.getenv("BIGQUERY_KEY_PATH")

# Niveles de batch
BATCH_SIZE = 500        # Batch interno: cada 500 registros se carga a staging y se hace MERGE
MAX_RECORDS = 20000     # Batch global: cada 20,000 registros se fuerza una carga + reset

CHILE_TZ = pytz.timezone("America/Santiago")

# -----------------------------------------------------------------------------
# 2) Función para cargar datos a BigQuery con upsert (MERGE)
# -----------------------------------------------------------------------------
def load_to_bigquery_upsert(df):
    """
    Carga un DataFrame a una tabla de staging en BigQuery (creándola si no existe)
    y luego ejecuta un MERGE para insertar o actualizar (upsert) en la tabla final.
    """
    if df.empty:
        logger.info("No hay datos nuevos para cargar en BigQuery.")
        return

    # Función auxiliar para convertir a entero
    def convert_to_int(value):
        try:
            # Convertir el valor a entero si es numérico o una cadena numérica
            # Si es una cadena vacía o no convertible, devuelve None
            if pd.isna(value) or str(value).strip() == "":
                return None
            return int(float(value))
        except Exception as e:
            logger.warning(f"Error al convertir '{value}' a int: {e}")
            return None

    # Convertir columnas específicas a Int64 (nullable)
    for col in ["daily_budget_campaign", "lifetime_budget_campaign", "budget_remaining_campaign"]:
        if col in df.columns:
            df[col] = df[col].apply(convert_to_int).astype("Int64")

    try:
        client = bigquery.Client.from_service_account_json(
            BIGQUERY_KEY_PATH,
            project=BIGQUERY_PROJECT_ID
        )
        destination_table = f"{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET}.{BIGQUERY_TABLE}"
        staging_table = f"{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET}.{STAGING_TABLE}"

        # 1. Cargar los datos a la tabla de staging
        job_config = bigquery.LoadJobConfig(
            create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            autodetect=True
        )
        load_job = client.load_table_from_dataframe(df, staging_table, job_config=job_config)
        load_job.result()  # Espera a que termine la carga
        logger.info(f"Cargados {len(df)} registros en la tabla de staging {staging_table}.")

        # 2. Asegurarnos de que la tabla final exista
        init_query = f"""
        CREATE TABLE IF NOT EXISTS `{destination_table}`
        AS SELECT * FROM `{staging_table}`
        WHERE 1=0
        """
        client.query(init_query).result()

        # 3. Ejecutar MERGE para insertar o actualizar (upsert) en la tabla final
        merge_query = f"""
        MERGE `{destination_table}` T
        USING `{staging_table}` S
        ON T.id = S.id
        WHEN MATCHED THEN
          UPDATE SET
            ad_id = S.ad_id,
            ad_name = S.ad_name,
            adset_id = S.adset_id,
            adset_name = S.adset_name,
            campaign_id = S.campaign_id,
            campaign_name = S.campaign_name,
            impressions = S.impressions,
            clicks = S.clicks,
            ctr = S.ctr,
            spend = S.spend,
            date_start = S.date_start,
            date_stop = S.date_stop,
            actions = S.actions,
            action_values = S.action_values,
            purchases = S.purchases,
            purchase_value = S.purchase_value,
            status = S.status,
            effective_status = S.effective_status,
            daily_budget_adset = S.daily_budget_adset,
            lifetime_budget_adset = S.lifetime_budget_adset,
            budget_remaining_adset = S.budget_remaining_adset,
            daily_budget_campaign = S.daily_budget_campaign,
            lifetime_budget_campaign = S.lifetime_budget_campaign,
            budget_remaining_campaign = S.budget_remaining_campaign
        WHEN NOT MATCHED THEN
          INSERT (
            id, ad_id, ad_name, adset_id, adset_name, campaign_id, campaign_name,
            impressions, clicks, ctr, spend, date_start, date_stop,
            actions, action_values, purchases, purchase_value, status, effective_status,
            daily_budget_adset, lifetime_budget_adset, budget_remaining_adset,
            daily_budget_campaign, lifetime_budget_campaign, budget_remaining_campaign
          )
          VALUES(
            S.id, S.ad_id, S.ad_name, S.adset_id, S.adset_name, S.campaign_id, S.campaign_name,
            S.impressions, S.clicks, S.ctr, S.spend, S.date_start, S.date_stop,
            S.actions, S.action_values, S.purchases, S.purchase_value, S.status, S.effective_status,
            S.daily_budget_adset, S.lifetime_budget_adset, S.budget_remaining_adset,
            S.daily_budget_campaign, S.lifetime_budget_campaign, S.budget_remaining_campaign
          )
        """
        merge_job = client.query(merge_query)
        merge_job.result()

        logger.info("Carga y actualización completadas mediante MERGE.")

    except Exception as e:
        logger.error(f"Error al cargar datos en BigQuery: {e}")

# -----------------------------------------------------------------------------
# 3) Función para obtener TODOS los anuncios y su status de la cuenta
#    (timeout y reintentos ante Timeout)
# -----------------------------------------------------------------------------
def fetch_all_ads_status():
    """Obtiene el estado de los anuncios con manejo de rate limits y reintentos."""
    ads_status_map = {}
    url = (
        f"https://graph.facebook.com/v16.0/act_{AD_ACCOUNT_ID}/ads"
        f"?fields=id,name,status,effective_status"
        f"&access_token={ACCESS_TOKEN}"
    )

    retries = 0
    max_retries = 5
    wait_time = 300

    while True:
        try:
            response = rate_limited_get(url, timeout=60)
            data = response.json()

            if "error" in data:
                error_data = data["error"]
                if error_data.get("code") == 17:
                    retries += 1
                    if retries > max_retries:
                        logger.error("❌ Se alcanzó el límite de reintentos. Abortando extracción.")
                        break
                    logger.warning(f"🚨 Límite de llamadas alcanzado. Pausando {wait_time//60} minutos...")
                    time.sleep(wait_time)
                    wait_time *= 2
                    continue
                else:
                    logger.error(f"❌ Error en la API de Meta: {error_data}")
                    break

            ads_data = data.get("data", [])
            for ad in ads_data:
                ad_id = ad.get("id")
                if ad_id:
                    ads_status_map[ad_id] = {
                        "status": ad.get("status", "unknown"),
                        "effective_status": ad.get("effective_status", "unknown"),
                    }

            paging = data.get("paging", {})
            next_page = paging.get("next")
            if not next_page:
                logger.info("✅ No hay más páginas en /ads.")
                break
            else:
                logger.info(f"✅ Obtenidos {len(ads_data)} anuncios, avanzando a la siguiente página...")
                url = next_page
                time.sleep(5)
        except requests.exceptions.Timeout:
            logger.warning("⚠️ Timeout en Ads. Reintentando en 30 segundos...")
            time.sleep(30)
            continue
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Error en la API de Ads: {e}")
            break

    logger.info(f"✅ Total de anuncios obtenidos: {len(ads_status_map)}")
    return ads_status_map

# -----------------------------------------------------------------------------
# 3.1) Función para obtener información de presupuesto de los ad sets (ABO)
# -----------------------------------------------------------------------------
def fetch_adset_budgets():
    """Obtiene la información de presupuesto de los conjuntos de anuncios (ad sets)."""
    adset_budgets = {}
    url = (
        f"https://graph.facebook.com/v16.0/act_{AD_ACCOUNT_ID}/adsets"
        f"?fields=id,name,daily_budget,lifetime_budget,budget_remaining,campaign_id"
        f"&access_token={ACCESS_TOKEN}"
    )
    while True:
        try:
            response = rate_limited_get(url, timeout=60)
            data = response.json()
            if "error" in data:
                logger.error(f"Error al obtener ad sets: {data['error']}")
                break
            for adset in data.get("data", []):
                adset_id = adset["id"]
                adset_budgets[adset_id] = {
                    "name": adset.get("name"),
                    "daily_budget": adset.get("daily_budget"),
                    "lifetime_budget": adset.get("lifetime_budget"),
                    "budget_remaining": adset.get("budget_remaining"),
                    "campaign_id": adset.get("campaign_id"),
                }
            paging = data.get("paging", {})
            next_page = paging.get("next")
            if not next_page:
                break
            url = next_page
            time.sleep(5)
        except requests.exceptions.Timeout:
            logger.warning("Se agotó el tiempo de espera al consultar ad sets. Reintentando en 30 seg...")
            time.sleep(30)
            continue
        except requests.exceptions.RequestException as e:
            logger.error(f"Error al obtener ad sets: {e}")
            break
    logger.info(f"Se obtuvieron {len(adset_budgets)} conjuntos de anuncios con presupuesto.")
    return adset_budgets

# -----------------------------------------------------------------------------
# 3.2) Función para obtener información de presupuesto de las campañas (CBO)
# -----------------------------------------------------------------------------
def fetch_campaign_budgets():
    """Obtiene la información de presupuesto de las campañas (CBO)."""
    campaign_budgets = {}
    url = (
        f"https://graph.facebook.com/v16.0/act_{AD_ACCOUNT_ID}/campaigns"
        f"?fields=id,name,daily_budget,lifetime_budget,budget_remaining"
        f"&access_token={ACCESS_TOKEN}"
    )
    while True:
        try:
            response = rate_limited_get(url, timeout=60)
            data = response.json()
            if "error" in data:
                logger.error(f"Error al obtener campañas: {data['error']}")
                break
            for campaign in data.get("data", []):
                campaign_id = campaign["id"]
                campaign_budgets[campaign_id] = {
                    "name": campaign.get("name"),
                    "daily_budget": campaign.get("daily_budget"),
                    "lifetime_budget": campaign.get("lifetime_budget"),
                    "budget_remaining": campaign.get("budget_remaining"),
                }
            paging = data.get("paging", {})
            next_page = paging.get("next")
            if not next_page:
                break
            url = next_page
            time.sleep(5)
        except requests.exceptions.Timeout:
            logger.warning("Se agotó el tiempo de espera al consultar campañas. Reintentando en 30 seg...")
            time.sleep(30)
            continue
        except requests.exceptions.RequestException as e:
            logger.error(f"Error al obtener campañas: {e}")
            break
    logger.info(f"Se obtuvieron {len(campaign_budgets)} campañas con presupuesto.")
    return campaign_budgets

# -----------------------------------------------------------------------------
# 4) Paginación en la API de Insights (nivel=ad) con timeout y reintentos
# -----------------------------------------------------------------------------
def fetch_all_insights(base_url):
    """Llama a la API de Insights con paginación y manejo de rate limits."""
    all_data = []
    url = base_url
    retries = 0
    max_retries = 5
    wait_time = 30
    while True:
        try:
            response = rate_limited_get(url, timeout=60)
            data = response.json()
            if "error" in data:
                error_data = data["error"]
                if error_data.get("code") == 17:
                    retries += 1
                    if retries > max_retries:
                        logger.error("❌ Se alcanzó el límite de reintentos. Abortando extracción.")
                        break
                    logger.warning(f"🚨 Rate limit alcanzado. Reintentando en {wait_time} segundos...")
                    time.sleep(wait_time)
                    wait_time *= 2
                    continue
                else:
                    logger.error(f"❌ Error en la API de Meta: {error_data}")
                    break
            insights = data.get("data", [])
            all_data.extend(insights)
            logger.info(f"✅ Recibidos {len(insights)} registros en esta página.")
            paging = data.get("paging", {})
            next_page = paging.get("next")
            if not next_page:
                logger.info("✅ No hay más páginas de Insights.")
                break
            else:
                url = next_page
                logger.info("🔄 Avanzando a la siguiente página...")
                time.sleep(5)
        except requests.exceptions.Timeout:
            logger.warning("⚠️ Timeout en Insights. Reintentando en 30 segundos...")
            time.sleep(30)
            continue
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Error en la API de Insights: {e}")
            break
    return all_data

# -----------------------------------------------------------------------------
# 5) Procesar cada fila de Insights (crear 'id' único) + status + presupuesto ABO
# -----------------------------------------------------------------------------
def process_insight(insight, ads_status_map, adset_budgets, campaign_budgets):
    try:
        ad_id = insight.get("ad_id", "unknown")
        adset_id = insight.get("adset_id", "unknown")
        campaign_id = insight.get("campaign_id", "unknown")
        date_start = insight.get("date_start", "unknown")
        unique_id = f"{ad_id}_{date_start}"

        ad_status_info = ads_status_map.get(ad_id, {})
        status = ad_status_info.get("status", "unknown")
        effective_status = ad_status_info.get("effective_status", "unknown")

        daily_budget_adset = None
        lifetime_budget_adset = None
        budget_remaining_adset = None
        if adset_id in adset_budgets:
            daily_budget_adset = adset_budgets[adset_id].get("daily_budget")
            lifetime_budget_adset = adset_budgets[adset_id].get("lifetime_budget")
            budget_remaining_adset = adset_budgets[adset_id].get("budget_remaining")

        daily_budget_campaign = None
        lifetime_budget_campaign = None
        budget_remaining_campaign = None
        if campaign_id in campaign_budgets:
            daily_budget_campaign = campaign_budgets[campaign_id].get("daily_budget")
            lifetime_budget_campaign = campaign_budgets[campaign_id].get("lifetime_budget")
            budget_remaining_campaign = campaign_budgets[campaign_id].get("budget_remaining")
            try:
                daily_budget_campaign = int(daily_budget_campaign) if daily_budget_campaign is not None else None
            except Exception as e:
                logger.warning(f"Error al convertir daily_budget_campaign '{daily_budget_campaign}' a int: {e}")
                daily_budget_campaign = None
            try:
                lifetime_budget_campaign = int(lifetime_budget_campaign) if lifetime_budget_campaign is not None else None
            except Exception as e:
                logger.warning(f"Error al convertir lifetime_budget_campaign '{lifetime_budget_campaign}' a int: {e}")
                lifetime_budget_campaign = None
            try:
                budget_remaining_campaign = int(budget_remaining_campaign) if budget_remaining_campaign is not None else None
            except Exception as e:
                logger.warning(f"Error al convertir budget_remaining_campaign '{budget_remaining_campaign}' a int: {e}")
                budget_remaining_campaign = None

        purchase_types = {
            "purchase",
            "onsite_web_purchase",
            "onsite_web_app_purchase",
            "offsite_conversion.fb_pixel_purchase",
            "omni_purchase",
            "web_in_store_purchase"
        }

        actions = insight.get("actions", [])
        actions_json = json.dumps(actions)
        purchases = 0
        for action in actions:
            if action.get("action_type") in purchase_types:
                purchases += int(action.get("value", 0))

        action_values = insight.get("action_values", [])
        action_values_json = json.dumps(action_values)
        purchase_value = 0.0
        for av in action_values:
            if av.get("action_type") in purchase_types:
                purchase_value += float(av.get("value", 0))

        return {
            "id": unique_id,
            "ad_id": ad_id,
            "ad_name": insight.get("ad_name"),
            "adset_id": adset_id,
            "adset_name": insight.get("adset_name"),
            "campaign_id": campaign_id,
            "campaign_name": insight.get("campaign_name"),
            "impressions": insight.get("impressions"),
            "clicks": insight.get("clicks"),
            "ctr": insight.get("ctr"),
            "spend": insight.get("spend"),
            "date_start": date_start,
            "date_stop": insight.get("date_stop", "unknown"),
            "actions": actions_json,
            "action_values": action_values_json,
            "purchases": purchases,
            "purchase_value": purchase_value,
            "status": status,
            "effective_status": effective_status,
            "daily_budget_adset": daily_budget_adset,
            "lifetime_budget_adset": lifetime_budget_adset,
            "budget_remaining_adset": budget_remaining_adset,
            "daily_budget_campaign": daily_budget_campaign,
            "lifetime_budget_campaign": lifetime_budget_campaign,
            "budget_remaining_campaign": budget_remaining_campaign
        }
    except Exception as e:
        logger.error(f"Error al procesar la fila de Insights: {e}")
        return None

# -----------------------------------------------------------------------------
# 6) Función principal de extracción + carga (con batch interno y global)
# -----------------------------------------------------------------------------
def extract_insights_meta(start_date=None, end_date=None):
    """
    Extrae Insights a nivel de anuncio para un rango de fechas.
    Si no se especifican start_date y end_date, se extraen los datos de los 2 últimos días.
    Los parámetros deben estar en formato "YYYY-MM-DD".
    """
    now_chile = datetime.now(CHILE_TZ)
    if start_date is None or end_date is None:
        start_date = (now_chile - timedelta(days=2)).strftime("%Y-%m-%d")
        end_date = now_chile.strftime("%Y-%m-%d")
    logger.info(f"Extrayendo Insights (ad level) desde {start_date} hasta {end_date}.")

    ads_status_map = fetch_all_ads_status()
    adset_budgets = fetch_adset_budgets()
    campaign_budgets = fetch_campaign_budgets()

    base_url = (
        f"https://graph.facebook.com/v16.0/act_{AD_ACCOUNT_ID}/insights"
        f"?level=ad"
        f"&fields=ad_id,ad_name,adset_id,adset_name,campaign_id,campaign_name,"
        f"impressions,clicks,ctr,spend,actions,action_values"
        f"&action_breakdowns=action_type"
        f"&time_increment=1"
        f"&time_range={{'since':'{start_date}','until':'{end_date}'}}"
        f"&access_token={ACCESS_TOKEN}"
    )

    all_insights = fetch_all_insights(base_url)
    buffer = []
    new_records = 0
    count_total = 0

    for insight in all_insights:
        record = process_insight(insight, ads_status_map, adset_budgets, campaign_budgets)
        if not record:
            continue
        buffer.append(record)
        new_records += 1
        count_total += 1
        time.sleep(0.1)
        if len(buffer) >= BATCH_SIZE:
            df = pd.DataFrame(buffer)
            load_to_bigquery_upsert(df)
            buffer = []
        if count_total >= MAX_RECORDS:
            if buffer:
                df = pd.DataFrame(buffer)
                load_to_bigquery_upsert(df)
                buffer = []
            logger.info(f"Alcanzado el límite global de {MAX_RECORDS} registros. Reseteando contador.")
            count_total = 0

    if buffer:
        df = pd.DataFrame(buffer)
        load_to_bigquery_upsert(df)
    logger.info(f"Finalizado. Se procesaron {new_records} registros nuevos.")

# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    extract_insights_meta(start_date="2025-03-01", end_date="2025-03-19")
