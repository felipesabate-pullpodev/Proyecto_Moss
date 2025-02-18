import os
import requests
import json
import sys
import logging
import time
import pandas as pd
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
from google.cloud import bigquery

# ---------------------------------------------------------------------
# 1) Configuración General
# ---------------------------------------------------------------------
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# Variables de entorno
SHOPIFY_STORE = os.getenv("SHOPIFY_STORE")
SHOPIFY_API_TOKEN = os.getenv("SHOPIFY_API_TOKEN")
BIGQUERY_PROJECT_ID = os.getenv("BIGQUERY_PROJECT_ID")
BIGQUERY_DATASET = os.getenv("BIGQUERY_DATASET")
BIGQUERY_TABLE = os.getenv("BIGQUERY_TABLE_ORDERS_SHOPIFY")
BIGQUERY_KEY_PATH = os.getenv("BIGQUERY_KEY_PATH")

# Shopify API URL
BASE_URL = f"https://{SHOPIFY_STORE}.myshopify.com/admin/api/2024-01/orders.json"

# Configuración de paginación y rate limit
REQUEST_DELAY = 1  # Evitar bloqueos de API (puedes subirlo a 2-3 segundos si es necesario)
MAX_PAGES = 100  # Máximo de páginas a extraer (ajusta según necesidad)

# ---------------------------------------------------------------------
# 2) Función para obtener órdenes de Shopify con paginación
# ---------------------------------------------------------------------
def fetch_orders(days_back):
    """
    Extrae TODAS las órdenes de Shopify usando `page_info` para paginar correctamente.
    Se implementa una pausa entre requests para evitar bloqueos por rate limit.
    """
    all_orders = []
    total_pages = 0  # Contador de páginas

    # ✅ Fecha de inicio basada en `DAYS_BACK`
    start_date = (datetime.now(timezone.utc) - timedelta(days=days_back)).isoformat()

    # Parámetros iniciales
    params = {
        "limit": 250,  # Shopify permite un máximo de 250 registros por request
        "status": "any",  # Traer todas las órdenes (no solo abiertas)
        "created_at_min": start_date  # Filtrar órdenes creadas después de esta fecha
    }

    headers = {
        "X-Shopify-Access-Token": SHOPIFY_API_TOKEN,
        "Content-Type": "application/json"
    }

    next_page_url = BASE_URL  # Inicialmente usamos la URL base

    while next_page_url and total_pages < MAX_PAGES:
        response = requests.get(next_page_url, headers=headers, params=params if total_pages == 0 else {})

        if response.status_code == 200:
            data = response.json()
            orders = data.get("orders", [])

            if not orders:
                break  # No hay más órdenes

            all_orders.extend(orders)  # Agregar órdenes a la lista
            total_pages += 1
            logger.info(f"📦 Página {total_pages}: Extraídas {len(orders)} órdenes. Total acumulado: {len(all_orders)}.")

            # ✅ Extraer `next_page_info` desde los Headers
            next_page_url = None
            if "Link" in response.headers:
                links = response.headers["Link"].split(", ")
                for link in links:
                    if 'rel="next"' in link:
                        next_page_url = link.split(";")[0].strip("<>")
                        break  # Usar solo el primer `next` encontrado

            if not next_page_url:
                break  # No hay más páginas

            # ✅ Pausa entre requests para evitar rate limit
            time.sleep(REQUEST_DELAY)

        else:
            logger.error(f"❌ Error al obtener órdenes: {response.status_code} - {response.text}")
            break

    logger.info(f"✅ Extracción finalizada. Total de órdenes obtenidas: {len(all_orders)}.")
    return all_orders

# ---------------------------------------------------------------------
# 3) Función para cargar datos en BigQuery en bloques (Batches)
# ---------------------------------------------------------------------
def load_to_bigquery(df):
    """
    Carga un DataFrame en BigQuery sin duplicados y crea la tabla si no existe.
    Se obtiene la lista de IDs existentes antes de insertar nuevos datos.
    """
    if df.empty:
        logger.info("⚠️ No hay datos nuevos para cargar en BigQuery.")
        return

    try:
        client = bigquery.Client.from_service_account_json(
            BIGQUERY_KEY_PATH,
            project=BIGQUERY_PROJECT_ID
        )
        table_id = f"{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET}.{BIGQUERY_TABLE}"

        # ✅ Verificar si la tabla existe
        table_exists = True
        try:
            client.get_table(table_id)  # Intenta obtener la tabla
            logger.info("✅ La tabla ya existe en BigQuery.")
        except Exception:
            table_exists = False
            logger.info("⚠️ La tabla no existe. Se creará automáticamente.")

        # ✅ Obtener IDs existentes en BigQuery solo si la tabla ya existe
        if table_exists:
            query = f"SELECT DISTINCT id FROM `{table_id}`"
            existing_ids_df = client.query(query).to_dataframe()

            if not existing_ids_df.empty:
                existing_ids = set(existing_ids_df["id"].astype(str))
                df = df[~df["id"].astype(str).isin(existing_ids)]
                logger.info(f"✅ Filtrados {len(existing_ids)} registros ya existentes en BigQuery.")

        if df.empty:
            logger.info("⚠️ No hay datos nuevos para insertar después de eliminar duplicados.")
            return

        # ✅ Convertir TODOS los valores a `str` para evitar errores de tipo
        df = df.astype(str)

        # ✅ Convertir JSON anidados a string
        nested_columns = [
            "billing_address", "customer", "discount_applications",
            "fulfillments", "line_items", "payment_terms",
            "refunds", "shipping_address", "shipping_lines"
        ]
        for col in nested_columns:
            if col in df.columns:
                df[col] = df[col].apply(lambda x: json.dumps(x) if isinstance(x, (dict, list)) else str(x))

        # ✅ Configuración de carga en BigQuery
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,  # ✅ CREA TABLA SI NO EXISTE
            autodetect=True
        )

        # ✅ Cargar datos a BigQuery
        load_job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        load_job.result()  # Esperar la carga

        logger.info(f"✅ Cargados {len(df)} registros en BigQuery sin duplicados.")

    except Exception as e:
        logger.error(f"❌ Error al cargar datos en BigQuery: {e}")


# ---------------------------------------------------------------------
# 4) Función principal
# ---------------------------------------------------------------------
def extract_shopify_orders(days_back=800):
    """
    Obtiene todas las órdenes de Shopify y las sube a BigQuery.
    """
    start_time = time.time()
    logger.info("🚀 Iniciando extracción de órdenes de Shopify...")

    orders = fetch_orders(days_back)

    if not orders:
        logger.info("⚠️ No se encontraron órdenes nuevas.")
        return

    logger.info(f"📦 Se encontraron {len(orders)} órdenes en Shopify.")

    # Convertir a DataFrame
    df_orders = pd.DataFrame(orders)

    # Guardar JSON con datos completos
    with open("shopify_orders.json", "w", encoding="utf-8") as file:
        json.dump(orders, file, indent=4, ensure_ascii=False)
    
    logger.info("✅ Datos guardados en shopify_orders.json")

    # Cargar a BigQuery
    load_to_bigquery(df_orders)

    end_time = time.time()
    logger.info(f"🎯 Proceso finalizado en {round(end_time - start_time, 2)} segundos.")

# ---------------------------------------------------------------------
# 5) Ejecutar script
# ---------------------------------------------------------------------
if __name__ == "__main__":
    extract_shopify_orders(days_back=800)
