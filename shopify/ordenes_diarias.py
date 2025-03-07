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
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

SHOPIFY_STORE = os.getenv("SHOPIFY_STORE")
SHOPIFY_API_TOKEN = os.getenv("SHOPIFY_API_TOKEN")
BIGQUERY_PROJECT_ID = os.getenv("BIGQUERY_PROJECT_ID")
BIGQUERY_DATASET = os.getenv("BIGQUERY_DATASET")
BIGQUERY_TABLE = os.getenv("BIGQUERY_TABLE_ORDERS_SHOPIFY")
BIGQUERY_KEY_PATH = os.getenv("BIGQUERY_KEY_PATH")

BASE_URL = f"https://{SHOPIFY_STORE}.myshopify.com/admin/api/2024-01/orders.json"

REQUEST_DELAY = 1.5  
MAX_PAGES = 100  

# ---------------------------------------------------------------------
# 2) Función para obtener órdenes de Shopify con paginación
# ---------------------------------------------------------------------
def fetch_orders(days_back):
    all_orders = []
    total_pages = 0

    start_date = (datetime.now(timezone.utc) - timedelta(days=days_back)).isoformat()
    params = {
        "limit": 250,
        "status": "any",
        "created_at_min": start_date
    }
    headers = {
        "X-Shopify-Access-Token": SHOPIFY_API_TOKEN,
        "Content-Type": "application/json"
    }
    next_page_url = BASE_URL

    while next_page_url and total_pages < MAX_PAGES:
        response = requests.get(next_page_url, headers=headers, params=params if total_pages == 0 else {})

        if response.status_code == 200:
            data = response.json()
            orders = data.get("orders", [])

            if not orders:
                break  

            all_orders.extend(orders)
            total_pages += 1
            logger.info(f"Página {total_pages}: Extraídas {len(orders)} órdenes. Total acumulado: {len(all_orders)}.")

            next_page_url = None
            if "Link" in response.headers:
                links = response.headers["Link"].split(", ")
                for link in links:
                    if 'rel="next"' in link:
                        next_page_url = link.split(";")[0].strip("<>")
                        break  

            if not next_page_url:
                break  

            time.sleep(REQUEST_DELAY)

        else:
            logger.error(f"Error al obtener órdenes: {response.status_code} - {response.text}")
            break

    logger.info(f"Extracción finalizada. Total de órdenes obtenidas: {len(all_orders)}.")
    return all_orders

# ---------------------------------------------------------------------
# 3) Función para estructurar los datos
# ---------------------------------------------------------------------
def process_orders(orders):
    if not orders:
        return pd.DataFrame()

    processed_orders = []
    for order in orders:
        processed_orders.append({
            "id": order.get("id"),
            "admin_graphql_api_id": order.get("admin_graphql_api_id"),
            "app_id": order.get("app_id"),
            "browser_ip": order.get("browser_ip"),
            "cancel_reason": order.get("cancel_reason"),
            "cancelled_at": order.get("cancelled_at"),
            "cart_token": order.get("cart_token"),
            "checkout_id": order.get("checkout_id"),
            "closed_at": order.get("closed_at"),
            "created_at": order.get("created_at"),
            "currency": order.get("currency"),
            "current_total_price": order.get("current_total_price"),
            "customer_locale": order.get("customer_locale"),
            "device_id": order.get("device_id"),
            "discount_codes": json.dumps(order.get("discount_codes", [])),
            "email": order.get("email"),
            "financial_status": order.get("financial_status"),
            "fulfillment_status": order.get("fulfillment_status"),
            "gateway": order.get("gateway"),
            "landing_site": order.get("landing_site"),
            "name": order.get("name"),
            "note": order.get("note"),
            "number": order.get("number"),
            "order_number": order.get("order_number"),
            "payment_gateway_names": json.dumps(order.get("payment_gateway_names", [])),
            "phone": order.get("phone"),
            "processed_at": order.get("processed_at"),
            "referring_site": order.get("referring_site"),
            "subtotal_price": order.get("subtotal_price"),
            "tags": order.get("tags"),
            "taxes_included": order.get("taxes_included"),
            "total_discounts": order.get("total_discounts"),
            "total_line_items_price": order.get("total_line_items_price"),
            "total_price": order.get("total_price"),
            "total_shipping_price_set": json.dumps(order.get("total_shipping_price_set", {})),
            "total_tax": order.get("total_tax"),
            "total_weight": order.get("total_weight"),
            "updated_at": order.get("updated_at"),
            "user_id": order.get("user_id"),
            "billing_address": json.dumps(order.get("billing_address", {})),
            "customer": json.dumps(order.get("customer", {})),
            "shipping_address": json.dumps(order.get("shipping_address", {})),
            "line_items": json.dumps(order.get("line_items", []))
        })

    return pd.DataFrame(processed_orders)

# ---------------------------------------------------------------------
# 4) Función para cargar datos en BigQuery
# ---------------------------------------------------------------------
def load_to_bigquery(df):
    if df.empty:
        logger.info("No hay datos nuevos para cargar en BigQuery.")
        return

    try:
        client = bigquery.Client.from_service_account_json(
            BIGQUERY_KEY_PATH,
            project=BIGQUERY_PROJECT_ID
        )
        table_id = f"{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET}.{BIGQUERY_TABLE}"

        table_exists = True
        try:
            client.get_table(table_id)
            logger.info("La tabla ya existe en BigQuery.")
        except Exception:
            table_exists = False
            logger.info("La tabla no existe. Se creará automáticamente.")

        if table_exists:
            query = f"SELECT DISTINCT id FROM `{table_id}`"
            existing_ids_df = client.query(query).to_dataframe()

            if not existing_ids_df.empty:
                existing_ids = set(existing_ids_df["id"].astype(str))
                df = df[~df["id"].astype(str).isin(existing_ids)]
                logger.info(f"Filtrados {len(existing_ids)} registros ya existentes en BigQuery.")

        if df.empty:
            logger.info("No hay datos nuevos para insertar después de eliminar duplicados.")
            return

        df = df.astype(str)

        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
            autodetect=True
        )

        load_job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        load_job.result()

        logger.info(f"Cargados {len(df)} registros en BigQuery sin duplicados.")

    except Exception as e:
        logger.error(f"Error al cargar datos en BigQuery: {e}")

# ---------------------------------------------------------------------
# 5) Función principal
# ---------------------------------------------------------------------
def extract_shopify_orders(days_back=10):
    start_time = time.time()
    logger.info("Iniciando extracción de órdenes de Shopify...")

    orders = fetch_orders(days_back)
    if not orders:
        logger.info("No se encontraron órdenes nuevas.")
        return

    df_orders = process_orders(orders)

    with open("shopify_orders.json", "w", encoding="utf-8") as file:
        json.dump(orders, file, indent=4, ensure_ascii=False)
    
    load_to_bigquery(df_orders)

    end_time = time.time()
    logger.info(f"Proceso finalizado en {round(end_time - start_time, 2)} segundos.")

if __name__ == "__main__":
    extract_shopify_orders(days_back=11)
