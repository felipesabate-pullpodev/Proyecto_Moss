import os
import requests
import pandas as pd
from dotenv import load_dotenv
from google.cloud import bigquery

# Cargar variables de entorno desde .env
load_dotenv()

# Token de acceso de Bsale
API_TOKEN = os.getenv("BSALE_ACCESS_TOKEN")

# Configurar credenciales de BigQuery
BIGQUERY_KEY_PATH = os.getenv("BIGQUERY_KEY_PATH")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = BIGQUERY_KEY_PATH

# Configuración de BigQuery
BQ_PROJECT_ID = os.getenv("BIGQUERY_PROJECT_ID")
BQ_DATASET = os.getenv("BIGQUERY_DATASET")
BQ_TABLE = "bsale_stock_receptions"

# Configuración de la API
BASE_URL = "https://api.bsale.io/v1/stocks/receptions.json"
PAGE_SIZE = 100  # Límite de recepciones por request


def obtener_todas_las_recepciones():
    """
    Obtiene todas las recepciones de stock de Bsale con paginación.
    """
    headers = {
        'Content-Type': 'application/json',
        'access_token': API_TOKEN
    }
    params = {'expand': '[office,details]', 'limit': PAGE_SIZE, 'offset': 0}
    recepciones = []

    while True:
        print(f"Consultando recepciones de stock con offset {params['offset']}...")
        try:
            response = requests.get(BASE_URL, headers=headers, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()

            if "items" in data and data["items"]:
                recepciones.extend(data["items"])
                params['offset'] += PAGE_SIZE  # Aumentar el offset para la siguiente página
            else:
                break  # No hay más datos, salir del bucle

        except requests.exceptions.RequestException as e:
            print(f"Error en la conexión con la API: {e}")
            break

    return recepciones


def transformar_a_dataframe(recepciones):
    """
    Convierte la respuesta JSON de Bsale en un DataFrame de Pandas.
    """
    registros = []
    for recepcion in recepciones:
        for detalle in recepcion.get("details", {}).get("items", []):
            registros.append({
                "recepcion_id": recepcion["id"],
                "admission_date": recepcion["admissionDate"],
                "document": recepcion["document"],
                "document_number": recepcion.get("documentNumber", ""),
                "note": recepcion.get("note", ""),
                "office_id": recepcion["office"]["id"],
                "office_name": recepcion["office"]["name"],
                "variant_id": detalle["variant"]["id"],
                "quantity": detalle["quantity"],
                "cost": detalle["cost"]
            })

    return pd.DataFrame(registros)


def cargar_a_bigquery(df):
    """
    Carga los datos a BigQuery en la tabla especificada.
    """
    client = bigquery.Client(project=BQ_PROJECT_ID)
    table_id = f"{BQ_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}"

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND  # Añade datos sin borrar lo anterior
    )

    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()
    print(f"Datos cargados en {table_id} con {len(df)} registros.")


# Obtener todas las recepciones de stock
recepciones_data = obtener_todas_las_recepciones()

if recepciones_data:
    df_recepcion = transformar_a_dataframe(recepciones_data)
    print(df_recepcion.head())  # Mostrar una muestra antes de cargar
    cargar_a_bigquery(df_recepcion)
