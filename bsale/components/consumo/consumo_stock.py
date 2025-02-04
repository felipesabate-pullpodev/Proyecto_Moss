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
BQ_TABLE = "bsale_stock_consumptions"  # Nombre de la tabla para consumos

# Configuración de la API
BASE_URL = "https://api.bsale.io/v1/stocks/consumptions.json"

def obtener_consumos():
    """
    Extrae todos los consumos de stock desde la API de Bsale con detalles y oficinas.
    """
    headers = {
        'Content-Type': 'application/json',
        'access_token': API_TOKEN
    }
    params = {'expand': '[office,details]', 'limit': 100, 'offset': 0}  # Límite para pruebas

    registros = []

    while True:
        print(f"Consultando consumos de stock con offset {params['offset']}...")
        response = requests.get(BASE_URL, headers=headers, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()

        if "items" in data and data["items"]:
            for consumo in data["items"]:
                consumo_id = consumo["id"]
                consumption_date = consumo.get("consumptionDate", None)
                note = consumo.get("note", "")
                office_id = consumo["office"]["id"]
                office_name = consumo["office"]["name"]

                # Recorrer los detalles del consumo
                for detalle in consumo["details"]["items"]:
                    registros.append({
                        "consumption_id": consumo_id,
                        "consumption_date": consumption_date,
                        "note": note,
                        "office_id": office_id,
                        "office_name": office_name,
                        "variant_id": detalle["variant"]["id"],
                        "quantity": detalle["quantity"],
                        "cost": detalle["cost"]
                    })

            # Si hay más páginas, avanzar en el offset
            if len(data["items"]) < params["limit"]:
                break  # No hay más registros
            params["offset"] += params["limit"]

        else:
            print("No se encontraron más consumos de stock.")
            break

    return registros

def transformar_a_dataframe(registros):
    """
    Convierte los datos extraídos en un DataFrame de Pandas.
    """
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

# Ejecutar extracción y carga a BigQuery
consumos_data = obtener_consumos()

if consumos_data:
    df_consumo = transformar_a_dataframe(consumos_data)
    cargar_a_bigquery(df_consumo)
else:
    print("No se encontraron consumos de stock.")
