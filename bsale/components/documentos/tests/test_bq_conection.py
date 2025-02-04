import os
from dotenv import load_dotenv
from google.cloud import bigquery

def test_bigquery_connection():
    """
    Verifica la conexión a BigQuery y hace una consulta de prueba.
    """
    try:
        # Cargar variables del archivo .env
        load_dotenv()

        # Ahora las variables están disponibles en el entorno
        key_path = os.getenv("BIGQUERY_KEY_PATH")
        project_id = os.getenv("BIGQUERY_PROJECT_ID")

        # Crea el cliente de BigQuery
        client = bigquery.Client.from_service_account_json(key_path, project=project_id)
        
        # 1) Prueba listando datasets en el proyecto
        print("Listando datasets en el proyecto...")
        datasets = list(client.list_datasets())
        if datasets:
            print(f"Se encontraron {len(datasets)} dataset(s):")
            for dataset in datasets:
                print(f" - {dataset.dataset_id}")
        else:
            print("No se encontraron datasets. (¿Está vacío el proyecto?)")

        # 2) Prueba ejecutando una consulta simple
        print("\nEjecutando una consulta de prueba: SELECT 1 as test_col ...")
        query = "SELECT 1 as test_col"
        query_job = client.query(query)
        results = query_job.result()

        for row in results:
            print(f"Consulta exitosa, valor obtenido: {row.test_col}")

        print("\n¡Conexión y consulta de prueba a BigQuery finalizadas con éxito!")
    except Exception as e:
        print(f"Ocurrió un error al conectar o consultar BigQuery: {e}")

if __name__ == "__main__":
    test_bigquery_connection()
