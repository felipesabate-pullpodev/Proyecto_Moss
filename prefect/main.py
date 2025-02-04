import os
import sys
import subprocess
import time
import logging
from datetime import datetime
import requests
from dotenv import load_dotenv
from prefect import flow, task

# Configuración del logger para incluir hora, nivel y mensaje
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%H:%M:%S"
)

# Cargar las variables de entorno desde el archivo .env
load_dotenv()

@task
def start_sync(airbyte_url: str, jwt_token: str, connection_id: str):
    """
    Inicia la sincronización de una conexión en Airbyte.
    """
    try:
        url = f"{airbyte_url}/api/v1/connections/sync"
        headers = {"Authorization": f"Bearer {jwt_token}"}
        payload = {"connectionId": connection_id}
        
        response = requests.post(url, json=payload, headers=headers)
        response.raise_for_status()
        
        sync_data = response.json()
        job_id = sync_data.get("job", {}).get("id")
        if job_id:
            logging.info(f"Sincronización iniciada. Job ID: {job_id}")
            return job_id
        else:
            logging.error("Error: No se obtuvo ningún Job ID.")
            return None
    except requests.exceptions.RequestException as e:
        logging.error(f"Error al iniciar sincronización: {e}")
        raise

@task
def monitor_sync(airbyte_url: str, jwt_token: str, job_id: str):
    """
    Monitorea la sincronización en Airbyte hasta que finalice.
    """
    try:
        url = f"{airbyte_url}/api/v1/jobs/get"
        headers = {"Authorization": f"Bearer {jwt_token}"}
        payload = {"id": job_id}
        
        start_time = datetime.now()
        while True:
            response = requests.post(url, json=payload, headers=headers)
            response.raise_for_status()
            
            job_data = response.json()
            job_status = job_data.get("job", {}).get("status")
            logging.info(f"Estado del Job {job_id}: {job_status}")
            
            if job_status in ["succeeded", "failed", "cancelled"]:
                end_time = datetime.now()
                duration = (end_time - start_time).total_seconds()
                logging.info(f"Estado final del Job {job_id}: {job_status}")
                logging.info(f"Duración total: {duration:.2f} segundos")
                
                if job_status == "succeeded":
                    stats = job_data.get("job", {}).get("attempts", [{}])[-1].get("attemptStats", {})
                    bytes_synced = stats.get("syncStats", {}).get("bytesSynced", 0)
                    records_synced = stats.get("syncStats", {}).get("recordsSynced", 0)
                    logging.info(f"Datos transferidos: {bytes_synced / (1024 ** 2):.2f} MB")
                    logging.info(f"Registros sincronizados: {records_synced}")
                elif job_status in ["failed", "cancelled"]:
                    raise RuntimeError(f"Sincronización fallida o cancelada. Estado final: {job_status}")
                break
            
            time.sleep(10)
    except requests.exceptions.RequestException as e:
        logging.error(f"Error al monitorear la sincronización: {e}")
        raise

@task
def run_carga_diaria():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    carga_path = os.path.join(base_dir, "..", "bsale", "components", "documentos", "carga_diaria.py")
    carga_path = os.path.abspath(carga_path)
    
    logging.info(f"Iniciando ejecución de carga diaria desde: {carga_path}")
    start_time = datetime.now()
    
    result = subprocess.run(
        [sys.executable, carga_path],
        capture_output=True,
        text=True,
        encoding="cp1252"
    )
    
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    logging.info(f"Duración de la ejecución de carga diaria: {duration:.2f} segundos")
    
    if result.stdout:
        for line in result.stdout.splitlines():
            logging.info(f"carga_diaria STDOUT: {line}")
    if result.stderr:
        for line in result.stderr.splitlines():
            logging.error(f"carga_diaria STDERR: {line}")
    
    if result.returncode != 0:
        raise RuntimeError("Error en la ejecución de carga diaria.")
    
    logging.info("Ejecución de carga diaria completada con éxito.")
    return result.stdout

@task
def run_dbt_run():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.abspath(os.path.join(base_dir, ".."))
    dbt_project_dir = os.path.join(project_root, "dbt_project")
    logging.info(f"Iniciando ejecución de dbt run en: {dbt_project_dir}")
    start_time = datetime.now()
    
    result = subprocess.run(
        ["dbt", "run", "--models", "mart.core_document_explode"],
        cwd=dbt_project_dir,
        capture_output=True,
        text=True,
        encoding="utf-8"
    )
    
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    logging.info(f"Duración de dbt run: {duration:.2f} segundos")
    
    if result.stdout:
        for line in result.stdout.splitlines():
            logging.info(f"dbt STDOUT: {line}")
    if result.stderr:
        for line in result.stderr.splitlines():
            logging.error(f"dbt STDERR: {line}")
    
    if result.returncode != 0:
        raise RuntimeError("Error en dbt run.")
    
    logging.info("dbt run finalizado con éxito.")
    return result.stdout

@flow
def daily_flow():
    # Variables de entorno
    AIRBYTE_URL = os.getenv('AIRBYTE_URL')
    JWT_TOKEN = os.getenv('AIRBYTE_JWT_TOKEN')
    CONNECTION_ID = os.getenv('AIRBYTE_CONNECTION_ID')
    
    # Paso 1: Sincronización de Airbyte
    job_id = start_sync(AIRBYTE_URL, JWT_TOKEN, CONNECTION_ID)
    monitor_sync(AIRBYTE_URL, JWT_TOKEN, job_id)

    # Paso 2: Carga diaria
    run_carga_diaria()
    
    # Paso 3: Ejecución de dbt
    time.sleep(10)  # Espera opcional
    run_dbt_run()

if __name__ == "__main__":
    daily_flow.deploy(
        name="daily_job",
        work_pool_name="mi_work_pool",
        cron="0 0 * * *",
    )

#  Si queremos probar 
# if __name__ == "__main__":
#     daily_flow()