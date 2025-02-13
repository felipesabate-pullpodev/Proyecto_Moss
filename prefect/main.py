import os
import sys
import subprocess
import time
import logging
from datetime import datetime
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

@task(name="Extracción Documentos Bsale")
def run_carga_diaria():
    run_script("Extracción Documentos Bsale", "../bsale/components/documentos/carga_diaria.py")

@task(name="Extracción Stock Bsale")
def run_stock_masivo_actual():
    run_script("Extracción Stock Bsale", "../bsale/components/stock/stock_masivo_actual.py")

@task(name="Extracción Meta")
def run_carga_diaria_meta():
    run_script("Extracción Meta", "../meta/carga_diaria_meta.py")

@task(name="DBT Run")
def run_dbt():
    run_dbt_run()


def run_script(task_name, script_path):
    logging.info(f"Iniciando {task_name} desde: {script_path}")
    result = subprocess.run(
        [sys.executable, script_path],
        capture_output=True,
        text=True,
        encoding="utf-8"
    )
    if result.stdout:
        logging.info(f"{task_name} STDOUT:\n{result.stdout}")
    if result.stderr:
        logging.error(f"{task_name} STDERR:\n{result.stderr}")
    if result.returncode != 0:
        raise RuntimeError(f"Error en {task_name}.")


def run_dbt_run():
    logging.info("Iniciando DBT Run")
    result = subprocess.run(
        ["dbt", "run", "--models", "*"],
        capture_output=True,
        text=True,
        encoding="utf-8"
    )
    if result.stdout:
        logging.info(f"DBT STDOUT:\n{result.stdout}")
    if result.stderr:
        logging.error(f"DBT STDERR:\n{result.stderr}")
    if result.returncode != 0:
        raise RuntimeError("Error en DBT Run.")


@flow(name="Daily ETL Flow")
def daily_flow():
    run_carga_diaria()
    run_stock_masivo_actual()
    run_carga_diaria_meta()
    run_dbt()


if __name__ == "__main__":
    daily_flow()


# # if __name__ == "__main__":
# #     daily_flow.deploy(
# #         name="daily_job",
# #         work_pool_name="mi_work_pool",
# #         cron="0 0 * * *",
# #     )

# # #  Si queremos probar 
# if __name__ == "__main__":
#      daily_flow()



