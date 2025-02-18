import os
import sys
import subprocess
import logging
from datetime import datetime
from dotenv import load_dotenv
from prefect import flow, task

# Configuración del logger para incluir fecha, hora, nivel y mensaje
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%H:%M:%S"
)

# Cargar las variables de entorno desde el archivo .env
load_dotenv()

@task(name="Extraccion Documentos Bsale")
def run_carga_diaria():
    logging.info("Iniciando extraccion de documentos desde Bsale...")
    run_script("Extraccion Documentos Bsale", "../bsale/components/documentos/carga_diaria.py")
    logging.info("Extraccion de documentos completada.")

@task(name="Extraccion Stock Bsale")
def run_stock_masivo_actual():
    logging.info("Iniciando extraccion de stock desde Bsale...")
    run_script("Extraccion Stock Bsale", "../bsale/components/stock/stock_masivo_actual.py")
    logging.info("Extraccion de stock completada.")

@task(name="Extraccion Meta")
def run_carga_diaria_meta():
    logging.info("Iniciando extraccion de datos desde Meta...")
    run_script("Extraccion Meta", "../meta/carga_diaria_meta.py")
    logging.info("Extraccion de datos de Meta completada.")

@task(name="DBT Run")
def run_dbt():
    logging.info("Iniciando transformacion de datos con DBT...")
    run_dbt_run()
    logging.info("DBT Run finalizado con exito.")

def run_script(task_name, script_path):
    """Ejecuta un script Python desde el path indicado."""
    logging.info(f"Ejecutando {task_name} desde: {script_path}")

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

    logging.info(f"{task_name} ejecutado correctamente.")

def run_dbt_run():
    """Ejecuta 'dbt run' asegurando el directorio correcto y captura de estadisticas."""
    logging.info("Iniciando DBT Run...")
    start_time = datetime.now()

    # Determinar el directorio del proyecto dbt
    script_dir = os.path.dirname(os.path.abspath(__file__))
    dbt_project_dir = os.path.abspath(os.path.join(script_dir, "..", "dbt_project"))
    logging.info(f"Ejecutando DBT en el directorio: {dbt_project_dir}")

    # Agregar variables de entorno
    env = os.environ.copy()
    env["DBT_PROFILES_DIR"] = dbt_project_dir  # Perfil dbt

    result = subprocess.run(
        ["dbt", "run"],
        cwd=dbt_project_dir,
        env=env,
        capture_output=True,
        text=True,
        encoding="utf-8"
    )

    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()

    # Procesar el output para extraer estadisticas
    pass_count = warn_count = error_count = total_count = 0
    for line in result.stdout.splitlines():
        logging.info(f"DBT STDOUT: {line}")

        if "Completed successfully" in line or "Done. PASS=" in line:
            total_count = int(line.split("TOTAL=")[-1]) if "TOTAL=" in line else 0
            pass_count = int(line.split("PASS=")[-1].split()[0]) if "PASS=" in line else 0
            warn_count = int(line.split("WARN=")[-1].split()[0]) if "WARN=" in line else 0
            error_count = int(line.split("ERROR=")[-1].split()[0]) if "ERROR=" in line else 0

    if result.stderr:
        for line in result.stderr.splitlines():
            logging.error(f"DBT STDERR: {line}")

    # Si falla, guarda STDERR en un archivo
    if result.returncode != 0:
        error_log_path = os.path.join(dbt_project_dir, "dbt_error_log.txt")
        with open(error_log_path, "w", encoding="utf-8") as f:
            f.write(result.stderr)
        logging.error(f"Error en DBT Run. Ver detalles en {error_log_path}")
        raise RuntimeError(f"Error en DBT Run. Revisa el archivo {error_log_path}")

    logging.info(f"DBT Run completado en {duration:.2f} segundos")
    logging.info(f"Modelos ejecutados: {total_count} | PASS: {pass_count} | WARN: {warn_count} | ERROR: {error_count}")

#
# FLUJO COMPLETO
#
@flow(name="Daily ETL Flow")
def daily_flow():
    """Flujo ETL completo con extraccion, transformacion y carga."""
    logging.info("Iniciando el flujo de ETL diario...")
    run_carga_diaria()
    run_stock_masivo_actual()
    run_carga_diaria_meta()
    run_dbt()
    logging.info("Flujo ETL diario completado con exito.")

#
# FLUJO SOLO DBT
#
@flow(name="DBT Only Flow")
def dbt_only_flow():
    """Flujo que solo corre DBT."""
    logging.info("Iniciando flujo para ejecutar solo dbt run...")
    run_dbt()

if __name__ == "__main__":
    # Por defecto, ejecuta el flujo completo
    #daily_flow()

    # Para probar solo dbt, descomenta la siguiente línea:
    dbt_only_flow()
