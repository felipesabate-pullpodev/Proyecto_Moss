import os
import sys
import subprocess
import logging
from datetime import datetime
from dotenv import load_dotenv
from prefect import flow, task

# Configuración global del logger para que imprima en la CLI
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%H:%M:%S"
)

# Cargar variables de entorno desde el archivo .env
load_dotenv()

def get_daily_log_file(safe_task_name):
    """Devuelve el nombre del archivo de log para el día actual."""
    today = datetime.now().strftime("%Y%m%d")
    return f"/home/mosspullpo/logs/{safe_task_name}_{today}.log"

def run_script(task_name, script_path):
    """
    Ejecuta un script Python desde el path indicado, mostrando la salida en tiempo real 
    y escribiéndola en un archivo de log específico para el día.
    Las líneas de stderr que contienen "Procesando" o "Se obtuvieron" se registran como INFO.
    """
    # Crear un nombre seguro para el archivo de log (sin espacios ni acentos)
    safe_task_name = task_name.encode("ascii", "ignore").decode().replace(" ", "_").lower()
    log_file = get_daily_log_file(safe_task_name)
    logging.info(f"Ejecutando {task_name} desde: {script_path}. Logs en: {log_file}")

    process = subprocess.Popen(
        [sys.executable, script_path],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding="utf-8"
    )

    # Leer y mostrar la salida de stdout en tiempo real
    for line in iter(process.stdout.readline, ""):
        line = line.rstrip()
        if line:
            logging.info(f"{task_name} STDOUT: {line}")
            with open(log_file, "a") as f:
                f.write(f"{task_name} STDOUT: {line}\n")
    # Leer y mostrar la salida de stderr en tiempo real, con filtrado
    for line in iter(process.stderr.readline, ""):
        line = line.rstrip()
        if line:
            if "Procesando" in line or "Se obtuvieron" in line:
                logging.info(f"{task_name} STDERR: {line}")
            else:
                logging.error(f"{task_name} STDERR: {line}")
            with open(log_file, "a") as f:
                f.write(f"{task_name} STDERR: {line}\n")
    process.stdout.close()
    process.stderr.close()
    return_code = process.wait()
    if return_code != 0:
        raise RuntimeError(f"Error en {task_name}.")
    logging.info(f"{task_name} ejecutado correctamente.")

@task(name="Extracción Documentos Bsale")
def run_carga_diaria():
    logging.info("Iniciando extracción de documentos desde Bsale...")
    # Usar ruta absoluta para evitar problemas con rutas relativas
    run_script("Extracción Documentos Bsale", "/home/mosspullpo/Proyecto_Moss/bsale/components/documentos/carga_diaria.py")
    logging.info("Extracción de documentos completada.")

@task(name="Extracción Stock Bsale")
def run_stock_masivo_actual():
    logging.info("Iniciando extracción de stock desde Bsale...")
    run_script("Extracción Stock Bsale", "/home/mosspullpo/Proyecto_Moss/bsale/components/stock/stock_masivo_actual.py")
    logging.info("Extracción de stock completada.")

@task(name="Extracción Meta")
def run_carga_diaria_meta():
    logging.info("Iniciando extracción de datos desde Meta...")
    run_script("Extracción Meta", "/home/mosspullpo/Proyecto_Moss/meta/carga_diaria_meta.py")
    logging.info("Extracción de datos de Meta completada.")

@task(name="DBT Run")
def run_dbt():
    logging.info("Iniciando transformación de datos con DBT...")
    run_dbt_run()
    logging.info("DBT Run finalizado con éxito.")

def run_dbt_run():
    """
    Ejecuta 'dbt run' en el directorio del proyecto DBT, mostrando la salida en tiempo real 
    y guardándola en un archivo de log.
    """
    log_file = "/home/mosspullpo/logs/dbt_run.log"
    logging.info("Iniciando DBT Run...")
    start_time = datetime.now()

    # Determinar el directorio del proyecto DBT
    script_dir = os.path.dirname(os.path.abspath(__file__))
    dbt_project_dir = os.path.abspath(os.path.join(script_dir, "..", "dbt_project"))
    logging.info(f"Ejecutando DBT en el directorio: {dbt_project_dir}")

    # Configurar el entorno para DBT
    env = os.environ.copy()
    env["DBT_PROFILES_DIR"] = dbt_project_dir  # Asegurar que DBT use el perfil correcto

    process = subprocess.Popen(
        ["dbt", "run"],
        cwd=dbt_project_dir,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding="utf-8"
    )

    for line in iter(process.stdout.readline, ""):
        line = line.rstrip()
        if line:
            logging.info(f"DBT STDOUT: {line}")
            with open(log_file, "a") as f:
                f.write(f"DBT STDOUT: {line}\n")
    for line in iter(process.stderr.readline, ""):
        line = line.rstrip()
        if line:
            logging.error(f"DBT STDERR: {line}")
            with open(log_file, "a") as f:
                f.write(f"DBT STDERR: {line}\n")
    process.stdout.close()
    process.stderr.close()
    return_code = process.wait()

    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()

    if return_code != 0:
        error_log_path = os.path.join(dbt_project_dir, "dbt_error_log.txt")
        with open(error_log_path, "w", encoding="utf-8") as f:
            f.write("Error en DBT run\n")
        logging.error(f"Error en DBT Run. Ver detalles en {error_log_path}")
        raise RuntimeError(f"Error en DBT Run. Revisa el archivo {error_log_path}")

    logging.info(f"DBT Run completado en {duration:.2f} segundos")

@flow(name="Daily ETL Flow")
def daily_flow():
    """Flujo ETL completo con extracción, transformación y carga."""
    logging.info("Iniciando el flujo de ETL diario...")
    run_carga_diaria()
    run_stock_masivo_actual()
    run_carga_diaria_meta()
    run_dbt()
    logging.info("Flujo ETL diario completado con éxito.")

if __name__ == "__main__":
    daily_flow()
