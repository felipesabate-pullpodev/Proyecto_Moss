import os
import sys
import subprocess
import logging
from datetime import datetime
from dotenv import load_dotenv
from prefect import flow, task
import shutil  # Import necesario para verificar rutas de ejecutables

# Configuración global del logger para que imprima en la CLI
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%H:%M:%S"
)

# Cargar variables de entorno desde el archivo .env
load_dotenv()

def get_daily_log_file():
    """Devuelve el nombre del archivo de log para el día actual."""
    today = datetime.now().strftime("%Y%m%d")
    return f"/home/mosspullpo/logs/daily_{today}.log"

def log_flow_run_header():
    """Escribe un encabezado en el log diario para cada ejecución del flujo."""
    log_file = get_daily_log_file()
    header = f"\n=== Daily Flow Run - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ===\n"
    with open(log_file, "a") as f:
        f.write(header)
    logging.info(header.strip())

def run_script(task_name, script_path):
    """
    Ejecuta un script Python desde el path indicado, mostrando la salida en tiempo real 
    y escribiéndola en un único archivo de log diario.
    Las líneas de stderr que contienen "Procesando" o "Se obtuvieron" se registran como INFO.
    """
    log_file = get_daily_log_file()
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
    meta_script_path = "/home/mosspullpo/Proyecto_Moss/meta/carga_diaria_meta.py"
    run_script("Extracción Meta", meta_script_path)
    logging.info("Extracción de datos de Meta completada.")



@task(name="DBT Run")
def run_dbt():
    logging.info("Iniciando transformación de datos con DBT...")
    run_dbt_run()
    logging.info("DBT Run finalizado con éxito.")


def run_dbt_run():
    """Ejecuta 'dbt run' asegurando el entorno correcto y capturando la salida en tiempo real."""
    log_file = "/home/mosspullpo/logs/daily_{}.log".format(datetime.now().strftime("%Y%m%d"))
    logging.info("Iniciando DBT Run...")

    # 🔥 **Verificar qué Python y dbt está usando Prefect**
    logging.info(f"Python ejecutándose desde: {sys.executable}")
    logging.info(f"DBT encontrado en: {shutil.which('dbt')}")
    logging.info(f"Entorno virtual activo: {os.environ.get('VIRTUAL_ENV', 'Ninguno')}")

    # Directorio donde está el proyecto dbt
    dbt_project_dir = "/home/mosspullpo/Proyecto_Moss/dbt_project"

    # Ruta absoluta del ejecutable de dbt dentro del entorno virtual
    dbt_executable = "/home/mosspullpo/venv/bin/dbt"

    # Verificar si dbt existe en la ruta especificada
    if not os.path.exists(dbt_executable):
        logging.error(f"No se encontró dbt en {dbt_executable}.")
        raise RuntimeError(f"No se encontró dbt en {dbt_executable}. Revisa la instalación.")

    logging.info(f"Ejecutando DBT desde: {dbt_executable}")
    logging.info(f"Directorio del proyecto DBT: {dbt_project_dir}")

    # Definir variables de entorno
    env = os.environ.copy()
    env["DBT_PROFILES_DIR"] = dbt_project_dir  # Asegurar que usa el perfil correcto

    # Asegurar que GOOGLE_APPLICATION_CREDENTIALS está definido
    if "GOOGLE_APPLICATION_CREDENTIALS" not in env:
        env["GOOGLE_APPLICATION_CREDENTIALS"] = "/home/mosspullpo/Proyecto_Moss/env/moss-dbt.json"

    logging.info(f"DBT_PROFILES_DIR = {env['DBT_PROFILES_DIR']}")
    logging.info(f"GOOGLE_APPLICATION_CREDENTIALS = {env['GOOGLE_APPLICATION_CREDENTIALS']}")

    # Ejecutar DBT y capturar salida en tiempo real
    process = subprocess.Popen(
        [dbt_executable, "run"],  # 🔹 **Usar la ruta completa de dbt**
        cwd=dbt_project_dir,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )

    # Capturar y mostrar salida en tiempo real
    for line in process.stdout:
        print(line.strip())
    for line in process.stderr:
        print(f"ERROR: {line.strip()}")

    process.wait()

    if process.returncode != 0:
        logging.error("DBT Run falló. Revisa la salida anterior para más detalles.")
        raise RuntimeError("Error en DBT Run.")

    logging.info("DBT Run ejecutado correctamente.")


@flow(name="Daily ETL Flow")
def daily_flow():
    # Encabezado en el log
    log_flow_run_header()
    logging.info("Iniciando el flujo de ETL diario...")

    # 1) Bsale - Documentos
    run_carga_diaria()

    # 2) Bsale - Stock
    run_stock_masivo_actual()

    # 3) Meta
    run_carga_diaria_meta()  # <--- Descomenta esta línea para incluir la extracción de Meta

    # 4) DBT
    run_dbt()

    logging.info("Flujo ETL diario completado con éxito.")


if __name__ == "__main__":
    daily_flow()
