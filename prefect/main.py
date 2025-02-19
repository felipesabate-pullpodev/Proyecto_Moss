import os
import sys
import subprocess
import logging
from datetime import datetime
from dotenv import load_dotenv
from prefect import flow, task

# Configuración global del logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%H:%M:%S"
)

# Cargar variables de entorno desde el archivo .env
load_dotenv()

def run_script(task_name, script_path):
    """
    Ejecuta un script Python desde el path indicado y guarda la salida en un log específico.
    """
    # Define un archivo de log único para cada tarea (reemplazamos espacios y lo pasamos a minúsculas)
    log_file = f"/home/mosspullpo/logs/{task_name.replace(' ', '_').lower()}_run.log"
    logging.info(f"Ejecutando {task_name} desde: {script_path}. Se guardarán logs en: {log_file}")

    result = subprocess.run(
        [sys.executable, script_path],
        capture_output=True,
        text=True,
        encoding="utf-8"
    )

    with open(log_file, 'a') as f:
        if result.stdout:
            f.write(f"{task_name} STDOUT:\n{result.stdout}\n")
        if result.stderr:
            f.write(f"{task_name} STDERR:\n{result.stderr}\n")

    if result.returncode != 0:
        raise RuntimeError(f"Error en {task_name}.")
    logging.info(f"{task_name} ejecutado correctamente.")

@task(name="Extracción Documentos Bsale")
def run_carga_diaria():
    logging.info("Iniciando extracción de documentos desde Bsale...")
    # Usamos la ruta absoluta para evitar problemas con rutas relativas
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
    Ejecuta 'dbt run' en el directorio del proyecto DBT y guarda la salida en un archivo de log.
    """
    log_file = "/home/mosspullpo/logs/dbt_run.log"
    logging.info("Iniciando DBT Run...")
    start_time = datetime.now()

    # Determinar el directorio del proyecto dbt
    script_dir = os.path.dirname(os.path.abspath(__file__))
    dbt_project_dir = os.path.abspath(os.path.join(script_dir, "..", "dbt_project"))
    logging.info(f"Ejecutando DBT en el directorio: {dbt_project_dir}")

    # Configurar el entorno para DBT
    env = os.environ.copy()
    # Aquí configuramos DBT_PROFILES_DIR para que apunte al directorio donde está el archivo profiles.yml
    env["DBT_PROFILES_DIR"] = dbt_project_dir  

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

    with open(log_file, 'a') as f:
        for line in result.stdout.splitlines():
            f.write(f"DBT STDOUT: {line}\n")
        if result.stderr:
            for line in result.stderr.splitlines():
                f.write(f"DBT STDERR: {line}\n")

    if result.returncode != 0:
        error_log_path = os.path.join(dbt_project_dir, "dbt_error_log.txt")
        with open(error_log_path, "w", encoding="utf-8") as f:
            f.write(result.stderr)
        logging.error(f"Error en DBT Run. Ver detalles en {error_log_path}")
        raise RuntimeError(f"Error en DBT Run. Revisa el archivo {error_log_path}")

    logging.info(f"DBT Run completado en {duration:.2f} segundos")

    # Extraer estadísticas (opcional)
    pass_count = warn_count = error_count = total_count = 0
    for line in result.stdout.splitlines():
        if "Completed successfully" in line or "Done. PASS=" in line:
            total_count = int(line.split("TOTAL=")[-1]) if "TOTAL=" in line else 0
            pass_count = int(line.split("PASS=")[-1].split()[0]) if "PASS=" in line else 0
            warn_count = int(line.split("WARN=")[-1].split()[0]) if "WARN=" in line else 0
            error_count = int(line.split("ERROR=")[-1].split()[0]) if "ERROR=" in line else 0

    logging.info(f"Modelos ejecutados: {total_count} | PASS: {pass_count} | WARN: {warn_count} | ERROR: {error_count}")

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


# # if __name__ == "__main__":
# #     daily_flow.deploy(
# #         name="daily_job",
# #         work_pool_name="mi_work_pool",
# #         cron="0 0 * * *",
# #     )

# # #  Si queremos probar 
# if __name__ == "__main__":
#      daily_flow()



