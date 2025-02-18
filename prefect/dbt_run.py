import os
import subprocess
import logging
from prefect import flow, task

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

@task(name="DBT Test Run")
def run_dbt():
    """Ejecuta 'dbt run' en el directorio del proyecto DBT y muestra la salida en tiempo real."""
    logging.info("Iniciando DBT Run de prueba...")

    dbt_project_dir = os.path.abspath("../dbt_project")
    logging.info(f"Ejecutando 'dbt run' en el directorio: {dbt_project_dir}")

    env = os.environ.copy()
    env["DBT_PROFILES_DIR"] = dbt_project_dir  # Asegurar que usa el perfil correcto

    # 🔥 **Agregar credenciales si no están definidas**
    if "GOOGLE_APPLICATION_CREDENTIALS" not in env:
        env["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\Users\Felip\Desktop\PULLPO\Repositorio Demos\ELT&V\Proyecto_Moss\env\moss-dbt.json"

    logging.info(f"DBT_PROFILES_DIR = {env['DBT_PROFILES_DIR']}")
    logging.info(f"GOOGLE_APPLICATION_CREDENTIALS = {env['GOOGLE_APPLICATION_CREDENTIALS']}")

    # 🔥 **Ejecutar dbt run y capturar salida en tiempo real**
    process = subprocess.Popen(
        ["dbt", "run"],
        cwd=dbt_project_dir,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )

    # Leer salida en tiempo real
    for line in process.stdout:
        print(line.strip())
    for line in process.stderr:
        print(f"ERROR: {line.strip()}")

    process.wait()

    if process.returncode != 0:
        logging.error("DBT Run falló. Revisa la salida anterior para más detalles.")
        raise RuntimeError("Error en DBT Run.")

    logging.info("DBT Run ejecutado correctamente.")

@flow(name="DBT Test Flow")
def test_dbt_flow():
    """Flujo para probar DBT."""
    logging.info("Iniciando flujo de prueba DBT...")
    run_dbt()
    logging.info("Flujo de prueba DBT completado con éxito.")

if __name__ == "__main__":
    test_dbt_flow()
