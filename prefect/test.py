import os
import subprocess
import logging

# Configurar logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def test_dbt_run():
    """ Prueba ejecutar `dbt run` en el directorio correcto. """
    dbt_project_dir = os.path.abspath("../dbt_project")  # Ruta al proyecto dbt

    logging.info(f"Ejecutando DBT Run en {dbt_project_dir}...")

    result = subprocess.run(
        ["dbt", "run"],
        cwd=dbt_project_dir,  # 👈 Ahora se ejecuta en el directorio correcto
        capture_output=True,
        text=True,
        encoding="utf-8"
    )

    if result.stdout:
        logging.info(f"DBT STDOUT:\n{result.stdout}")
    if result.stderr:
        logging.error(f"DBT STDERR:\n{result.stderr}")

    if result.returncode != 0:
        logging.error("❌ DBT Run falló.")
        exit(1)  # Finaliza con error

    logging.info("✅ DBT Run ejecutado correctamente.")

if __name__ == "__main__":
    test_dbt_run()
