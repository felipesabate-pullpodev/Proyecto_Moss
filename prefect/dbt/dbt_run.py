from prefect import flow, task
import subprocess
import os
from datetime import datetime

@task
def run_dbt_model(model: str, dbt_project_path: str):
    try:
        # Validar si el directorio del proyecto dbt existe
        if not os.path.exists(dbt_project_path):
            raise FileNotFoundError(f"La ruta del proyecto dbt no existe: {dbt_project_path}")
        
        # Cambiar al directorio del proyecto dbt
        os.chdir(dbt_project_path)

        # Registrar el tiempo de inicio
        start_time = datetime.now()

        # Ejecutar el comando dbt run con las dependencias
        print(f"Iniciando dbt run para el modelo y dependencias: +{model}")
        result = subprocess.run(
            ["dbt", "run", "-s", f"+{model}"],
            capture_output=True,
            text=True
        )

        # Registrar el tiempo de finalización
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        # Mostrar resultados
        if result.returncode == 0:
            print("\n=== dbt run finalizado con éxito ===\n")
            print(result.stdout)  # Logs de salida
            print(f"Duración total: {duration:.2f} segundos")
        else:
            print("\n=== Error durante dbt run ===\n")
            print(result.stderr)  # Logs de error
            print(f"Duración total: {duration:.2f} segundos")
            raise Exception(f"Error al ejecutar dbt run: {result.stderr}")

    except FileNotFoundError as fnf_error:
        print(fnf_error)
        raise
    except Exception as e:
        print(f"Error: {e}")
        raise

@flow
def dbt_flow():
    MODEL_NAME = "core_document_explode"  # Cambia esto según tu modelo
    DBT_PROJECT_PATH = r"C:\Users\Felip\Desktop\PULLPO\Repositorio Demos\ELT&V\moss_etl_project\dbt_project"
    run_dbt_model(MODEL_NAME, DBT_PROJECT_PATH)

if __name__ == "__main__":
    dbt_flow()
