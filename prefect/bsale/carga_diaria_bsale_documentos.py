from prefect import flow, task
import subprocess
import os
from datetime import datetime
import sys

@task
def run_carga_diaria(script_path: str):
    try:
        # Validar si el archivo del script existe
        if not os.path.exists(script_path):
            raise FileNotFoundError(f"El script no existe: {script_path}")
        
        # Obtener el intérprete de Python del entorno virtual
        python_executable = sys.executable

        # Registrar el tiempo de inicio
        start_time = datetime.now()

        # Ejecutar el script Python usando el intérprete del entorno virtual
        print(f"Iniciando la ejecución del script: {script_path}")
        result = subprocess.run(
            [python_executable, script_path],
            capture_output=True,
            text=True
        )

        # Registrar el tiempo de finalización
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        # Mostrar resultados
        if result.returncode == 0:
            print("\n=== Script ejecutado con éxito ===\n")
            print(result.stdout)  # Logs de salida
            print(f"Duración total: {duration:.2f} segundos")
        else:
            print("\n=== Error durante la ejecución del script ===\n")
            print(result.stderr)  # Logs de error
            print(f"Duración total: {duration:.2f} segundos")
            raise Exception(f"Error al ejecutar el script: {result.stderr}")

    except FileNotFoundError as fnf_error:
        print(fnf_error)
        raise
    except Exception as e:
        print(f"Error: {e}")
        raise

@flow
def carga_diaria_flow():
    SCRIPT_PATH = r"C:\Users\Felip\Desktop\PULLPO\Repositorio Demos\ELT&V\moss_etl_project\bsale\components\documentos\carga_diaria.py"
    run_carga_diaria(SCRIPT_PATH)

if __name__ == "__main__":
    carga_diaria_flow()
