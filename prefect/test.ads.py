import requests
import time
from datetime import datetime
import os
from dotenv import load_dotenv

# Cargar las variables de entorno desde el archivo .env
load_dotenv()

def start_sync(airbyte_url: str, jwt_token: str, connection_id: str):
    try:
        url = f"{airbyte_url}/api/v1/connections/sync"
        headers = {"Authorization": f"Bearer {jwt_token}"}
        payload = {"connectionId": connection_id}
        
        response = requests.post(url, json=payload, headers=headers)
        response.raise_for_status()
        
        sync_data = response.json()
        job_id = sync_data.get("job", {}).get("id")
        if job_id:
            print(f"Sincronización iniciada. Job ID: {job_id}")
            return job_id
        else:
            print("Error: No se obtuvo ningún Job ID.")
            return None
    except requests.exceptions.RequestException as e:
        print(f"Error al iniciar sincronización: {e}")
        return None

def monitor_sync(airbyte_url: str, jwt_token: str, job_id: str):
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
            print(f"Estado del Job {job_id}: {job_status}")
            
            if job_status in ["succeeded", "failed", "cancelled"]:
                end_time = datetime.now()
                duration = (end_time - start_time).total_seconds()
                
                # Obtener estadísticas del Job
                attempts = job_data.get("job", {}).get("attempts", [])
                for attempt in attempts:
                    attempt_log = attempt.get("logs", {}).get("logLines", [])
                    for line in attempt_log:
                        print(f"Log: {line}")
                
                print(f"Duración total: {duration} segundos")
                if job_status == "succeeded":
                    stats = job_data.get("job", {}).get("attempts", [{}])[-1].get("attemptStats", {})
                    bytes_synced = stats.get("syncStats", {}).get("bytesSynced", 0)
                    records_synced = stats.get("syncStats", {}).get("recordsSynced", 0)
                    print(f"Datos transferidos: {bytes_synced / (1024 ** 2):.2f} MB")
                    print(f"Registros sincronizados: {records_synced}")
                break
            
            time.sleep(10)
    except requests.exceptions.RequestException as e:
        print(f"Error al monitorear la sincronización: {e}")

if __name__ == "__main__":
    # Leer las variables de entorno
    AIRBYTE_URL = os.getenv('AIRBYTE_URL')
    JWT_TOKEN = os.getenv('AIRBYTE_JWT_TOKEN')
    CONNECTION_ID = os.getenv('AIRBYTE_CONNECTION_ID')

    # Verificar que las variables estén correctamente cargadas
    if not AIRBYTE_URL or not JWT_TOKEN or not CONNECTION_ID:
        print("Error: Faltan variables de entorno necesarias. Revisa tu archivo .env.")
        exit(1)

    # Iniciar sincronización
    job_id = start_sync(AIRBYTE_URL, JWT_TOKEN, CONNECTION_ID)
    if job_id:
        # Monitorear sincronización
        monitor_sync(AIRBYTE_URL, JWT_TOKEN, job_id)
