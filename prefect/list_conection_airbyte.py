import requests
import os
from dotenv import load_dotenv

# Cargar las variables de entorno desde el archivo .env
load_dotenv()

def list_connections(airbyte_url: str, jwt_token: str, workspace_id: str):
    try:
        # Verificar que las variables necesarias no sean None
        if not airbyte_url or not jwt_token or not workspace_id:
            raise ValueError("Faltan variables de entorno necesarias (AIRBYTE_URL, JWT_TOKEN, WORKSPACE_ID).")

        # Endpoint para listar conexiones
        url = f"{airbyte_url}/api/v1/connections/list"
        headers = {"Authorization": f"Bearer {jwt_token}"}
        payload = {"workspaceId": workspace_id}
        
        response = requests.post(url, json=payload, headers=headers)
        response.raise_for_status()
        
        # Imprimir las conexiones disponibles
        connections = response.json()
        print("Conexiones disponibles en el workspace:")
        for connection in connections.get("connections", []):
            print(f"- {connection['name']} (ID: {connection['connectionId']})")
    except requests.exceptions.RequestException as e:
        print(f"Error al listar conexiones: {e}")
    except ValueError as ve:
        print(f"Error de configuración: {ve}")

if __name__ == "__main__":
    # Leer las variables de entorno
    AIRBYTE_URL = os.getenv('AIRBYTE_URL')
    JWT_TOKEN = os.getenv('AIRBYTE_JWT_TOKEN')
    WORKSPACE_ID = os.getenv('WORKSPACE_ID')

    # Verificar si las variables se cargaron correctamente
    print(f"AIRBYTE_URL: {AIRBYTE_URL}")
    print(f"JWT_TOKEN: {'****' if JWT_TOKEN else None}")  # Ocultar el token por seguridad
    print(f"WORKSPACE_ID: {WORKSPACE_ID}")

    # Llamar a la función para listar conexiones
    list_connections(AIRBYTE_URL, JWT_TOKEN, WORKSPACE_ID)
