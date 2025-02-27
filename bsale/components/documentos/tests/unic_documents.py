import requests
import json
from dotenv import load_dotenv
import os

# Cargar las variables de entorno desde un archivo .env
load_dotenv()

# Configuración de la API
API_TOKEN = os.getenv("BSALE_ACCESS_TOKEN")

def fetch_document_by_number(document_number):
    """
    Obtiene un documento específico filtrando por el número de documento.
    """
    headers = {
        'Content-Type': 'application/json',
        'access_token': API_TOKEN
    }

    # Endpoint con el filtro por número
    url = f"https://api.bsale.cl/v1/documents.json"
    params = {
        'number': document_number,  # Filtrar por número de documento
        'expand': 'document_type,client,office,user,details,references,document_taxes,sellers,payments'
    }

    try:
        print(f"Buscando documento con número {document_number}...")
        response = requests.get(url, headers=headers, params=params, timeout=10)
        response.raise_for_status()  # Verifica si hay errores HTTP
        data = response.json()

        # Si hay resultados, muestra el primer documento encontrado
        if data.get("items"):
            document = data["items"][0]
            print(json.dumps(document, indent=4, ensure_ascii=False))
            return document
        else:
            print(f"No se encontró ningún documento con el número {document_number}.")
            return None

    except requests.exceptions.HTTPError as e:
        print(f"Error HTTP al consultar la API: {e}")
    except requests.exceptions.RequestException as e:
        print(f"Error al consultar la API: {e}")

    return None


# Ejemplo de uso
if __name__ == "__main__":
    # Número de documento a consultar
    document_number = input("Ingresa el número del documento a consultar: ").strip()

    if document_number.isdigit():
        fetch_document_by_number(document_number)
    else:
        print("Por favor, ingresa un número de documento válido.")