import requests
import os

API_TOKEN = os.getenv("BSALE_ACCESS_TOKEN")

headers = {
    'Content-Type': 'application/json',
    'access_token': API_TOKEN
}

document_id = 91657  # Usa un documento donde viste payments
url = f"https://api.bsale.cl/v1/documents/{document_id}.json?expand=[payments]"

response = requests.get(url, headers=headers)
print(response.json())
