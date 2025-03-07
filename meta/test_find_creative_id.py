import os
import requests
import json
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()
ACCESS_TOKEN = os.getenv("FB_ACCESS_TOKEN")
ad_id = "120212383452380657"  # Reemplaza con el ID del anuncio

url = (
    f"https://graph.facebook.com/v16.0/{ad_id}"
    f"?fields=creative"
    f"&access_token={ACCESS_TOKEN}"
)

response = requests.get(url)
ad_data = response.json()
print(json.dumps(ad_data, indent=2))

# Extraer el ad_creative_id
ad_creative_id = ad_data.get("creative", {}).get("id")
print(f"Ad Creative ID: {ad_creative_id}")
