import os
import requests
import json
import logging
from dotenv import load_dotenv

# -----------------------------------------------------------------------------
# 1) Configuración General
# -----------------------------------------------------------------------------
load_dotenv()  # Carga variables de entorno desde .env

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Variables de entorno
AD_ACCOUNT_ID = os.getenv("FB_AD_ACCOUNT_ID")
ACCESS_TOKEN = os.getenv("FB_ACCESS_TOKEN")
# Construimos la URL de la API
url = (
    f"https://graph.facebook.com/v16.0/act_{AD_ACCOUNT_ID}/insights"
    f"?fields=impressions,clicks,spend"
    f"&access_token={ACCESS_TOKEN}"
)

# Hacemos la solicitud GET
response = requests.get(url)

# Convertimos la respuesta a JSON
data = response.json()

# Imprimimos el resultado de forma legible
print(json.dumps(data, indent=2))