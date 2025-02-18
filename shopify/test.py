import requests
import os
import json
from dotenv import load_dotenv

# Cargar credenciales
load_dotenv()

SHOPIFY_STORE = os.getenv("SHOPIFY_STORE")
API_TOKEN = os.getenv("SHOPIFY_API_TOKEN")

# Endpoint para obtener órdenes (solo 1 para inspección)
url = f"https://{SHOPIFY_STORE}.myshopify.com/admin/api/2024-01/orders.json?limit=1"

# Headers de autenticación
headers = {
    "X-Shopify-Access-Token": API_TOKEN,
    "Content-Type": "application/json"
}

# Hacer la solicitud GET
response = requests.get(url, headers=headers)

if response.status_code == 200:
    orders = response.json().get("orders", [])
    
    if orders:
        first_order = orders[0]  # Tomamos la primera orden
        print("🔍 Campos disponibles en la orden:")
        for key in first_order.keys():
            print("-", key)
    else:
        print("⚠️ No hay órdenes disponibles.")

else:
    print("Error:", response.status_code, response.text)
