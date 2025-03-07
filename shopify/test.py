import os
import requests
import json
from dotenv import load_dotenv

# Cargar credenciales
load_dotenv()
SHOPIFY_STORE = os.getenv("SHOPIFY_STORE")
SHOPIFY_API_TOKEN = os.getenv("SHOPIFY_API_TOKEN")

# URL GraphQL
GRAPHQL_URL = f"https://{SHOPIFY_STORE}.myshopify.com/admin/api/2024-01/graphql.json"

headers = {
    "X-Shopify-Access-Token": SHOPIFY_API_TOKEN,
    "Content-Type": "application/json"
}

# Nueva consulta para sesiones
query = """
{
  shopifyqlQuery(query: "FROM sessions SINCE -30d UNTIL today SHOW total_visits, total_sessions, new_visitors, returning_visitors") {
    data
    columnLabels
  }
}
"""

# Realizar la consulta
response = requests.post(GRAPHQL_URL, headers=headers, json={"query": query})

if response.status_code == 200:
    result = response.json()
    if "data" in result and result["data"].get("shopifyqlQuery"):
        print(json.dumps(result["data"]["shopifyqlQuery"], indent=4))
    else:
        print("No se encontraron datos en la respuesta.")
else:
    print(f"Error: {response.status_code} - {response.text}")
