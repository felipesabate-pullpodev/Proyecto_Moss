import os
import requests
import json
import logging
import time
from dotenv import load_dotenv

# -------------------------------------------------------------------
# CONFIGURACIONES GENERALES
# -------------------------------------------------------------------
load_dotenv()
ACCESS_TOKEN = os.getenv("FB_ACCESS_TOKEN")
AD_CREATIVE_ID = "120212383532290657"  # ID del Creative a consultar

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# -------------------------------------------------------------------
# LISTA DE CAMPOS A SOLICITAR
# -------------------------------------------------------------------
FIELDS_GROUPS = [
    ["id", "account_id", "name", "status", "object_id", "object_type", "object_story_id", "effective_object_story_id"],
    ["body", "title", "image_url", "thumbnail_url", "video_id"],
    ["object_story_spec", "ad_disclaimer_spec", "authorization_category", "call_to_action_type"],
]

# -------------------------------------------------------------------
# FUNCIÓN PARA OBTENER DETALLES DEL AD CREATIVE
# -------------------------------------------------------------------
def fetch_ad_creative_details(ad_creative_id):
    """
    Obtiene información detallada de un Ad Creative.
    Si `object_story_id` existe, hace una segunda llamada para obtener el contenido del post.
    """
    creative_data = {"id": ad_creative_id}
    
    # Obtener información del Ad Creative
    for fields in FIELDS_GROUPS:
        fields_str = ",".join(fields)
        url = f"https://graph.facebook.com/v16.0/{ad_creative_id}?fields={fields_str}&access_token={ACCESS_TOKEN}"
        
        try:
            response = requests.get(url, timeout=60)
            data = response.json()
            
            if "error" in data:
                logger.error(f"Error obteniendo campos {fields_str}: {data['error']}")
                continue  # Pasar al siguiente grupo de campos en caso de error
            
            creative_data.update(data)
            time.sleep(0.5)  # Espera para evitar rate limits
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error de conexión: {e}")

    # Obtener contenido real del post si hay `object_story_id`
    object_story_id = creative_data.get("object_story_id") or creative_data.get("effective_object_story_id")
    if object_story_id:
        post_content = fetch_post_details(object_story_id)
        creative_data.update(post_content)

    return creative_data

# -------------------------------------------------------------------
# FUNCIÓN PARA OBTENER EL CONTENIDO DEL POST (MENSAJE / TEXTO DEL ANUNCIO)
# -------------------------------------------------------------------
def fetch_post_details(post_id):
    """
    Obtiene el contenido del post asociado a un `object_story_id` (texto del anuncio).
    """
    url = f"https://graph.facebook.com/v16.0/{post_id}?fields=message,link,created_time&access_token={ACCESS_TOKEN}"
    
    try:
        response = requests.get(url, timeout=60)
        data = response.json()
        
        if "error" in data:
            logger.error(f"Error obteniendo detalles del post {post_id}: {data['error']}")
            return {"body": None}  # Devolver vacío si hay error
        
        return {
            "body": data.get("message"),
            "post_link": data.get("link"),
            "post_created_time": data.get("created_time")
        }
    
    except requests.exceptions.RequestException as e:
        logger.error(f"Error de conexión al obtener el post: {e}")
        return {"body": None}

# -------------------------------------------------------------------
# EJECUCIÓN
# -------------------------------------------------------------------
if __name__ == "__main__":
    logger.info(f"Obteniendo detalles del Ad Creative: {AD_CREATIVE_ID}")
    creative_details = fetch_ad_creative_details(AD_CREATIVE_ID)
    
    if creative_details:
        print("\n🔍 **DETALLES COMPLETOS DEL AD CREATIVE:**")
        print(json.dumps(creative_details, indent=2, ensure_ascii=False))
    else:
        logger.error(f"No se pudo obtener información para el Creative ID {AD_CREATIVE_ID}")
