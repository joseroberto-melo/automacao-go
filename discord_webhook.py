import requests
from config import secrets

def send_alert(message):
    payload = {
        "content": message
    }
    try:
        requests.post(secrets.DISCORD_WEBHOOK, json=payload)
    except Exception as e:
        print(f"Erro ao enviar mensagem para o Discord: {e}")
