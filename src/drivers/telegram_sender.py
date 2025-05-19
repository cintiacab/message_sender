import requests
from src.configuration.env_config import chat_info

def send_telegram_message(message: str):
    token = chat_info["TOKEN"]
    chat_id = chat_info["CHAT_ID"]
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload ={
        "chat_id": chat_id,
        "text": message
    }
    requests.post(url, data=payload, timeout=None)
