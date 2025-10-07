import re
import requests

def import_query(path):
    with open(path, "r") as open_file:
        return open_file.read()
    
def trata_query(query):

    # Substitui qualquer coisa depois de FROM por "df" e adicona ", query_modificada"
    query_modificada = re.sub(r"FROM\s+[\w\.\"]+", "FROM df", query, flags=re.IGNORECASE)
    query_modificada = re.sub(r"(SELECT\s+.*?)(FROM)", r"\1,_change_type \2", query_modificada, flags=re.IGNORECASE|re.DOTALL)
    return query_modificada

def send_telegram_message(bot_token, chat_id, message):
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {"chat_id": chat_id, "text": message}
    response = requests.post(url, data=payload)
    if response.status_code == 200:
        print("Mensagem enviada com sucesso ✅")
    else:
        print(f"Erro: {response.text}")