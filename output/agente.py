import requests
from databricks.sdk import WorkspaceClient

BOT_TOKEN = dbutils.secrets.get(scope="meu-escopo", key="bot_token")
CHAT_ID = dbutils.secrets.get(scope="meu-escopo", key="chat_id")
API_URL = f"https://api.telegram.org/bot{BOT_TOKEN}"

# Cliente Databricks
w = WorkspaceClient()
client = w.serving_endpoints.get_open_ai_client()
ENDPOINT = "meu-agente-sql"

def get_updates(offset=None):
    url = f"{API_URL}/getUpdates"
    params = {"timeout": 30, "offset": offset}
    return requests.get(url, params=params).json()

def send_message(chat_id, text):
    url = f"{API_URL}/sendMessage"
    payload = {"chat_id": chat_id, "text": text}
    requests.post(url, data=payload)

def ask_agent(question):
    input_msgs = [{"role": "user", "content": question}]
    response = client.responses.create(model=ENDPOINT, input=input_msgs)
    return response.output_text

def main():
    offset = None
    while True:
        updates = get_updates(offset)
        for update in updates.get("result", []):
            offset = update["update_id"] + 1
            message = update["message"]["text"]
            chat_id = update["message"]["chat"]["id"]

            # Pergunta ao agente
            resposta = ask_agent(message)

            # Responde no Telegram
            send_message(chat_id, resposta)

if __name__ == "__main__":
    main()