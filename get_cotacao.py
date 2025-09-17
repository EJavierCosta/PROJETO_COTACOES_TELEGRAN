import requests
import os
from dotenv import load_dotenv
import pandas as pd

def get_cotacao ():

    load_dotenv()
    token = os.getenv("token_brapi")

    url = "https://brapi.dev/api/quote/BBDC4"
    header = {"Authorization": f"Bearer {token}"}

    response = requests.get(
                        url,
                        headers=header,
                        params={"range": "1d", "interval": "1d"})
    dados_brapi = response.json()["results"][0]


    # Monta o DataFrame
    df = pd.DataFrame([{
        "Símbolo": dados_brapi["symbol"],
        "Nome Curto":   dados_brapi["shortName"],
        "Moeda": dados_brapi["currency"],
        "Fechamento Anterior": dados_brapi["regularMarketPreviousClose"],
        "Preço Atual": dados_brapi["regularMarketPrice"],
        "Intervalo 52 Semanas": dados_brapi["fiftyTwoWeekRange"],
        "Data da Cotação": dados_brapi["regularMarketTime"]
    }])

    return df, response

if __name__ == "__main__":
    df, response =get_cotacao()
    if response.status_code == 200:
        print(f"Coleta realizada com sucesso, status code: {response.status_code}") 
        print(df)
    else:
        print(f"Erro: {response.status_code}")