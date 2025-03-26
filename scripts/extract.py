import requests
from datetime import datetime
import pandas as pd

url = "https://api.coingecko.com/api/v3/simple/price"

params = {
      "ids": "bitcoin,ethereum,ripple,solana,doge",  # Cryptocurrencies to fetch
      "vs_currencies": "usd"     # Currency to convert to
  }


response = requests.get(url, params=params)
harga_crypto = []
nama_crypto = []
for coin in response.json().keys():
  harga_crypto.append(response.json()[coin]['usd'])
  nama_crypto.append(coin)
df = pd.DataFrame({'coin_name':nama_crypto, 'coin_price':harga_crypto})
df.to_csv('/opt/airflow/data/extract_result_crypto_pipeline.csv', index=False)