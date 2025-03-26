import pandas as pd 
import requests
from datetime import datetime

url = "https://api.frankfurter.app/latest"
params = {
    "from": "USD",
    "to": "IDR"
}

response = requests.get(url, params=params)

if response.status_code == 200:
    data = response.json()
    exchange_rate = data['rates']['IDR']
    print(f"Exchange rate from USD to IDR: {exchange_rate}")
else:
    print(f"Failed to fetch data. Status code: {response.status_code}")

df = pd.read_csv('/opt/airflow/data/extract_result_crypto_pipeline.csv')
current_datetime = datetime.now()
df['datetime'] = [current_datetime for x in range(df.shape[0])]
df['coin_price_idr'] = df['coin_price']*exchange_rate
df.to_csv('/opt/airflow/data/transform_result_crypto_pipeline.csv', index=False)