import pandas as pd
import psycopg2
from sqlalchemy import create_engine

DB_CONFIG = {
    'host': 'ep-snowy-art-a17wausj-pooler.ap-southeast-1.aws.neon.tech',
    'database': 'group001',
    'user': 'neondb_owner',
    'password': 's9URtWjSK6IT',
    'port': '5432'
}
engine = create_engine(f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}", connect_args={'sslmode': "require"})

df = pd.read_csv('/opt/airflow/data/transform_result_crypto_pipeline.csv')
df.to_sql(name='crypto_price', con=engine, if_exists="append", index=False)