import yfinance as yf
import pandas as pd 
import sqlalchemy
from dotenv import load_dotenv
import os

load_dotenv()

DB_HOST = os.getenv('DB_HOST_PROD')
DB_PORT = os.getenv('DB_PORT_PROD')
DB_NAME = os.getenv('DB_NAME_PROD')
DB_USER = os.getenv('DB_USER_PROD')
DB_PASS = os.getenv('DB_PASS_PROD')
DB_SCHEMA = os.getenv('DB_SCHEMA_PROD')
DB_THREADS = os.getenv('DB_THREADS_PROD')
DB_TYPE = os.getenv('DB_TYPE_PROD')
DBT_PROFILES_DIR =  os.getenv('DBT_PROFILES_DIR')

commodities = ['CL=F' , 'GC=F', "SI=F"]

engine = sqlalchemy.engine.create_engine(f"postgresql://{DB_USER}:{DB_PORT}@{DB_HOST}/{DB_NAME}")

def buscar_dados_commodities(símbolo, periodo = '5y', intervalo = '1d'):
    ticker = yf.Ticker(símbolo)
    dados = ticker.history(period = periodo, interval = intervalo)[['Close']]
    dados['símbolo'] = símbolo
    return dados

def buscar_todos_dados_commodities(commodities):
    todos_dados = []
    for símbolos in commodities:
        dados = buscar_dados_commodities(símbolos)
        todos_dados.append(dados)
    return pd.concat(todos_dados)

def salvar_no_postgres(df, schema = 'public'):
    df.to_sql('commodities', engine, if_exists='replace', index=True, schema = schema)

if __name__ == '__main__':
    dados_concat = buscar_todos_dados_commodities(commodities)
    salvar_no_postgres(dados_concat, schema='public')