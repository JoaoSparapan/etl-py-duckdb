import os
import gdown
import duckdb
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

from duckdb import DuckDBPyRelation
from pandas import DataFrame

load_dotenv()

def download_files_from_googledrive(url_pasta, diretorio_local):
    os.makedirs(diretorio_local, exist_ok=True)
    gdown.download_folder(url_pasta, output=diretorio_local, quiet=False, use_cookies=False)
    
def list_files(diretorio):
    arquivos = []
    todos_arquivos = os.listdir(diretorio)
    for arquivo in todos_arquivos:
        if arquivo.endswith('.csv'):
            caminho_completo = os.path.join(diretorio, arquivo)
            arquivos.append(caminho_completo)
    return arquivos
    
def read_csv(caminho):
    df = duckdb.read_csv(caminho)
    print(df)
    print(type(df))
    return df

def transform(df: DuckDBPyRelation) -> DataFrame:
    df_transformado = duckdb.sql("SELECT *, quantidade * valor as total_vendas FROM df").df()
    return df_transformado

def save_on_postgres(df_duckdb, tabela):
    DATABASE_URL = os.getenv("DATABASE_URL")  # Ex: 'postgresql://user:password@localhost:5432/database_name'
    engine = create_engine(DATABASE_URL)
    # Salvar o DataFrame no PostgreSQL
    df_duckdb.to_sql(tabela, con=engine, if_exists='append', index=False)
    

if __name__ == "__main__":
    url_pasta = 'https://drive.google.com/drive/folders/1Xd1eqLvR1aF3Cp8YMXdjJ5HkI6sTmqyv'
    diretorio_local = './pasta_gdown'
    # download_files_from_googledrive(url_pasta, diretorio_local)
    files = list_files(diretorio_local)
    for file_path in files:
        df_duckdb = read_csv(file_path)
        df_pandas = transform(df_duckdb)
        save_on_postgres(df_pandas, "vendas_calculado")