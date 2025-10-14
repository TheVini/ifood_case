# Databricks notebook source
# MAGIC %pip install loguru

# COMMAND ----------

from datetime import datetime
from dateutil.relativedelta import relativedelta
from botocore.exceptions import ClientError
from loguru import logger

import os
import requests
import re
import boto3

# COMMAND ----------

def validar_datas(start_date: str, end_date: str):
    """
    Valida os valores de start_date e end_date.
    Formato esperado: YYYY-MM
    """
    # Regex para garantir o formato correto
    padrao = r"^\d{4}-(0[1-9]|1[0-2])$"
    
    if not re.match(padrao, start_date):
        raise ValueError(f"data_inicio inválida: '{start_date}'. Formato esperado: YYYY-MM")
    if not re.match(padrao, end_date):
        raise ValueError(f"data_fim inválida: '{end_date}'. Formato esperado: YYYY-MM")

    # Converter para datetime
    data_inicio = datetime.strptime(start_date, "%Y-%m")
    data_fim = datetime.strptime(end_date, "%Y-%m")

    # Verificar se início vem antes do fim
    if data_inicio > data_fim:
        raise ValueError(f"data_inicio ({start_date}) não pode ser posterior à data_fim ({end_date}).")

    # (Opcional) Bloquear datas futuras
    hoje = datetime.today()
    if data_inicio > hoje or data_fim > hoje:
        raise ValueError("Datas não podem estar no futuro.")

    return data_inicio, data_fim

# COMMAND ----------

def generate_month_range(start_date: str, end_date: str):
    """
    Gera uma lista (ano, mês) entre start_date e end_date (inclusive).
    Exemplo: ("2023-01", "2023-03") -> [(2023, 1), (2023, 2), (2023, 3)]
    """
    start, end = validar_datas(start_date, end_date)

    current = start
    months = []

    while current <= end:
        months.append((current.year, current.month))
        current += relativedelta(months=1)

    return months

# COMMAND ----------

def download_taxi_data(year: int, month: int, landing_path: str):
    """
    Baixa o arquivo parquet de um mês/ano da NYC TLC e salva na landing zone.
    """
    file_name = f"yellow_tripdata_{year}-{month:02d}.parquet"
    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{file_name}"
    local_path = os.path.join(landing_path, file_name)

    print(f"📥 Baixando {url} ...")
    response = requests.get(url)
    if response.status_code == 200:
        s3 = boto3.client(
            "s3",
            aws_access_key_id=dbutils.widgets.get("aws_access_key_id"),
            aws_secret_access_key=dbutils.widgets.get("aws_secret_access_key"),
            region_name="us-east-1"
        )
        bucket_name = local_path.split('/')[2]
        object_key = local_path.split('/', 3)[3]

        try:
            s3.head_object(Bucket=bucket_name, Key=object_key)
            logger.warning(f"⚠️ O arquivo '{object_key}' já existe no bucket '{bucket_name}'. Nenhum upload foi feito.")
        except ClientError as e:
            if e.response['Error']['Code'] == "404":
                # ✅ Arquivo não existe — pode fazer o upload
                s3.put_object(Bucket=bucket_name, Key=object_key, Body=response.content)
                logger.info(f"✅ Arquivo '{object_key}' enviado com sucesso para o bucket '{bucket_name}'.")
            else:
                # Outro erro inesperado
                logger.error(f"❌ Erro ao verificar existência do arquivo: {e}")
    else:
        print(f"❌ Erro ao baixar {file_name}: {response.status_code}")
        local_path = None
    
    return local_path

# COMMAND ----------

def run_pipeline(start_date: str, end_date: str, datalake_path: str):
    """
    Executa o pipeline entre duas datas (YYYY-MM), salvando os resultados no datalake.
    """    
    months = generate_month_range(start_date, end_date)

    for year, month in months:
        landing = f"{datalake_path}/yellow/"

        local_file = download_taxi_data(year, month, landing)
    
    spark.stop()

# COMMAND ----------

import os
if __name__ == "__main__":
    datalake_path = dbutils.widgets.get("datalake_path")
    start_date = dbutils.widgets.get("start_date")
    end_date = dbutils.widgets.get("end_date")
    run_pipeline(start_date, end_date, datalake_path)
