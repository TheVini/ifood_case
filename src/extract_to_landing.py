# Databricks notebook source
"""
Módulo de Extração de Dados - Landing Layer

Este módulo é responsável por fazer o download dos arquivos Parquet de corridas
de táxi de NY (NYC TLC Trip Record Data) e armazená-los na Landing Zone do Data Lake.

Autor: Vinicius
Data: 2025
"""

# COMMAND ----------

# Necessário pois só há máquinas serverless no Databricks
%pip install loguru

# COMMAND ----------

from datetime import datetime
from dateutil.relativedelta import relativedelta
from botocore.exceptions import ClientError
from loguru import logger
from typing import Tuple, List

import os
import requests
import re
import boto3

# COMMAND ----------

def validar_datas(start_date: str, end_date: str) -> Tuple[datetime, datetime]:
    """
    Valida os valores de start_date e end_date.
    
    Verifica se as datas estão no formato correto (YYYY-MM), se a data inicial
    não é posterior à data final e se não são datas futuras.
    
    Args:
        start_date (str): Data de início no formato YYYY-MM (ex: "2023-01")
        end_date (str): Data de fim no formato YYYY-MM (ex: "2023-05")
    
    Returns:
        Tuple[datetime, datetime]: Tupla contendo (data_inicio, data_fim) convertidas
                                   para objetos datetime
    
    Raises:
        ValueError: Se o formato das datas for inválido, se data_inicio > data_fim,
                   ou se as datas estiverem no futuro
    
    Examples:
        >>> validar_datas("2023-01", "2023-05")
        (datetime(2023, 1, 1, 0, 0), datetime(2023, 5, 1, 0, 0))
        
        >>> validar_datas("2023-13", "2023-05")  # Mês inválido
        ValueError: data_inicio inválida: '2023-13'. Formato esperado: YYYY-MM
    """
    # Regex para garantir o formato correto YYYY-MM
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

    # Bloquear datas futuras
    hoje = datetime.today()
    if data_inicio > hoje or data_fim > hoje:
        raise ValueError("Datas não podem estar no futuro.")

    return data_inicio, data_fim

# COMMAND ----------

def generate_month_range(start_date: str, end_date: str) -> List[Tuple[int, int]]:
    """
    Gera uma lista de tuplas (ano, mês) entre start_date e end_date (inclusive).
    
    Útil para iterar sobre todos os meses em um intervalo de datas para processar
    dados mensais sequencialmente.
    
    Args:
        start_date (str): Data de início no formato YYYY-MM
        end_date (str): Data de fim no formato YYYY-MM
    
    Returns:
        List[Tuple[int, int]]: Lista de tuplas contendo (ano, mês) para cada mês
                               no intervalo especificado
    
    Examples:
        >>> generate_month_range("2023-01", "2023-03")
        [(2023, 1), (2023, 2), (2023, 3)]
        
        >>> generate_month_range("2022-11", "2023-02")
        [(2022, 11), (2022, 12), (2023, 1), (2023, 2)]
    """
    start, end = validar_datas(start_date, end_date)

    current = start
    months = []

    while current <= end:
        months.append((current.year, current.month))
        current += relativedelta(months=1)

    return months

# COMMAND ----------

def download_taxi_data(year: int, month: int, landing_path: str) -> str:
    """
    Baixa o arquivo parquet de um mês/ano específico da NYC TLC e salva no S3.
    
    Realiza o download do arquivo diretamente da CDN do NYC TLC e faz upload
    para o bucket S3 especificado. Implementa verificação de existência para
    evitar uploads duplicados (comportamento idempotente).
    
    Args:
        year (int): Ano da referência dos dados (ex: 2023)
        month (int): Mês da referência dos dados (1-12)
        landing_path (str): Caminho completo do S3 onde o arquivo será salvo
                           (formato: s3://bucket-name/path/)
    
    Returns:
        str: Caminho completo do arquivo no S3 se o download foi bem-sucedido,
             None caso contrário
    
    Raises:
        ClientError: Se houver erro na comunicação com o S3 (exceto 404)
    
    Side Effects:
        - Faz requisição HTTP à CDN do NYC TLC
        - Faz upload de arquivo para o S3 (se não existir)
        - Registra logs usando loguru (info, warning, error)
    
    Examples:
        >>> download_taxi_data(2023, 5, "s3://meu-bucket/landing/yellow/")
        "s3://meu-bucket/landing/yellow/yellow_tripdata_2023-05.parquet"
    
    Notes:
        - URL base: https://d37ci6vzurychx.cloudfront.net/trip-data/
        - Formato do arquivo: yellow_tripdata_YYYY-MM.parquet
        - Requer credenciais AWS configuradas via dbutils.widgets
    """
    file_name = f"yellow_tripdata_{year}-{month:02d}.parquet"
    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{file_name}"
    local_path = os.path.join(landing_path, file_name)

    print(f"📥 Baixando {url} ...")
    response = requests.get(url)
    
    if response.status_code == 200:
        # Configurar cliente S3
        s3 = boto3.client(
            "s3",
            aws_access_key_id=dbutils.widgets.get("aws_access_key_id"),
            aws_secret_access_key=dbutils.widgets.get("aws_secret_access_key"),
            region_name="us-east-1"
        )
        
        # Extrair bucket e key do caminho S3
        bucket_name = local_path.split('/')[2]
        object_key = local_path.split('/', 3)[3]

        try:
            # Verificar se o arquivo já existe no S3
            s3.head_object(Bucket=bucket_name, Key=object_key)
            logger.warning(f"⚠️ O arquivo '{object_key}' já existe no bucket '{bucket_name}'. Nenhum upload foi feito.")
        except ClientError as e:
            if e.response['Error']['Code'] == "404":
                # Arquivo não existe – pode fazer o upload
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

def run_pipeline(start_date: str, end_date: str, datalake_path: str) -> None:
    """
    Executa o pipeline completo de extração de dados para a Landing Zone.
    
    Orquestra o processo de download de todos os arquivos mensais entre as datas
    especificadas, salvando cada arquivo no caminho de destino do Data Lake.
    
    Args:
        start_date (str): Data de início no formato YYYY-MM
        end_date (str): Data de fim no formato YYYY-MM
        datalake_path (str): Caminho base do Data Lake (ex: s3://datalake-ifood/landing_layer)
    
    Returns:
        None
    
    Side Effects:
        - Baixa múltiplos arquivos da NYC TLC CDN
        - Faz upload para o S3
        - Registra logs de progresso
    
    Examples:
        >>> run_pipeline("2023-01", "2023-05", "s3://datalake-ifood/landing_layer")
        # Baixa e processa arquivos de Jan-2023 a Mai-2023
    
    Notes:
        - Processa os meses sequencialmente (não paralelo)
        - Continua o processamento mesmo se um mês falhar
        - Todos os arquivos são salvos no subdiretório 'yellow/'
    """
    months = generate_month_range(start_date, end_date)

    for year, month in months:
        landing = f"{datalake_path}/yellow/"
        local_file = download_taxi_data(year, month, landing)

# COMMAND ----------

if __name__ == "__main__":
    """
    Ponto de entrada principal do notebook quando executado via Databricks Job.
    
    Widgets esperados:
        - datalake_path: Caminho base do Data Lake (ex: s3://datalake-ifood/landing_layer)
        - start_date: Data início no formato YYYY-MM
        - end_date: Data fim no formato YYYY-MM
        - aws_access_key_id: AWS Access Key ID
        - aws_secret_access_key: AWS Secret Access Key
    """
    datalake_path = dbutils.widgets.get("datalake_path")
    start_date = dbutils.widgets.get("start_date")
    end_date = dbutils.widgets.get("end_date")
    run_pipeline(start_date, end_date, datalake_path)
