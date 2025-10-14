# Databricks notebook source
# MAGIC %pip install loguru

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import (
    col, year, month, hour, avg, round, count, sum, 
    when, to_date, to_timestamp, lit
)

from datetime import datetime
from dateutil.relativedelta import relativedelta
from loguru import logger

import re
import pytz

now = datetime.now(tz=pytz.timezone("America/Sao_Paulo")).date()

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS IDENTIFIER(:catalogo || '.' || :schema || '.' || :table) (
# MAGIC   VendorID BIGINT,
# MAGIC   tpep_pickup_datetime TIMESTAMP,
# MAGIC   tpep_dropoff_datetime TIMESTAMP,
# MAGIC   passenger_count DOUBLE,
# MAGIC   trip_distance DOUBLE,
# MAGIC   RatecodeID DOUBLE,
# MAGIC   store_and_fwd_flag STRING,
# MAGIC   PULocationID BIGINT,
# MAGIC   DOLocationID BIGINT,
# MAGIC   payment_type BIGINT,
# MAGIC   fare_amount DOUBLE,
# MAGIC   extra DOUBLE,
# MAGIC   mta_tax DOUBLE,
# MAGIC   tip_amount DOUBLE,
# MAGIC   tolls_amount DOUBLE,
# MAGIC   improvement_surcharge DOUBLE,
# MAGIC   total_amount DOUBLE,
# MAGIC   congestion_surcharge DOUBLE,
# MAGIC   airport_fee DOUBLE,
# MAGIC   date_reference DATE,
# MAGIC   insertion_date DATE
# MAGIC )
# MAGIC COMMENT 'Tabela com os dados brutos de TAXI de NY.'
# MAGIC CLUSTER BY (date_reference, VendorID)
# MAGIC LOCATION 's3://datalake-ifood/raw_layer/tb_taxi_data_api' ;

# COMMAND ----------

spark = SparkSession.builder \
    .appName("iFood-NYC-Taxi-Pipeline") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

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

start_date = dbutils.widgets.get("start_date")
end_date = dbutils.widgets.get("end_date")
catalogo = dbutils.widgets.get("catalogo")
schema = dbutils.widgets.get("schema")
table = dbutils.widgets.get("table")

months = generate_month_range(start_date, end_date)

# COMMAND ----------

def check_colums(sample_df, expected_schema):
    df_columns = set(sample_df.columns)
    expected_columns = set([f.name for f in expected_schema])
    missing_columns = expected_columns - df_columns
    extra_columns = df_columns - expected_columns
    return expected_columns, missing_columns, extra_columns

def treat_and_test(sample_df):
    expected_schema = StructType([
        StructField("VendorID", LongType(), True),
        StructField("tpep_pickup_datetime", TimestampType(), True),
        StructField("tpep_dropoff_datetime", TimestampType(), True),
        StructField("passenger_count", DoubleType(), True),
        StructField("trip_distance", DoubleType(), True),
        StructField("RatecodeID", DoubleType(), True),
        StructField("store_and_fwd_flag", StringType(), True),
        StructField("PULocationID", LongType(), True),
        StructField("DOLocationID", LongType(), True),
        StructField("payment_type", LongType(), True),
        StructField("fare_amount", DoubleType(), True),
        StructField("extra", DoubleType(), True),
        StructField("mta_tax", DoubleType(), True),
        StructField("tip_amount", DoubleType(), True),
        StructField("tolls_amount", DoubleType(), True),
        StructField("improvement_surcharge", DoubleType(), True),
        StructField("total_amount", DoubleType(), True),
        StructField("congestion_surcharge", DoubleType(), True),
        StructField("airport_fee", DoubleType(), True),
        StructField("date_reference", DateType(), True),
        StructField("insertion_date", DateType(), True),
    ])
    # 1. Primeira verificação: verifica se todas as colunas esperadas existem no DataFrame
    expected_columns, missing_columns, extra_columns = check_colums(sample_df, expected_schema)

    # 2. Verificar se o extra_columns não está vazio, se não tiver, verificar se a extra_column se parece com alguma coluna do expected_columns e só esta apenas com o case sensitive diferente, se for o caso, renomear a extra_column para o nome correto
    for extra_column in extra_columns:
        for expected_column in expected_columns:
            if extra_column.lower() == expected_column.lower():
                sample_df = sample_df.withColumnRenamed(extra_column, expected_column)

    # 3. Verificar se tem alguma missing_columns, se sim, criar a coluna faltante com o nome correto e colocar o valor NULO e com o tipo correspondente do expected_schema
    for missing_column in missing_columns:
        for expected_column in expected_columns:
            if missing_column.lower() == expected_column.lower():
                sample_df = sample_df.withColumn(missing_column, lit(None).cast(expected_schema[expected_column].dataType))

    # 4. Converter o tipo de todas as colunas para o tipo esperado do expected_schema
    for column in sample_df.columns:
        sample_df = sample_df.withColumn(column, sample_df[column].cast(expected_schema[column].dataType))

    # 5. Primeira verificação: verifica se todas as colunas esperadas existem no DataFrame
    expected_columns, missing_columns, extra_columns = check_colums(sample_df, expected_schema)
    
    # 6. Ordernar as colunas
    sample_df = sample_df.select(*[f.name for f in expected_schema])

    # 7. Testes
    assert not missing_columns, f"Colunas faltando: {missing_columns}"
    assert not extra_columns, f"Colunas extras não esperadas: {extra_columns}"
    assert not sample_df.isEmpty()
    logger.info(f"Testes concluídos com sucesso")
    return sample_df

# COMMAND ----------

for each_month in months:
    try:
        date_base = f"{each_month[0]}-{each_month[1]:02}"
        date_reference = f"{date_base}-01"

        # Leitura dos dados
        sample_df = spark.read.parquet(f"s3://datalake-ifood/landing_layer/yellow/yellow_tripdata_{date_base}.parquet")
        sample_df = sample_df.withColumn(
            "tpep_pickup_datetime",
            col("tpep_pickup_datetime").cast("timestamp")
        ).withColumn(
            "tpep_dropoff_datetime",
            col("tpep_dropoff_datetime").cast("timestamp")
        ).withColumn(
            "date_reference",
            to_date(lit(f"{date_reference}"), "yyyy-MM-dd")
        ).withColumn(
            "insertion_date",
            to_date(lit(f"{date_reference}"), "yyyy-MM-dd")
        )

        # Testes
        sample_df = treat_and_test(sample_df)

        # Remover a data para inserir os dados novos
        spark.sql(f"""
            DELETE FROM {catalogo}.{schema}.{table}
            WHERE date_reference = '{date_reference}'
        """)

        # Salvar dados
        sample_df.write.format("delta") \
            .mode("append") \
            .saveAsTable(f"{catalogo}.{schema}.{table}")
        logger.success(f"Dado persistido com sucesso para a data de: '{date_base}'")

    except Exception as e:
        logger.error(f"Erro ao processar a data '{date_base}': {str(e)}")
        # Possível enviar um alerta ou os dados para uma dead letter para serem tratados
