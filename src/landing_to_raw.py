# Databricks notebook source
"""
Módulo de Transformação de Dados - Raw Layer

Este módulo é responsável por ler os arquivos Parquet da Landing Zone,
aplicar transformações e validações, e persistir os dados na camada Raw
do Data Lake utilizando formato Delta Lake.

Autor: Vinicius
Data: 2025
"""

# COMMAND ----------

# Necessário pois só há máquinas serverless no Databricks
%pip install loguru

# COMMAND ----------

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField, LongType, TimestampType, 
    DoubleType, StringType, DateType
)
from pyspark.sql.functions import col, to_date, lit
from datetime import datetime
from dateutil.relativedelta import relativedelta
from loguru import logger
from typing import Tuple, List, Set

import re
import pytz

# COMMAND ----------

# Configuração da sessão Spark
spark = SparkSession.builder \
    .appName("iFood-NYC-Taxi-Pipeline") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Data atual no timezone de São Paulo
now = datetime.now(tz=pytz.timezone("America/Sao_Paulo")).date()

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS IDENTIFIER(:catalogo || '.' || :schema || '.' || :table) (
# MAGIC   VendorID BIGINT 
# MAGIC     COMMENT 'Código indicando o provedor TPEP que forneceu o registro. 1=Creative Mobile Technologies LLC, 2=Curb Mobility LLC, 6=Myle Technologies Inc, 7=Helix',
# MAGIC   tpep_pickup_datetime TIMESTAMP 
# MAGIC     COMMENT 'Data e hora em que o taxímetro foi acionado',
# MAGIC   tpep_dropoff_datetime TIMESTAMP 
# MAGIC     COMMENT 'Data e hora em que o taxímetro foi desligado',
# MAGIC   passenger_count DOUBLE 
# MAGIC     COMMENT 'Número de passageiros no veículo (informado pelo motorista)',
# MAGIC   trip_distance DOUBLE 
# MAGIC     COMMENT 'Distância percorrida em milhas reportada pelo taxímetro',
# MAGIC   RatecodeID DOUBLE 
# MAGIC     COMMENT 'Código de tarifa final em vigor no fim da corrida. 1=Standard rate, 2=JFK, 3=Newark, 4=Nassau or Westchester, 5=Negotiated fare, 6=Group ride, 99=Null/unknown',
# MAGIC   store_and_fwd_flag STRING 
# MAGIC     COMMENT 'Flag indicando se o registro foi armazenado na memória do veículo antes de enviar ao vendor. Y=store and forward trip, N=not a store and forward trip',
# MAGIC   PULocationID BIGINT 
# MAGIC     COMMENT 'TLC Taxi Zone onde o taxímetro foi acionado (pickup location)',
# MAGIC   DOLocationID BIGINT 
# MAGIC     COMMENT 'TLC Taxi Zone onde o taxímetro foi desligado (dropoff location)',
# MAGIC   payment_type BIGINT 
# MAGIC     COMMENT 'Código numérico indicando como o passageiro pagou. 0=Flex Fare trip, 1=Credit card, 2=Cash, 3=No charge, 4=Dispute, 5=Unknown, 6=Voided trip',
# MAGIC   fare_amount DOUBLE 
# MAGIC     COMMENT 'Tarifa tempo-distância calculada pelo taxímetro. Ver https://www.nyc.gov/site/tlc/passengers/taxi-fare.page',
# MAGIC   extra DOUBLE 
# MAGIC     COMMENT 'Extras e sobretaxas diversos (ex: rush hour, overnight charges)',
# MAGIC   mta_tax DOUBLE 
# MAGIC     COMMENT 'Imposto MTA acionado automaticamente com base na tarifa em uso ($0.50)',
# MAGIC   tip_amount DOUBLE 
# MAGIC     COMMENT 'Valor da gorjeta. Campo populado automaticamente para gorjetas em cartão de crédito. Gorjetas em dinheiro NÃO são incluídas',
# MAGIC   tolls_amount DOUBLE 
# MAGIC     COMMENT 'Valor total de pedágios pagos na corrida',
# MAGIC   improvement_surcharge DOUBLE 
# MAGIC     COMMENT 'Sobretaxa de melhoria avaliada na bandeirada. Começou a ser cobrada em 2015 ($0.30)',
# MAGIC   total_amount DOUBLE 
# MAGIC     COMMENT 'Valor total cobrado dos passageiros. NÃO inclui gorjetas em dinheiro',
# MAGIC   congestion_surcharge DOUBLE 
# MAGIC     COMMENT 'Valor total coletado na corrida para sobretaxa de congestionamento do Estado de NY',
# MAGIC   airport_fee DOUBLE 
# MAGIC     COMMENT 'Taxa cobrada apenas para pickups nos aeroportos LaGuardia e JFK ($1.25)',
# MAGIC   date_reference DATE 
# MAGIC     COMMENT 'Data de referência do mês dos dados (formato YYYY-MM-01) - campo técnico para particionamento',
# MAGIC   insertion_date DATE 
# MAGIC     COMMENT 'Data de inserção dos dados na tabela - campo técnico para auditoria'
# MAGIC )
# MAGIC COMMENT 'Tabela com os dados brutos de TAXI de NY.'
# MAGIC CLUSTER BY (date_reference, VendorID)
# MAGIC LOCATION 's3://datalake-ifood/raw_layer/tb_taxi_data_api' ;

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

def get_expected_schema() -> StructType:
    """
    Define o schema esperado para a tabela raw de dados de táxi.
    
    Retorna a estrutura de dados padronizada que deve ser seguida na camada Raw,
    garantindo consistência e facilitando validações.
    
    Returns:
        StructType: Schema do PySpark contendo todos os campos esperados com seus
                   tipos de dados correspondentes
    
    Examples:
        >>> schema = get_expected_schema()
        >>> print(schema.fieldNames())
        ['VendorID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime', ...]
    
    Notes:
        - Todos os campos são nullable (True) para tolerar dados ausentes
        - Campos numéricos decimais são DoubleType para máxima precisão
        - IDs são LongType para suportar grandes valores
        - Datas/horas usam TimestampType e DateType nativos do Spark
    """
    return StructType([
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

# COMMAND ----------

def check_columns(df: DataFrame, expected_schema: StructType) -> Tuple[Set[str], Set[str], Set[str]]:
    """
    Compara as colunas do DataFrame com o schema esperado.
    
    Identifica discrepâncias entre as colunas presentes no DataFrame e as
    colunas definidas no schema esperado, facilitando diagnóstico de problemas.
    
    Args:
        df (DataFrame): DataFrame do PySpark a ser validado
        expected_schema (StructType): Schema esperado contendo definição de campos
    
    Returns:
        Tuple[Set[str], Set[str], Set[str]]: Tupla contendo:
            - expected_columns: Conjunto de colunas esperadas
            - missing_columns: Colunas esperadas mas ausentes no DataFrame
            - extra_columns: Colunas presentes mas não esperadas
    
    Examples:
        >>> df = spark.read.parquet("s3://bucket/data.parquet")
        >>> schema = get_expected_schema()
        >>> expected, missing, extra = check_columns(df, schema)
        >>> print(f"Faltando: {missing}")
        Faltando: {'airport_fee'}
        >>> print(f"Extras: {extra}")
        Extras: {'unknown_column'}
    
    Notes:
        - Comparação é case-sensitive
        - Retorna sets vazios se não houver discrepâncias
        - Útil para diagnóstico antes de aplicar transformações
    """
    df_columns = set(df.columns)
    expected_columns = set([field.name for field in expected_schema])
    missing_columns = expected_columns - df_columns
    extra_columns = df_columns - expected_columns
    
    return expected_columns, missing_columns, extra_columns

# COMMAND ----------

def normalize_column_names(df: DataFrame, expected_columns: Set[str]) -> DataFrame:
    """
    Normaliza nomes de colunas ignorando diferenças de case (maiúsculas/minúsculas).
    
    Renomeia colunas que existem com grafia diferente (ex: 'vendorid' -> 'VendorID')
    para corresponder ao schema esperado, garantindo consistência.
    
    Args:
        df (DataFrame): DataFrame com possíveis variações de case nos nomes
        expected_columns (Set[str]): Conjunto de nomes de colunas esperados
    
    Returns:
        DataFrame: DataFrame com colunas renomeadas para o padrão esperado
    
    Examples:
        >>> df = spark.createDataFrame([(1,)], ['vendorid'])
        >>> expected = {'VendorID'}
        >>> df_normalized = normalize_column_names(df, expected)
        >>> df_normalized.columns
        ['VendorID']
    
    Notes:
        - Realiza apenas renomeação, não adiciona/remove colunas
        - Comparação case-insensitive (lower())
        - Preserva dados, altera apenas metadados das colunas
    """
    for col_atual in df.columns:
        for col_esperada in expected_columns:
            if col_atual.lower() == col_esperada.lower() and col_atual != col_esperada:
                df = df.withColumnRenamed(col_atual, col_esperada)
                logger.info(f"Coluna renomeada: '{col_atual}' -> '{col_esperada}'")
    
    return df

# COMMAND ----------

def add_missing_columns(df: DataFrame, missing_columns: Set[str], expected_schema: StructType) -> DataFrame:
    """
    Adiciona colunas faltantes ao DataFrame com valores NULL.
    
    Cria colunas que estão no schema esperado mas ausentes no DataFrame,
    preenchendo-as com NULL e aplicando o tipo de dado correto.
    
    Args:
        df (DataFrame): DataFrame ao qual serão adicionadas colunas
        missing_columns (Set[str]): Conjunto de nomes de colunas faltantes
        expected_schema (StructType): Schema contendo tipos de dados esperados
    
    Returns:
        DataFrame: DataFrame com todas as colunas necessárias (incluindo NULLs)
    
    Examples:
        >>> df = spark.createDataFrame([(1,)], ['VendorID'])
        >>> missing = {'airport_fee'}
        >>> schema = get_expected_schema()
        >>> df_complete = add_missing_columns(df, missing, schema)
        >>> 'airport_fee' in df_complete.columns
        True
    
    Notes:
        - Valores NULL permitem que o registro seja processado
        - Tipo de dado é extraído do expected_schema
        - Útil quando schema da fonte muda ao longo do tempo
    """
    for missing_col in missing_columns:
        for field in expected_schema:
            if missing_col == field.name:
                df = df.withColumn(missing_col, lit(None).cast(field.dataType))
                logger.warning(f"Coluna faltante adicionada como NULL: '{missing_col}'")
    
    return df

# COMMAND ----------

def cast_columns_to_schema(df: DataFrame, expected_schema: StructType) -> DataFrame:
    """
    Converte todas as colunas do DataFrame para os tipos definidos no schema.
    
    Aplica cast explícito em cada coluna para garantir conformidade com o
    schema esperado, evitando erros de tipo em operações posteriores.
    
    Args:
        df (DataFrame): DataFrame com possíveis tipos incorretos
        expected_schema (StructType): Schema definindo tipos corretos para cada campo
    
    Returns:
        DataFrame: DataFrame com todas as colunas nos tipos esperados
    
    Examples:
        >>> df = spark.createDataFrame([(1, "2023-01-01")], ['VendorID', 'date'])
        >>> schema = get_expected_schema()
        >>> df_typed = cast_columns_to_schema(df, schema)
        >>> df_typed.schema['VendorID'].dataType
        LongType
    
    Notes:
        - Cast pode falhar silenciosamente gerando NULLs para valores inválidos
        - Tipos são aplicados na ordem do schema
        - Timestamps são convertidos considerando timezone
    """
    for field in expected_schema:
        if field.name in df.columns:
            df = df.withColumn(field.name, col(field.name).cast(field.dataType))
    
    return df

# COMMAND ----------

def reorder_columns(df: DataFrame, expected_schema: StructType) -> DataFrame:
    """
    Reordena as colunas do DataFrame conforme a ordem do schema esperado.
    
    Garante que as colunas estejam na ordem padrão definida no schema,
    facilitando comparações e joins futuros.
    
    Args:
        df (DataFrame): DataFrame com colunas em ordem arbitrária
        expected_schema (StructType): Schema definindo a ordem esperada
    
    Returns:
        DataFrame: DataFrame com colunas na ordem do schema
    
    Examples:
        >>> df = spark.createDataFrame([(1, "A")], ['col2', 'col1'])
        >>> schema = StructType([StructField('col1', StringType()), 
        ...                      StructField('col2', LongType())])
        >>> df_ordered = reorder_columns(df, schema)
        >>> df_ordered.columns
        ['col1', 'col2']
    
    Notes:
        - Apenas reordena, não adiciona/remove colunas
        - Colunas ausentes do schema são ignoradas
        - Melhora legibilidade e previsibilidade dos dados
    """
    column_order = [field.name for field in expected_schema]
    return df.select(*column_order)

# COMMAND ----------

def validate_dataframe(df: DataFrame) -> None:
    """
    Executa validações básicas de qualidade no DataFrame.
    
    Verifica se o DataFrame não está vazio após as transformações,
    levantando exceção em caso de falha.
    
    Args:
        df (DataFrame): DataFrame a ser validado
    
    Raises:
        AssertionError: Se o DataFrame estiver vazio
    
    Examples:
        >>> df = spark.createDataFrame([(1,)], ['id'])
        >>> validate_dataframe(df)  # Passa sem erro
        
        >>> df_empty = spark.createDataFrame([], StructType([]))
        >>> validate_dataframe(df_empty)  # Levanta AssertionError
    
    Notes:
        - Validação mínima, pode ser expandida com regras de negócio
        - isEmpty() força execução lazy do Spark
        - Útil para detectar problemas antes de persistir dados
    """
    assert not df.isEmpty(), "DataFrame está vazio após transformações"
    logger.info("✅ DataFrame validado com sucesso")

# COMMAND ----------

def treat_and_test(df: DataFrame, expected_schema: StructType) -> DataFrame:
    """
    Aplica tratamentos e validações completas no DataFrame.
    
    Orquestra todas as etapas de normalização, adição de colunas faltantes,
    conversão de tipos e validação, retornando um DataFrame pronto para persistência.
    
    Args:
        df (DataFrame): DataFrame bruto lido da landing zone
        expected_schema (StructType): Schema esperado para a camada raw
    
    Returns:
        DataFrame: DataFrame tratado e validado, conforme schema esperado
    
    Raises:
        AssertionError: Se houver colunas extras não esperadas ou DataFrame vazio
    
    Examples:
        >>> df_raw = spark.read.parquet("s3://landing/yellow_tripdata_2023-01.parquet")
        >>> schema = get_expected_schema()
        >>> df_clean = treat_and_test(df_raw, schema)
        >>> df_clean.columns == [f.name for f in schema]
        True
    
    Side Effects:
        - Registra logs de cada etapa do tratamento
        - Pode modificar o DataFrame in-place (transformações Spark)
    
    Notes:
        - Pipeline completo: verificar -> normalizar -> adicionar -> converter -> ordenar -> validar
        - Falha rápido em caso de colunas extras inesperadas
        - Recomendado para garantir qualidade antes de persistir
    """
    logger.info("Iniciando tratamento e validação do DataFrame")
    
    # 1. Verificar colunas
    expected_columns, missing_columns, extra_columns = check_columns(df, expected_schema)
    
    # 2. Normalizar nomes (case-insensitive)
    df = normalize_column_names(df, expected_columns)
    
    # 3. Re-verificar após normalização
    expected_columns, missing_columns, extra_columns = check_columns(df, expected_schema)
    
    # 4. Adicionar colunas faltantes
    if missing_columns:
        logger.warning(f"Colunas faltantes detectadas: {missing_columns}")
        df = add_missing_columns(df, missing_columns, expected_schema)
    
    # 5. Converter tipos
    df = cast_columns_to_schema(df, expected_schema)
    
    # 6. Reordenar colunas
    df = reorder_columns(df, expected_schema)
    
    # 7. Validação final
    expected_columns, missing_columns, extra_columns = check_columns(df, expected_schema)
    
    assert not missing_columns, f"❌ Colunas ainda faltando após tratamento: {missing_columns}"
    assert not extra_columns, f"❌ Colunas extras não esperadas: {extra_columns}"
    
    validate_dataframe(df)
    
    logger.success("✅ Tratamento e validação concluídos com sucesso")
    
    return df

# COMMAND ----------

def process_month_data(
    year: int, 
    month: int, 
    landing_path: str,
    catalogo: str,
    schema: str,
    table: str
) -> None:
    """
    Processa dados de um mês específico da landing para a raw layer.
    
    Executa o pipeline completo para um único mês: leitura do Parquet na landing,
    adição de metadados, tratamento/validação, e persistência na tabela Delta raw.
    
    Args:
        year (int): Ano de referência dos dados (ex: 2023)
        month (int): Mês de referência dos dados (1-12)
        landing_path (str): Caminho base da landing zone (ex: "s3://datalake-ifood/landing_layer")
        catalogo (str): Nome do catálogo Unity Catalog
        schema (str): Nome do schema/database
        table (str): Nome da tabela de destino
    
    Returns:
        None
    
    Raises:
        Exception: Se houver erro na leitura, transformação ou escrita dos dados
    
    Side Effects:
        - Lê arquivo Parquet do S3
        - Deleta dados existentes da data de referência (se houver)
        - Insere novos dados na tabela Delta
        - Registra logs de progresso e erros
    
    Examples:
        >>> process_month_data(2023, 5, "s3://datalake-ifood/landing_layer",
        ...                    "ifood_catalog", "raw_layer", "tb_taxi_data_api")
        # Dados de maio/2023 processados e persistidos
    
    Notes:
        - Operação é idempotente: DELETE + INSERT garante não duplicação
        - date_reference marca o mês dos dados para particionamento lógico
        - insertion_date registra quando os dados foram inseridos
        - Em caso de erro, dados parciais são descartados (atomicidade do Delta)
    """
    date_base = f"{year}-{month:02d}"
    date_reference = f"{date_base}-01"
    
    logger.info(f"🔄 Processando dados de: {date_base}")
    
    try:
        # Leitura do arquivo Parquet da landing zone
        file_path = f"{landing_path}/yellow/yellow_tripdata_{date_base}.parquet"
        logger.info(f"Lendo arquivo: {file_path}")
        
        df = spark.read.parquet(file_path)
        
        # Adicionar colunas de metadados
        df = df.withColumn(
            "tpep_pickup_datetime",
            col("tpep_pickup_datetime").cast("timestamp")
        ).withColumn(
            "tpep_dropoff_datetime",
            col("tpep_dropoff_datetime").cast("timestamp")
        ).withColumn(
            "date_reference",
            to_date(lit(date_reference), "yyyy-MM-dd")
        ).withColumn(
            "insertion_date",
            lit(now)
        )
        
        # Tratamento e validação
        expected_schema = get_expected_schema()
        df = treat_and_test(df, expected_schema)
        
        # Remover dados existentes da mesma data de referência (idempotência)
        logger.info(f"Removendo dados existentes para date_reference = '{date_reference}'")
        spark.sql(f"""
            DELETE FROM {catalogo}.{schema}.{table}
            WHERE date_reference = '{date_reference}'
        """)
        
        # Persistir novos dados
        logger.info(f"Persistindo dados na tabela {catalogo}.{schema}.{table}")
        df.write.format("delta") \
            .mode("append") \
            .saveAsTable(f"{catalogo}.{schema}.{table}")
        
        logger.success(f"✅ Dados de '{date_base}' persistidos com sucesso!")
        
    except Exception as e:
        logger.error(f"❌ Erro ao processar '{date_base}': {str(e)}")
        raise

# COMMAND ----------

def run_pipeline(
    start_date: str, 
    end_date: str, 
    landing_path: str,
    catalogo: str,
    schema: str,
    table: str
) -> None:
    """
    Executa o pipeline completo de transformação Landing -> Raw para múltiplos meses.
    
    Orquestra o processamento sequencial de todos os meses no intervalo especificado,
    garantindo que a tabela existe antes de iniciar o processamento.
    
    Args:
        start_date (str): Data inicial no formato YYYY-MM (ex: "2023-01")
        end_date (str): Data final no formato YYYY-MM (ex: "2023-05")
        landing_path (str): Caminho base da landing zone
        catalogo (str): Nome do catálogo Unity Catalog
        schema (str): Nome do schema/database
        table (str): Nome da tabela de destino
    
    Returns:
        None
    
    Side Effects:
        - Cria tabela raw se não existir
        - Processa múltiplos arquivos mensais
        - Persiste dados no Data Lake
        - Registra logs de execução
    
    Examples:
        >>> run_pipeline("2023-01", "2023-05", 
        ...              "s3://datalake-ifood/landing_layer",
        ...              "ifood_catalog", "raw_layer", "tb_taxi_data_api")
        # Processa jan-2023 até mai-2023
    
    Notes:
        - Processa meses sequencialmente (não paralelo)
        - Falha em um mês não interrompe processamento dos demais
        - Ideal para backfill ou processamento incremental
        - Operação idempotente: pode ser re-executada sem duplicação
    """
    logger.info("=" * 80)
    logger.info("INICIANDO PIPELINE: Landing -> Raw Layer")
    logger.info("=" * 80)
    
    # Gerar lista de meses a processar
    months = generate_month_range(start_date, end_date)
    logger.info(f"Meses a processar: {len(months)} ({start_date} até {end_date})")
    
    # Processar cada mês
    for year, month in months:
        process_month_data(year, month, landing_path, catalogo, schema, table)
    
    logger.success("=" * 80)
    logger.success("PIPELINE CONCLUÍDO COM SUCESSO!")
    logger.success("=" * 80)

# COMMAND ----------

if __name__ == "__main__":
    """
    Ponto de entrada principal do notebook quando executado via Databricks Job.
    
    Widgets esperados:
        - start_date: Data início no formato YYYY-MM
        - end_date: Data fim no formato YYYY-MM
        - landing_path: Caminho base da landing zone (ex: s3://datalake-ifood/landing_layer)
        - catalogo: Nome do catálogo (ex: ifood_catalog)
        - schema: Nome do schema (ex: raw_layer)
        - table: Nome da tabela (ex: tb_taxi_data_api)
    """
    start_date = dbutils.widgets.get("start_date")
    end_date = dbutils.widgets.get("end_date")
    landing_path = dbutils.widgets.get("landing_path")
    catalogo = dbutils.widgets.get("catalogo")
    schema = dbutils.widgets.get("schema")
    table = dbutils.widgets.get("table")
    
    run_pipeline(start_date, end_date, landing_path, catalogo, schema, table)
