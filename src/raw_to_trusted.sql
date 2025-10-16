-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Transformação: Raw Layer -> Trusted Layer
-- MAGIC
-- MAGIC Este notebook é responsável por transformar e refinar os dados da camada Raw
-- MAGIC para a camada Trusted, aplicando regras de negócio e mantendo apenas as
-- MAGIC colunas necessárias para análise.
-- MAGIC
-- MAGIC **Autor:** Vinicius  
-- MAGIC **Data:** 2025
-- MAGIC
-- MAGIC ## Objetivo
-- MAGIC - Selecionar apenas colunas relevantes para análise (conforme requisitos do case)
-- MAGIC - Remover duplicatas mantendo integridade referencial
-- MAGIC - Aplicar limpeza e padronização de dados
-- MAGIC - Garantir qualidade através de operação MERGE idempotente
-- MAGIC
-- MAGIC ## Colunas Mantidas (Requisitos do Case)
-- MAGIC - **VendorID**: Identificador do fornecedor de táxi
-- MAGIC - **tpep_pickup_datetime**: Data/hora de início da corrida
-- MAGIC - **tpep_dropoff_datetime**: Data/hora de fim da corrida
-- MAGIC - **passenger_count**: Quantidade de passageiros
-- MAGIC - **total_amount**: Valor total da corrida
-- MAGIC
-- MAGIC ## Estratégia de Merge
-- MAGIC - **Chave Composta**: vendor_id + tpep_pickup_datetime + tpep_dropoff_datetime
-- MAGIC - **Insert**: Novos registros da raw são inseridos
-- MAGIC - **Delete**: Registros que não existem mais na raw são removidos (SCD Type 1)
-- MAGIC - **Sem Update**: Devido à anomalia de dados duplicados com valores opostos
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1. Criação da Tabela Trusted
-- MAGIC
-- MAGIC Cria a tabela na camada Trusted se ela não existir, definindo:
-- MAGIC - Schema simplificado com apenas colunas necessárias
-- MAGIC - Clustering por vendor_id para otimizar queries filtradas
-- MAGIC - Localização no S3 para persistência
-- MAGIC %md

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS IDENTIFIER(:catalogo || '.' || :schema || '.' || :table) (
  vendor_id BIGINT COMMENT 'Identificador do fornecedor de táxi (1 = Creative Mobile Technologies, 2 = VeriFone Inc)',
  tpep_pickup_datetime TIMESTAMP COMMENT 'Data e hora em que o taxímetro foi acionado',
  tpep_dropoff_datetime TIMESTAMP COMMENT 'Data e hora em que o taxímetro foi desligado',
  passenger_count INTEGER COMMENT 'Número de passageiros no veículo (valor informado pelo motorista)',
  total_amount DOUBLE COMMENT 'Valor total cobrado dos passageiros (não inclui gorjetas em dinheiro)'
)
COMMENT 'Tabela trusted contendo dados refinados de corridas de táxi de NY para análise'
CLUSTER BY (vendor_id)
LOCATION 's3://datalake-ifood/trusted_layer/tb_taxi_data_for_analysis';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2. Merge de Dados: Raw -> Trusted
-- MAGIC
-- MAGIC Operação MERGE que:
-- MAGIC 1. **Lê dados da camada Raw** aplicando SELECT DISTINCT para remover duplicatas
-- MAGIC 2. **Renomeia VendorID** para vendor_id (padronização snake_case)
-- MAGIC 3. **Seleciona apenas colunas necessárias** conforme requisitos
-- MAGIC 4. **Aplica MERGE com três condições**:
-- MAGIC    - **WHEN NOT MATCHED**: Insere novos registros
-- MAGIC    - **WHEN NOT MATCHED BY SOURCE**: Remove registros que não existem mais na origem
-- MAGIC    - **WHEN MATCHED**: NÃO atualiza devido à anomalia nos dados (ver observações)
-- MAGIC
-- MAGIC ### Possível Anomalia Detectada
-- MAGIC Existem registros com mesma chave composta mas valores diferentes em passenger_count
-- MAGIC e total_amount (valores exatamente opostos), possivelmente a viagem foi cancelada. Exemplo:
-- MAGIC ```
-- MAGIC VendorID=2, pickup='2023-01-08 01:00:50', dropoff='2023-01-08 01:10:47'
-- MAGIC → Registro A: passenger_count=1, total_amount=15.50
-- MAGIC → Registro B: passenger_count=1, total_amount=-15.50
-- MAGIC ```
-- MAGIC Por isso, não implementamos WHEN MATCHED THEN UPDATE.

-- COMMAND ----------

MERGE WITH SCHEMA EVOLUTION INTO IDENTIFIER(:catalogo || '.' || :schema || '.' || :table)
USING (
  WITH raw_data as (
    SELECT 
      DISTINCT
      VendorID::INTEGER as vendor_id,
      tpep_pickup_datetime::TIMESTAMP,
      tpep_dropoff_datetime::TIMESTAMP,
      passenger_count::INTEGER,
      total_amount::DOUBLE
    FROM `ifood_catalog`.raw_layer.tb_taxi_data_api
    WHERE date_reference >= concat(:start_date, '-01') and date_reference <= concat(:end_date, '-01')
    AND insertion_date = (SELECT max(insertion_date) FROM `ifood_catalog`.raw_layer.tb_taxi_data_api)
  )
  SELECT * FROM raw_data
) AS tb_taxi_data_for_analysis_tmp
ON tb_taxi_data_for_analysis.vendor_id = tb_taxi_data_for_analysis_tmp.vendor_id
AND tb_taxi_data_for_analysis.tpep_pickup_datetime = tb_taxi_data_for_analysis_tmp.tpep_pickup_datetime
AND tb_taxi_data_for_analysis.tpep_dropoff_datetime = tb_taxi_data_for_analysis_tmp.tpep_dropoff_datetime
WHEN NOT MATCHED
    THEN INSERT *
WHEN NOT MATCHED BY SOURCE THEN DELETE

