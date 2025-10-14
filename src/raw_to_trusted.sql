-- Databricks notebook source
CREATE TABLE IF NOT EXISTS IDENTIFIER(:catalogo || '.' || :schema || '.' || :table) (
  vendor_id BIGINT,
  tpep_pickup_datetime TIMESTAMP,
  tpep_dropoff_datetime TIMESTAMP,
  passenger_count DOUBLE,
  total_amount DOUBLE
)
COMMENT 'Tabela dimensão que registra as informações dos pedidos cancelados'
CLUSTER BY (vendor_id)
LOCATION 's3://datalake-ifood/trusted_layer/tb_taxi_data_for_analysis' ;

-- COMMAND ----------

MERGE WITH SCHEMA EVOLUTION INTO IDENTIFIER(:catalogo || '.' || :schema || '.' || :table)
USING (
  WITH raw_data as (
    SELECT 
      DISTINCT
      VendorID as vendor_id,
      tpep_pickup_datetime,
      tpep_dropoff_datetime,
      passenger_count,
      total_amount
      FROM `ifood_catalog`.raw_layer.tb_taxi_data_api
  )
  SELECT * FROM raw_data
) AS tb_taxi_data_for_analysis_tmp
ON tb_taxi_data_for_analysis.vendor_id = tb_taxi_data_for_analysis_tmp.vendor_id
AND tb_taxi_data_for_analysis.tpep_pickup_datetime = tb_taxi_data_for_analysis_tmp.tpep_pickup_datetime
AND tb_taxi_data_for_analysis.tpep_dropoff_datetime = tb_taxi_data_for_analysis_tmp.tpep_dropoff_datetime

--A condição "WHEN MATCHED THEN UPDATE SET *" não pode ser considerada para fazer atualização pois há uma anomalia nos dados conforme exemplo abaixo onde para um mesmo VendorID, tpep_pickup_datetime e tpep_dropoff_datetime, existem registros com valores exatamente opostos. Exemplo:
-- SELECT *  FROM tb_taxi_data_api
-- WHERE VendorID = 2 
-- AND tpep_pickup_datetime = "2023-01-08T01:00:50.000+00:00" 
-- AND tpep_dropoff_datetime = "2023-01-08T01:10:47.000+00:00"

WHEN NOT MATCHED
    THEN INSERT *
WHEN NOT MATCHED BY SOURCE THEN DELETE


-- COMMAND ----------


