-- Databricks notebook source
CREATE SCHEMA ifood_catalog.trusted_layer
COMMENT 'Camada trusted'
MANAGED LOCATION 's3://datalake-ifood/trusted_layer/';

-- COMMAND ----------

CREATE SCHEMA ifood_catalog.raw_layer
COMMENT 'Camada raw'
MANAGED LOCATION 's3://datalake-ifood/raw_layer/';
