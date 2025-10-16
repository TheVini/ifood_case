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

CREATE TABLE IF NOT EXISTS IDENTIFIER(:catalogo || '.' || :schema || '.tmp_' || :table) (
  vendor_id BIGINT COMMENT 'Identificador do fornecedor de táxi (1 = Creative Mobile Technologies, 2 = VeriFone Inc)',
  tpep_pickup_datetime TIMESTAMP COMMENT 'Data e hora em que o taxímetro foi acionado',
  tpep_dropoff_datetime TIMESTAMP COMMENT 'Data e hora em que o taxímetro foi desligado',
  passenger_count INTEGER COMMENT 'Número de passageiros no veículo (valor informado pelo motorista)',
  total_amount DOUBLE COMMENT 'Valor total cobrado dos passageiros (não inclui gorjetas em dinheiro)',
  date_reference DATE COMMENT 'Data de referência da tabela',
  insertion_date DATE COMMENT 'Data de inserção da tabela'
)
COMMENT 'Tabela trusted contendo dados refinados de corridas de táxi de NY para teste'
CLUSTER BY (vendor_id)
LOCATION 's3://datalake-ifood/trusted_layer/tmp_tb_taxi_data_for_analysis';

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS IDENTIFIER(:catalogo || '.' || :schema || '.' || :table_quarentena) ( 
  vendor_id BIGINT COMMENT 'Identificador do fornecedor de táxi (1 = Creative Mobile Technologies, 2 = VeriFone Inc)',
  tpep_pickup_datetime TIMESTAMP COMMENT 'Data e hora em que o taxímetro foi acionado',
  tpep_dropoff_datetime TIMESTAMP COMMENT 'Data e hora em que o taxímetro foi desligado',
  passenger_count INTEGER COMMENT 'Número de passageiros no veículo (valor informado pelo motorista)',
  total_amount DOUBLE COMMENT 'Valor total cobrado dos passageiros (não inclui gorjetas em dinheiro)',
  date_reference DATE COMMENT 'Data de referência da tabela',
  insertion_date DATE COMMENT 'Data de inserção da tabela',
  falha_tipo_vendor_id INTEGER,
  falha_tipo_pickup_datetime INTEGER,
  falha_tipo_dropoff_datetime INTEGER,
  falha_tipo_passenger_count INTEGER,
  falha_tipo_total_amount INTEGER,
  falha_vendor_id_nulo INTEGER,
  falha_pickup_nulo INTEGER,
  falha_dropoff_nulo INTEGER,
  falha_passenger_count_nulo INTEGER,
  falha_total_amount_nulo INTEGER,
  falha_vendor_id_invalido INTEGER,
  falha_passenger_count_invalido INTEGER,
  falha_dropoff_antes_pickup INTEGER,
  falha_duracao_excessiva INTEGER,
  falha_duracao_muito_curta INTEGER,
  falha_pickup_futuro INTEGER,
  falha_pickup_muito_antigo INTEGER,
  tem_erro INTEGER,
  data_hora_quarentena TIMESTAMP COMMENT 'Data e hora de registro na quarentena',
  motivos_erro STRING COMMENT 'Lista dos motivos do erro',
  tipo_erro STRING COMMENT 'Classificação do erro',
  data_particao_quarentena DATE COMMENT 'Data de partição da quarentena'
)
COMMENT 'Tabela de quarentena para registros de corridas de táxi de NY reprovados nas validações'
CLUSTER BY (vendor_id)
LOCATION 's3://datalake-ifood/trusted_layer/tb_taxi_data_quarentena';

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

-- COMMAND ----------

-- ============================================
-- Pipeline RAW -> TRUSTED - Taxi Data com Validação Completa
-- ============================================

-- 1. Extrair dados da RAW com conversões de tipo
MERGE WITH SCHEMA EVOLUTION INTO IDENTIFIER(:catalogo || '.' || :schema || '.tmp_' || :table)
USING (
  WITH raw_data as (
    SELECT 
      DISTINCT
      VendorID::INTEGER as vendor_id,
      tpep_pickup_datetime::TIMESTAMP,
      tpep_dropoff_datetime::TIMESTAMP,
      passenger_count::INTEGER,
      total_amount::DOUBLE,
      date_reference::DATE,
      insertion_date::DATE
    FROM `ifood_catalog`.raw_layer.tb_taxi_data_api
    WHERE date_reference >= concat(:start_date, '-01') and date_reference <= concat(:end_date, '-01')
    AND insertion_date = (SELECT max(insertion_date) FROM `ifood_catalog`.raw_layer.tb_taxi_data_api)
  )
  SELECT * FROM raw_data
) AS tb_taxi_data_for_analysis_tmp
ON tmp_tb_taxi_data_for_analysis.vendor_id = tb_taxi_data_for_analysis_tmp.vendor_id
AND tmp_tb_taxi_data_for_analysis.tpep_pickup_datetime = tb_taxi_data_for_analysis_tmp.tpep_pickup_datetime
AND tmp_tb_taxi_data_for_analysis.tpep_dropoff_datetime = tb_taxi_data_for_analysis_tmp.tpep_dropoff_datetime
WHEN NOT MATCHED
    THEN INSERT *
WHEN NOT MATCHED BY SOURCE THEN DELETE;

-- COMMAND ----------


-- 2. Aplicar VALIDAÇÕES DE TIPAGEM e QUALIDADE
CREATE OR REPLACE TEMPORARY VIEW vw_taxi_validado AS
SELECT 
  *,
  
  -- ========================================
  -- TESTES DE TIPAGEM
  -- ========================================
  
  -- Validar se vendor_id pode ser INTEGER
  CASE 
    WHEN vendor_id IS NULL THEN 0
    WHEN TRY_CAST(vendor_id AS INTEGER) IS NULL THEN 1 
    ELSE 0 
  END AS falha_tipo_vendor_id,
  
  -- Validar se pickup_datetime é TIMESTAMP válido
  CASE 
    WHEN tpep_pickup_datetime IS NULL THEN 0
    WHEN TRY_CAST(tpep_pickup_datetime AS TIMESTAMP) IS NULL THEN 1 
    ELSE 0 
  END AS falha_tipo_pickup_datetime,
  
  -- Validar se dropoff_datetime é TIMESTAMP válido
  CASE 
    WHEN tpep_dropoff_datetime IS NULL THEN 0
    WHEN TRY_CAST(tpep_dropoff_datetime AS TIMESTAMP) IS NULL THEN 1 
    ELSE 0 
  END AS falha_tipo_dropoff_datetime,
  
  -- Validar se passenger_count pode ser INTEGER
  CASE 
    WHEN passenger_count IS NULL THEN 0
    WHEN TRY_CAST(passenger_count AS INTEGER) IS NULL THEN 1 
    ELSE 0 
  END AS falha_tipo_passenger_count,
  
  -- Validar se total_amount pode ser DOUBLE
  CASE 
    WHEN total_amount IS NULL THEN 0
    WHEN TRY_CAST(total_amount AS DOUBLE) IS NULL THEN 1 
    ELSE 0 
  END AS falha_tipo_total_amount,
  
  -- ========================================
  -- TESTES DE QUALIDADE - Campos Obrigatórios
  -- ========================================
  
  CASE WHEN vendor_id IS NULL THEN 1 ELSE 0 END AS falha_vendor_id_nulo,
  CASE WHEN tpep_pickup_datetime IS NULL THEN 1 ELSE 0 END AS falha_pickup_nulo,
  CASE WHEN falha_tipo_dropoff_datetime IS NULL THEN 1 ELSE 0 END AS falha_dropoff_nulo,
  CASE WHEN passenger_count IS NULL THEN 1 ELSE 0 END AS falha_passenger_count_nulo,
  CASE WHEN total_amount IS NULL THEN 1 ELSE 0 END AS falha_total_amount_nulo,
  
  -- ========================================
  -- TESTES DE QUALIDADE - Valores Válidos
  -- ========================================
  
  -- Vendor ID deve ser positivo
  CASE 
    WHEN TRY_CAST(vendor_id AS INTEGER) IS NOT NULL 
      AND (TRY_CAST(vendor_id AS INTEGER) < 1)
    THEN 1 
    ELSE 0 
  END AS falha_vendor_id_invalido,
  
  -- Passenger count deve ser positivo
  CASE 
    WHEN TRY_CAST(passenger_count AS INTEGER) IS NOT NULL 
      AND (TRY_CAST(passenger_count AS INTEGER) < 1)
    THEN 1 
    ELSE 0 
  END AS falha_passenger_count_invalido,
  
  -- ========================================
  -- TESTES DE LÓGICA DE NEGÓCIO
  -- ========================================
  
    -- Dropoff deve ser DEPOIS do pickup
  CASE 
    WHEN TRY_CAST(tpep_pickup_datetime AS TIMESTAMP) IS NOT NULL 
      AND TRY_CAST(tpep_dropoff_datetime AS TIMESTAMP) IS NOT NULL
      AND TRY_CAST(tpep_dropoff_datetime AS TIMESTAMP) <= TRY_CAST(tpep_pickup_datetime AS TIMESTAMP)
    THEN 1 
    ELSE 0 
  END AS falha_dropoff_antes_pickup,
  
  -- Duração da viagem deve ser razoável (max 24 horas)
  CASE 
    WHEN TRY_CAST(tpep_pickup_datetime AS TIMESTAMP) IS NOT NULL 
      AND TRY_CAST(tpep_dropoff_datetime AS TIMESTAMP) IS NOT NULL
      AND (CAST(TRY_CAST(tpep_dropoff_datetime AS TIMESTAMP) AS LONG) - 
           CAST(TRY_CAST(tpep_pickup_datetime AS TIMESTAMP) AS LONG)) > 86400
    THEN 1 
    ELSE 0 
  END AS falha_duracao_excessiva,
  
  -- Duração mínima de 1 minuto
  CASE 
    WHEN TRY_CAST(tpep_pickup_datetime AS TIMESTAMP) IS NOT NULL 
      AND TRY_CAST(tpep_dropoff_datetime AS TIMESTAMP) IS NOT NULL
      AND (CAST(TRY_CAST(tpep_dropoff_datetime AS TIMESTAMP) AS LONG) - 
           CAST(TRY_CAST(tpep_pickup_datetime AS TIMESTAMP) AS LONG)) < 60
    THEN 1 
    ELSE 0 
  END AS falha_duracao_muito_curta,
  
  -- Pickup date não pode ser futuro
  CASE 
    WHEN TRY_CAST(tpep_pickup_datetime AS TIMESTAMP) IS NOT NULL 
      AND TRY_CAST(tpep_pickup_datetime AS TIMESTAMP) > current_timestamp()
    THEN 1 
    ELSE 0 
  END AS falha_pickup_futuro,
  
  -- Pickup date não pode ser muito antigo (> 10 anos)
  CASE 
    WHEN TRY_CAST(tpep_pickup_datetime AS TIMESTAMP) IS NOT NULL 
      AND TRY_CAST(tpep_pickup_datetime AS TIMESTAMP) < date_sub(current_date(), 3650)
    THEN 1 
    ELSE 0 
  END AS falha_pickup_muito_antigo,
  
  -- ========================================
  -- FLAG GERAL DE ERRO
  -- ========================================
  CASE 
    WHEN (TRY_CAST(vendor_id AS INTEGER) IS NULL AND vendor_id IS NOT NULL)
      OR (TRY_CAST(tpep_pickup_datetime AS TIMESTAMP) IS NULL AND tpep_pickup_datetime  IS NOT NULL)
      OR (TRY_CAST(tpep_dropoff_datetime AS TIMESTAMP) IS NULL AND tpep_dropoff_datetime  IS NOT NULL)
      OR (TRY_CAST(passenger_count AS INTEGER) IS NULL AND passenger_count IS NOT NULL)
      OR (TRY_CAST(total_amount AS DOUBLE) IS NULL AND total_amount  IS NOT NULL)
      OR vendor_id IS NULL
      OR tpep_pickup_datetime IS NULL
      OR tpep_dropoff_datetime IS NULL
      OR passenger_count IS NULL
      OR total_amount IS NULL
      OR (TRY_CAST(vendor_id AS INTEGER) NOT BETWEEN 1 AND 4)
      OR (TRY_CAST(passenger_count AS INTEGER) < 0 OR TRY_CAST(passenger_count AS INTEGER) > 9)
      OR (TRY_CAST(total_amount AS DOUBLE) <= 0 OR TRY_CAST(total_amount AS DOUBLE) > 1000)
      OR (TRY_CAST(tpep_pickup_datetime AS TIMESTAMP) IS NOT NULL AND TRY_CAST(tpep_dropoff_datetime AS TIMESTAMP) IS NOT NULL AND TRY_CAST(tpep_dropoff_datetime AS TIMESTAMP) <= TRY_CAST(tpep_pickup_datetime AS TIMESTAMP))
      OR (TRY_CAST(tpep_pickup_datetime AS TIMESTAMP) IS NOT NULL AND TRY_CAST(tpep_dropoff_datetime AS TIMESTAMP) IS NOT NULL AND (CAST(TRY_CAST(tpep_dropoff_datetime AS TIMESTAMP) AS LONG) - CAST(TRY_CAST(tpep_pickup_datetime AS TIMESTAMP) AS LONG)) > 86400)
      OR (TRY_CAST(tpep_pickup_datetime AS TIMESTAMP) IS NOT NULL AND TRY_CAST(tpep_dropoff_datetime AS TIMESTAMP) IS NOT NULL AND (CAST(TRY_CAST(tpep_dropoff_datetime AS TIMESTAMP) AS LONG) - CAST(TRY_CAST(tpep_pickup_datetime AS TIMESTAMP) AS LONG)) < 60)
      OR (TRY_CAST(tpep_pickup_datetime AS TIMESTAMP) IS NOT NULL AND TRY_CAST(tpep_pickup_datetime AS TIMESTAMP) > current_timestamp())
      OR (TRY_CAST(tpep_pickup_datetime AS TIMESTAMP) IS NOT NULL AND TRY_CAST(tpep_pickup_datetime AS TIMESTAMP) < date_sub(current_date(), 3650))
    THEN 1 
    ELSE 0 
  END AS tem_erro
FROM ifood_catalog.trusted_layer.tmp_tb_taxi_data_for_analysis;

-- COMMAND ----------

-- 3. Criar métricas detalhadas de qualidade
CREATE OR REPLACE TEMPORARY VIEW vw_metricas_qualidade AS
SELECT
  COUNT(*) as total_registros,
  SUM(tem_erro) as total_erros,
  COUNT(*) - SUM(tem_erro) as total_validos,
  
  -- Métricas de Tipagem
  SUM(falha_tipo_vendor_id) as qtd_erro_tipo_vendor_id,
  SUM(falha_tipo_pickup_datetime) as qtd_erro_tipo_pickup,
  SUM(falha_tipo_dropoff_datetime) as qtd_erro_tipo_dropoff,
  SUM(falha_tipo_passenger_count) as qtd_erro_tipo_passenger,
  SUM(falha_tipo_total_amount) as qtd_erro_tipo_amount,
  
  -- Métricas de Nulos
  SUM(falha_vendor_id_nulo) as qtd_vendor_id_nulo,
  SUM(falha_pickup_nulo) as qtd_pickup_nulo,
  SUM(falha_dropoff_nulo) as qtd_dropoff_nulo,
  SUM(falha_passenger_count_nulo) as qtd_passenger_nulo,
  SUM(falha_total_amount_nulo) as qtd_amount_nulo,
  
  -- Métricas de Valores Inválidos
  SUM(falha_vendor_id_invalido) as qtd_vendor_id_invalido,
  SUM(falha_passenger_count_invalido) as qtd_passenger_invalido,
  
  -- Métricas de Lógica de Negócio
  SUM(falha_dropoff_antes_pickup) as qtd_dropoff_antes_pickup,
  SUM(falha_duracao_excessiva) as qtd_duracao_excessiva,
  SUM(falha_duracao_muito_curta) as qtd_duracao_muito_curta,
  SUM(falha_pickup_futuro) as qtd_pickup_futuro,
  SUM(falha_pickup_muito_antigo) as qtd_pickup_muito_antigo,
  
  -- Percentuais
  ROUND(100.0 * SUM(tem_erro) / COUNT(*), 2) as percentual_erro,
  ROUND(100.0 * (COUNT(*) - SUM(tem_erro)) / COUNT(*), 2) as percentual_validos,
  ROUND(100.0 * SUM(falha_tipo_vendor_id + falha_tipo_pickup_datetime + 
                     falha_tipo_dropoff_datetime + falha_tipo_passenger_count + 
                     falha_tipo_total_amount) / COUNT(*), 2) as percentual_erro_tipagem
FROM vw_taxi_validado;

-- COMMAND ----------

-- 4. Exibir RELATÓRIO DETALHADO de validação
SELECT '========================================' as separador;
SELECT '📊 RESUMO GERAL' as secao;
SELECT '========================================' as separador;

SELECT 
  total_registros as total_processado,
  total_validos as aprovados,
  total_erros as reprovados,
  CONCAT(percentual_validos, '%') as taxa_aprovacao,
  CONCAT(percentual_erro, '%') as taxa_erro
FROM vw_metricas_qualidade;

SELECT '========================================' as separador;
SELECT '🔍 ERROS DE TIPAGEM' as secao;
SELECT '========================================' as separador;

SELECT 
  qtd_erro_tipo_vendor_id as vendor_id_tipo_invalido,
  qtd_erro_tipo_pickup as pickup_tipo_invalido,
  qtd_erro_tipo_dropoff as dropoff_tipo_invalido,
  qtd_erro_tipo_passenger as passenger_count_tipo_invalido,
  qtd_erro_tipo_amount as total_amount_tipo_invalido,
  CONCAT(percentual_erro_tipagem, '%') as percentual_erro_tipo
FROM vw_metricas_qualidade;

SELECT '========================================' as separador;
SELECT '❌ CAMPOS NULOS' as secao;
SELECT '========================================' as separador;

SELECT 
  qtd_vendor_id_nulo,
  qtd_pickup_nulo,
  qtd_dropoff_nulo,
  qtd_passenger_nulo,
  qtd_amount_nulo
FROM vw_metricas_qualidade;

SELECT '========================================' as separador;
SELECT '⚠️ VALORES INVÁLIDOS' as secao;
SELECT '========================================' as separador;

SELECT 
  qtd_vendor_id_invalido as vendor_fora_range,
  qtd_passenger_invalido as passenger_fora_range
FROM vw_metricas_qualidade;

SELECT '========================================' as separador;
SELECT '🚨 ERROS DE LÓGICA DE NEGÓCIO' as secao;
SELECT '========================================' as separador;

SELECT 
  qtd_dropoff_antes_pickup as dropoff_antes_pickup,
  qtd_duracao_excessiva as viagens_muito_longas,
  qtd_duracao_muito_curta as viagens_muito_curtas,
  qtd_pickup_futuro as datas_futuras,
  qtd_pickup_muito_antigo as datas_muito_antigas
FROM vw_metricas_qualidade;

-- COMMAND ----------

-- -- 5. Definir thresholds aceitáveis
-- Não foi possível definir as variáveis, assunto para estudar porque o SET VAR não funciona, talvez por ser serverless
-- SET VAR threshold_percentual_erro = 2.0;  -- Max 2% de erro permitido
-- SET VAR threshold_tipagem = 0.1;           -- Max 0.1% de erro de tipo

-- SET VAR percentual_erro_atual = (SELECT percentual_erro FROM vw_metricas_qualidade);
-- SET VAR percentual_tipagem = (SELECT percentual_erro_tipagem FROM vw_metricas_qualidade);
-- SET VAR total_erros_atual = (SELECT total_erros FROM vw_metricas_qualidade);

-- 6. VALIDAÇÕES CRÍTICAS - Pipeline para se os thresholds forem excedidos
SELECT '========================================' as separador;
SELECT '✅ EXECUTANDO VALIDAÇÕES CRÍTICAS' as secao;
SELECT '========================================' as separador;

SELECT ASSERT_TRUE(
  (SELECT percentual_erro FROM vw_metricas_qualidade) <= 2.0,
  CONCAT(
    '❌ FALHA GERAL: ', 
    (SELECT total_erros FROM vw_metricas_qualidade), 
    ' registros com erro (',
    (SELECT percentual_erro FROM vw_metricas_qualidade),
    '% > ',
    2.0,
    '% permitido)'
  )
) as validacao_geral;

-- COMMAND ----------

SELECT ASSERT_TRUE(
  (SELECT percentual_erro_tipagem FROM vw_metricas_qualidade) <= 0.1,
  CONCAT(
    '❌ FALHA DE TIPAGEM CRÍTICA: ',
    (SELECT percentual_erro_tipagem FROM vw_metricas_qualidade),
    '% de erros de tipo (> ',
    0.1,
    '% permitido)'
  )
) as validacao_tipagem;

-- COMMAND ----------

SELECT '✅ Todas as validações críticas passaram!' as resultado;

-- COMMAND ----------

-- 7. Inserir dados VÁLIDOS na camada TRUSTED
MERGE WITH SCHEMA EVOLUTION INTO IDENTIFIER(:catalogo || '.' || :schema || '.' || :table)
USING (
  SELECT 
    CAST(vendor_id AS INTEGER) as vendor_id,
    CAST(tpep_pickup_datetime AS TIMESTAMP) as tpep_pickup_datetime,
    CAST(tpep_dropoff_datetime AS TIMESTAMP) as tpep_dropoff_datetime,
    CAST(passenger_count AS INTEGER) as passenger_count,
    CAST(total_amount AS DOUBLE) as total_amount
  FROM vw_taxi_validado
  WHERE tem_erro = 0
) AS tb_taxi_data_for_analysis_tmp
ON tb_taxi_data_for_analysis.vendor_id = tb_taxi_data_for_analysis_tmp.vendor_id
AND tb_taxi_data_for_analysis.tpep_pickup_datetime = tb_taxi_data_for_analysis_tmp.tpep_pickup_datetime
AND tb_taxi_data_for_analysis.tpep_dropoff_datetime = tb_taxi_data_for_analysis_tmp.tpep_dropoff_datetime
AND tb_taxi_data_for_analysis.passenger_count = tb_taxi_data_for_analysis_tmp.passenger_count
AND tb_taxi_data_for_analysis.total_amount = tb_taxi_data_for_analysis_tmp.total_amount
WHEN NOT MATCHED
    THEN INSERT *
WHEN NOT MATCHED BY SOURCE THEN DELETE

-- COMMAND ----------


-- 8. Registrar erros na tabela de quarentena

MERGE WITH SCHEMA EVOLUTION INTO IDENTIFIER(:catalogo || '.' || :schema || '.' || :table_quarentena)
USING (
  SELECT 
    *,
    current_timestamp() as data_hora_quarentena,
    
    -- Lista detalhada de erros
    CONCAT_WS(' | ',
      IF(falha_tipo_vendor_id = 1, 'TIPO_VENDOR_ID_INVALIDO', NULL),
      IF(falha_tipo_pickup_datetime = 1, 'TIPO_PICKUP_INVALIDO', NULL),
      IF(falha_tipo_dropoff_datetime = 1, 'TIPO_DROPOFF_INVALIDO', NULL),
      IF(falha_tipo_passenger_count = 1, 'TIPO_PASSENGER_INVALIDO', NULL),
      IF(falha_tipo_total_amount = 1, 'TIPO_AMOUNT_INVALIDO', NULL),
      IF(falha_vendor_id_nulo = 1, 'VENDOR_ID_NULO', NULL),
      IF(falha_pickup_nulo = 1, 'PICKUP_NULO', NULL),
      IF(falha_dropoff_nulo = 1, 'DROPOFF_NULO', NULL),
      IF(falha_passenger_count_nulo = 1, 'PASSENGER_COUNT_NULO', NULL),
      IF(falha_total_amount_nulo = 1, 'TOTAL_AMOUNT_NULO', NULL),
      IF(falha_vendor_id_invalido = 1, 'VENDOR_ID_FORA_RANGE', NULL),
      IF(falha_passenger_count_invalido = 1, 'PASSENGER_COUNT_FORA_RANGE', NULL),
      IF(falha_dropoff_antes_pickup = 1, 'DROPOFF_ANTES_PICKUP', NULL),
      IF(falha_duracao_excessiva = 1, 'DURACAO_EXCESSIVA', NULL),
      IF(falha_duracao_muito_curta = 1, 'DURACAO_MUITO_CURTA', NULL),
      IF(falha_pickup_futuro = 1, 'PICKUP_DATA_FUTURA', NULL),
      IF(falha_pickup_muito_antigo = 1, 'PICKUP_MUITO_ANTIGO', NULL)
    ) as motivos_erro,
    
    -- Classificação do erro
    CASE 
      WHEN falha_tipo_vendor_id + falha_tipo_pickup_datetime + 
          falha_tipo_dropoff_datetime + falha_tipo_passenger_count + 
          falha_tipo_total_amount > 0 
      THEN 'ERRO_TIPAGEM'
      WHEN falha_vendor_id_nulo + falha_pickup_nulo + falha_dropoff_nulo + 
          falha_passenger_count_nulo + falha_total_amount_nulo > 0
      THEN 'ERRO_NULO'
      WHEN falha_dropoff_antes_pickup + falha_duracao_excessiva + 
          falha_duracao_muito_curta > 0
      THEN 'ERRO_LOGICA_NEGOCIO'
      ELSE 'ERRO_VALIDACAO_RANGE'
    END as tipo_erro,
    
    current_date() as data_particao_quarentena
  FROM vw_taxi_validado
  WHERE tem_erro = 1
) AS tb_quarentena_tmp
ON tb_taxi_data_quarentena.vendor_id = tb_quarentena_tmp.vendor_id
AND tb_taxi_data_quarentena.tpep_pickup_datetime = tb_quarentena_tmp.tpep_pickup_datetime
AND tb_taxi_data_quarentena.tpep_dropoff_datetime = tb_quarentena_tmp.tpep_dropoff_datetime
AND tb_taxi_data_quarentena.passenger_count = tb_quarentena_tmp.passenger_count
AND tb_taxi_data_quarentena.total_amount = tb_quarentena_tmp.total_amount
AND tb_taxi_data_quarentena.date_reference = tb_quarentena_tmp.date_reference
AND tb_taxi_data_quarentena.insertion_date = tb_quarentena_tmp.insertion_date
AND tb_taxi_data_quarentena.falha_tipo_vendor_id = tb_quarentena_tmp.falha_tipo_vendor_id
AND tb_taxi_data_quarentena.falha_tipo_pickup_datetime = tb_quarentena_tmp.falha_tipo_pickup_datetime
AND tb_taxi_data_quarentena.falha_tipo_dropoff_datetime = tb_quarentena_tmp.falha_tipo_dropoff_datetime
AND tb_taxi_data_quarentena.falha_tipo_passenger_count = tb_quarentena_tmp.falha_tipo_passenger_count
AND tb_taxi_data_quarentena.falha_tipo_total_amount = tb_quarentena_tmp.falha_tipo_total_amount
AND tb_taxi_data_quarentena.falha_vendor_id_nulo = tb_quarentena_tmp.falha_vendor_id_nulo
AND tb_taxi_data_quarentena.falha_pickup_nulo = tb_quarentena_tmp.falha_pickup_nulo
AND tb_taxi_data_quarentena.falha_dropoff_nulo = tb_quarentena_tmp.falha_dropoff_nulo
AND tb_taxi_data_quarentena.falha_passenger_count_nulo = tb_quarentena_tmp.falha_passenger_count_nulo
AND tb_taxi_data_quarentena.falha_total_amount_nulo = tb_quarentena_tmp.falha_total_amount_nulo
AND tb_taxi_data_quarentena.falha_vendor_id_invalido = tb_quarentena_tmp.falha_vendor_id_invalido
AND tb_taxi_data_quarentena.falha_passenger_count_invalido = tb_quarentena_tmp.falha_passenger_count_invalido
AND tb_taxi_data_quarentena.falha_dropoff_antes_pickup = tb_quarentena_tmp.falha_dropoff_antes_pickup
AND tb_taxi_data_quarentena.falha_duracao_excessiva = tb_quarentena_tmp.falha_duracao_excessiva
AND tb_taxi_data_quarentena.falha_duracao_muito_curta = tb_quarentena_tmp.falha_duracao_muito_curta
AND tb_taxi_data_quarentena.falha_pickup_futuro = tb_quarentena_tmp.falha_pickup_futuro
AND tb_taxi_data_quarentena.falha_pickup_muito_antigo = tb_quarentena_tmp.falha_pickup_muito_antigo
AND tb_taxi_data_quarentena.tem_erro = tb_quarentena_tmp.tem_erro
AND tb_taxi_data_quarentena.data_hora_quarentena = tb_quarentena_tmp.data_hora_quarentena
AND tb_taxi_data_quarentena.motivos_erro = tb_quarentena_tmp.motivos_erro
AND tb_taxi_data_quarentena.tipo_erro = tb_quarentena_tmp.tipo_erro
AND tb_taxi_data_quarentena.data_particao_quarentena = tb_quarentena_tmp.data_particao_quarentena
WHEN NOT MATCHED
    THEN INSERT *
WHEN NOT MATCHED BY SOURCE THEN DELETE;

-- COMMAND ----------

-- 9. Resumo final da execução
SELECT '========================================' as separador;
SELECT '🎉 PIPELINE CONCLUÍDO COM SUCESSO!' as secao;
SELECT '========================================' as separador;

SELECT 
  -- (SELECT COUNT(*) FROM `ifood_catalog`.trusted_layer.tb_taxi_data 
  --  WHERE DATE(data_hora_carga) = current_date()) as registros_inseridos_trusted,
  (SELECT COUNT(*) FROM `ifood_catalog`.trusted_layer.tb_taxi_data_quarentena 
   WHERE data_particao_quarentena = current_date()) as registros_quarentena,
  (SELECT COUNT(*) FROM `ifood_catalog`.trusted_layer.tb_taxi_data_quarentena 
   WHERE data_particao_quarentena = current_date() AND tipo_erro = 'ERRO_TIPAGEM') as erros_tipagem,
  (SELECT COUNT(*) FROM `ifood_catalog`.trusted_layer.tb_taxi_data_quarentena 
   WHERE data_particao_quarentena = current_date() AND tipo_erro = 'ERRO_LOGICA_NEGOCIO') as erros_logica_negocio;

-- COMMAND ----------

-- 10. Limpar a tabela intermediária
TRUNCATE TABLE IDENTIFIER(:catalogo || '.' || :schema || '.tmp_' || :table);
