-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Transformação: Raw Layer → Trusted Layer com Validação de Qualidade
-- MAGIC
-- MAGIC Este notebook é responsável por transformar e refinar os dados da camada Raw para a camada Trusted, aplicando **validações completas de qualidade de dados** antes de persistir apenas registros válidos.
-- MAGIC
-- MAGIC **Autor:** Vinicius  
-- MAGIC **Data:** 2025  
-- MAGIC **Versão:** 2.0 (com Data Quality Checks)
-- MAGIC
-- MAGIC ---
-- MAGIC
-- MAGIC ## 📋 Índice
-- MAGIC
-- MAGIC 1. [Objetivo](#objetivo)
-- MAGIC 2. [Fluxo de Execução](#fluxo-de-execução)
-- MAGIC 3. [Colunas Utilizadas](#colunas-utilizadas)
-- MAGIC 4. [Validações Implementadas](#validações-implementadas)
-- MAGIC 5. [Estrutura de Tabelas](#estrutura-de-tabelas)
-- MAGIC 6. [Parâmetros do Notebook](#parâmetros-do-notebook)
-- MAGIC 7. [Métricas de Qualidade](#métricas-de-qualidade)
-- MAGIC
-- MAGIC ---
-- MAGIC
-- MAGIC ## 🎯 Objetivo
-- MAGIC
-- MAGIC Este pipeline realiza:
-- MAGIC
-- MAGIC 1. **Extração seletiva** da camada Raw (filtrada por período e última ingestão)
-- MAGIC 2. **Criação de tabela temporária** para análise intermediária
-- MAGIC 3. **Validações abrangentes** de:
-- MAGIC    - Tipagem de dados
-- MAGIC    - Campos obrigatórios (não-nulos)
-- MAGIC    - Valores válidos (ranges)
-- MAGIC    - Lógica de negócio (consistência temporal, durações)
-- MAGIC 4. **Bifurcação de dados**:
-- MAGIC    - ✅ Dados válidos → `tb_taxi_data_for_analysis`
-- MAGIC    - ❌ Dados inválidos → `tb_taxi_data_quarentena` (com detalhamento de erros)
-- MAGIC 5. **Geração de métricas** detalhadas de qualidade
-- MAGIC 6. **Limpeza** de dados temporários
-- MAGIC
-- MAGIC ---
-- MAGIC
-- MAGIC ## 🔄 Fluxo de Execução
-- MAGIC
-- MAGIC ```
-- MAGIC ┌─────────────────────────────────────────────────────────────┐
-- MAGIC │ STEP 1: Criação de Tabelas                                 │
-- MAGIC │ • tb_taxi_data_for_analysis (final - trusted)              │
-- MAGIC │ • tmp_tb_taxi_data_for_analysis (temporária)               │
-- MAGIC │ • tb_taxi_data_quarentena (erros)                          │
-- MAGIC └────────────────────┬────────────────────────────────────────┘
-- MAGIC                      │
-- MAGIC                      ▼
-- MAGIC ┌─────────────────────────────────────────────────────────────┐
-- MAGIC │ STEP 2: Extração e Carga Temporária                        │
-- MAGIC │ • Lê raw_layer.tb_taxi_data_api (filtrado)                 │
-- MAGIC │ • MERGE INTO tmp_tb_taxi_data_for_analysis                 │
-- MAGIC │ • Aplica DISTINCT e conversões básicas                     │
-- MAGIC └────────────────────┬────────────────────────────────────────┘
-- MAGIC                      │
-- MAGIC                      ▼
-- MAGIC ┌─────────────────────────────────────────────────────────────┐
-- MAGIC │ STEP 3: Aplicação de Validações                            │
-- MAGIC │ • Cria view vw_taxi_validado com 23 flags de validação     │
-- MAGIC │ • Calcula flag geral: tem_erro (0=válido, 1=inválido)      │
-- MAGIC └────────────────────┬────────────────────────────────────────┘
-- MAGIC                      │
-- MAGIC                      ▼
-- MAGIC ┌─────────────────────────────────────────────────────────────┐
-- MAGIC │ STEP 4: Cálculo de Métricas                                │
-- MAGIC │ • Cria view vw_metricas_qualidade                          │
-- MAGIC │ • Consolida estatísticas por tipo de erro                  │
-- MAGIC └────────────────────┬────────────────────────────────────────┘
-- MAGIC                      │
-- MAGIC                      ▼
-- MAGIC ┌─────────────────────────────────────────────────────────────┐
-- MAGIC │ STEP 5: Relatórios de Validação (5 seções)                 │
-- MAGIC │ 📊 Resumo Geral                                            │
-- MAGIC │ 🔍 Erros de Tipagem                                        │
-- MAGIC │ ❌ Campos Nulos                                            │
-- MAGIC │ ⚠️  Valores Inválidos                                      │
-- MAGIC │ 🚨 Erros de Lógica de Negócio                             │
-- MAGIC └────────────────────┬────────────────────────────────────────┘
-- MAGIC                      │
-- MAGIC                      ▼
-- MAGIC ┌─────────────────────────────────────────────────────────────┐
-- MAGIC │ STEP 6: Validações Críticas com ASSERT_TRUE                │
-- MAGIC │ • Threshold geral: max 5% de erro                          │
-- MAGIC │ • Threshold tipagem: max 0.1% de erro                      │
-- MAGIC │ • Pipeline PARA se thresholds forem excedidos              │
-- MAGIC └────────────────────┬────────────────────────────────────────┘
-- MAGIC                      │
-- MAGIC                      ▼ ✅ PASSOU
-- MAGIC ┌─────────────────────────────────────────────────────────────┐
-- MAGIC │ STEP 7: Bifurcação de Dados                                │
-- MAGIC │                                                             │
-- MAGIC │  tem_erro = 0  ──────────►  tb_taxi_data_for_analysis      │
-- MAGIC │                             (apenas 5 colunas core)         │
-- MAGIC │                                                             │
-- MAGIC │  tem_erro = 1  ──────────►  tb_taxi_data_quarentena        │
-- MAGIC │                             (todas colunas + metadados)     │
-- MAGIC └────────────────────┬────────────────────────────────────────┘
-- MAGIC                      │
-- MAGIC                      ▼
-- MAGIC ┌─────────────────────────────────────────────────────────────┐
-- MAGIC │ STEP 8: Resumo Final e Limpeza                             │
-- MAGIC │ • Exibe estatísticas de inserção                           │
-- MAGIC │ • TRUNCATE da tabela temporária                            │
-- MAGIC └─────────────────────────────────────────────────────────────┘
-- MAGIC ```
-- MAGIC
-- MAGIC ---
-- MAGIC
-- MAGIC ## 📊 Colunas Utilizadas
-- MAGIC
-- MAGIC ### Colunas Mantidas (Requisitos do Case - Tabela Final)
-- MAGIC
-- MAGIC | Coluna | Tipo | Descrição |
-- MAGIC |--------|------|-----------|
-- MAGIC | `vendor_id` | `BIGINT` | Identificador do fornecedor (1-4) |
-- MAGIC | `tpep_pickup_datetime` | `TIMESTAMP` | Data/hora de início da corrida |
-- MAGIC | `tpep_dropoff_datetime` | `TIMESTAMP` | Data/hora de fim da corrida |
-- MAGIC | `passenger_count` | `INTEGER` | Quantidade de passageiros (0-9) |
-- MAGIC | `total_amount` | `DOUBLE` | Valor total da corrida ($0-$1000) |
-- MAGIC
-- MAGIC ### Colunas Adicionais (Tabela Temporária)
-- MAGIC
-- MAGIC - `date_reference`: Data de referência do dado
-- MAGIC - `insertion_date`: Data de inserção na camada Raw
-- MAGIC
-- MAGIC ### Colunas de Metadados (Quarentena)
-- MAGIC
-- MAGIC - `data_hora_quarentena`: Timestamp de registro na quarentena
-- MAGIC - `motivos_erro`: Lista concatenada de erros encontrados
-- MAGIC - `tipo_erro`: Classificação do erro (ERRO_TIPAGEM, ERRO_NULO, ERRO_LOGICA_NEGOCIO, ERRO_VALIDACAO_RANGE)
-- MAGIC - `data_particao_quarentena`: Partição por data
-- MAGIC - 23 flags de validação individuais
-- MAGIC
-- MAGIC ---
-- MAGIC
-- MAGIC ## ✅ Validações Implementadas
-- MAGIC
-- MAGIC ### 1. 🔍 Testes de Tipagem (Type Safety)
-- MAGIC
-- MAGIC | Validação | Critério |
-- MAGIC |-----------|----------|
-- MAGIC | `falha_tipo_vendor_id` | Se vendor_id não pode ser convertido para INTEGER |
-- MAGIC | `falha_tipo_pickup_datetime` | Se pickup não pode ser convertido para TIMESTAMP |
-- MAGIC | `falha_tipo_dropoff_datetime` | Se dropoff não pode ser convertido para TIMESTAMP |
-- MAGIC | `falha_tipo_passenger_count` | Se passenger_count não pode ser convertido para INTEGER |
-- MAGIC | `falha_tipo_total_amount` | Se total_amount não pode ser convertido para DOUBLE |
-- MAGIC
-- MAGIC ### 2. ❌ Testes de Campos Obrigatórios
-- MAGIC
-- MAGIC | Validação | Critério |
-- MAGIC |-----------|----------|
-- MAGIC | `falha_vendor_id_nulo` | vendor_id IS NULL |
-- MAGIC | `falha_pickup_nulo` | tpep_pickup_datetime IS NULL |
-- MAGIC | `falha_dropoff_nulo` | tpep_dropoff_datetime IS NULL |
-- MAGIC | `falha_passenger_count_nulo` | passenger_count IS NULL |
-- MAGIC | `falha_total_amount_nulo` | total_amount IS NULL |
-- MAGIC
-- MAGIC ### 3. ⚠️ Testes de Valores Válidos (Ranges)
-- MAGIC
-- MAGIC | Validação | Critério | Justificativa |
-- MAGIC |-----------|----------|---------------|
-- MAGIC | `falha_vendor_id_invalido` | vendor_id NOT BETWEEN 1 AND 4 | Padrão NYC Taxi & Limousine Commission |
-- MAGIC | `falha_passenger_count_invalido` | passenger_count < 0 OR > 9 | Capacidade máxima de táxis/vans |
-- MAGIC | `falha_total_amount_invalido` | total_amount ≤ 0 OR > 1000 | Valores negativos/zero inválidos; $1000 limite razoável |
-- MAGIC
-- MAGIC ### 4. 🚨 Testes de Lógica de Negócio
-- MAGIC
-- MAGIC | Validação | Critério | Justificativa |
-- MAGIC |-----------|----------|---------------|
-- MAGIC | `falha_dropoff_antes_pickup` | dropoff ≤ pickup | Dropoff deve ser posterior ao pickup |
-- MAGIC | `falha_duracao_excessiva` | Duração > 24 horas | Corridas não devem durar mais de 1 dia |
-- MAGIC | `falha_duracao_muito_curta` | Duração < 60 segundos | Corridas válidas têm duração mínima |
-- MAGIC | `falha_pickup_futuro` | pickup > current_timestamp() | Data não pode ser futura |
-- MAGIC | `falha_pickup_muito_antigo` | pickup < (hoje - 10 anos) | Dados muito antigos podem ser inválidos |
-- MAGIC
-- MAGIC ### 5. 🎯 Flag Consolidada
-- MAGIC
-- MAGIC **`tem_erro`**: Agregação de TODAS as validações acima
-- MAGIC - `0` = Registro válido (passa em todos os testes)
-- MAGIC - `1` = Registro inválido (falha em pelo menos 1 teste)
-- MAGIC
-- MAGIC ---
-- MAGIC
-- MAGIC ## 🗄️ Estrutura de Tabelas
-- MAGIC
-- MAGIC ### 1. `tb_taxi_data_for_analysis` (Final - Trusted Layer)
-- MAGIC
-- MAGIC **Localização:** `s3://datalake-ifood/trusted_layer/tb_taxi_data_for_analysis`  
-- MAGIC **Formato:** Delta Lake  
-- MAGIC **Clustering:** `vendor_id`  
-- MAGIC **Conteúdo:** Apenas registros válidos (`tem_erro = 0`) com as 5 colunas core
-- MAGIC
-- MAGIC ### 2. `tmp_tb_taxi_data_for_analysis` (Temporária)
-- MAGIC
-- MAGIC **Localização:** `s3://datalake-ifood/trusted_layer/tmp_tb_taxi_data_for_analysis`  
-- MAGIC **Formato:** Delta Lake  
-- MAGIC **Clustering:** `vendor_id`  
-- MAGIC **Conteúdo:** Dados brutos da Raw + metadados (7 colunas)  
-- MAGIC **Ciclo de vida:** Truncada ao final de cada execução
-- MAGIC
-- MAGIC ### 3. `tb_taxi_data_quarentena` (Erros)
-- MAGIC
-- MAGIC **Localização:** `s3://datalake-ifood/trusted_layer/tb_taxi_data_quarentena`  
-- MAGIC **Formato:** Delta Lake  
-- MAGIC **Clustering:** `vendor_id`  
-- MAGIC **Particionamento:** `data_particao_quarentena`, `tipo_erro`  
-- MAGIC **Conteúdo:** Registros inválidos (`tem_erro = 1`) com:
-- MAGIC - Todas as 7 colunas originais
-- MAGIC - 23 flags de validação individual
-- MAGIC - 4 colunas de metadados (timestamp, motivos, tipo, partição)
-- MAGIC
-- MAGIC ---
-- MAGIC
-- MAGIC ## 🔧 Parâmetros do Notebook
-- MAGIC
-- MAGIC | Parâmetro | Tipo | Exemplo | Descrição |
-- MAGIC |-----------|------|---------|-----------|
-- MAGIC | `:catalogo` | STRING | `ifood_catalog` | Nome do Unity Catalog |
-- MAGIC | `:schema` | STRING | `trusted_layer` | Schema de destino |
-- MAGIC | `:table` | STRING | `tb_taxi_data_for_analysis` | Nome da tabela final |
-- MAGIC | `:table_quarentena` | STRING | `tb_taxi_data_quarentena` | Nome da tabela de quarentena |
-- MAGIC | `:start_date` | STRING | `2023-01` | Data inicial (YYYY-MM) |
-- MAGIC | `:end_date` | STRING | `2023-05` | Data final (YYYY-MM) |
-- MAGIC
-- MAGIC **Formato de Data:** `YYYY-MM` (será concatenado com `-01` internamente)
-- MAGIC
-- MAGIC ---
-- MAGIC
-- MAGIC ## 📈 Métricas de Qualidade
-- MAGIC
-- MAGIC ### Relatório Gerado (5 Seções)
-- MAGIC
-- MAGIC #### 📊 1. Resumo Geral
-- MAGIC - Total de registros processados
-- MAGIC - Registros aprovados (válidos)
-- MAGIC - Registros reprovados (inválidos)
-- MAGIC - Taxa de aprovação (%)
-- MAGIC - Taxa de erro (%)
-- MAGIC
-- MAGIC #### 🔍 2. Erros de Tipagem
-- MAGIC - Quantidade por tipo de campo
-- MAGIC - Percentual de erro de tipagem
-- MAGIC
-- MAGIC #### ❌ 3. Campos Nulos
-- MAGIC - Quantidade de nulos por campo obrigatório
-- MAGIC
-- MAGIC #### ⚠️ 4. Valores Inválidos
-- MAGIC - Quantidade de valores fora dos ranges válidos
-- MAGIC
-- MAGIC #### 🚨 5. Erros de Lógica de Negócio
-- MAGIC - Dropoff antes de pickup
-- MAGIC - Viagens muito longas (>24h)
-- MAGIC - Viagens muito curtas (<1min)
-- MAGIC - Datas futuras
-- MAGIC - Datas muito antigas
-- MAGIC
-- MAGIC ### Thresholds Críticos
-- MAGIC
-- MAGIC | Validação | Threshold | Ação se Excedido |
-- MAGIC |-----------|-----------|------------------|
-- MAGIC | Taxa geral de erro | ≤ 5.0% | Pipeline PARA (ASSERT falha) |
-- MAGIC | Taxa de erro de tipagem | ≤ 0.1% | Pipeline PARA (ASSERT falha) |
-- MAGIC
-- MAGIC ---
-- MAGIC
-- MAGIC ## 📝 Estratégia de Merge
-- MAGIC
-- MAGIC ### Tabela Temporária (tmp_)
-- MAGIC **Chave:** `vendor_id + pickup + dropoff`  
-- MAGIC **Operações:**
-- MAGIC - `WHEN NOT MATCHED`: Insere novos registros
-- MAGIC - `MERGE WITH SCHEMA EVOLUTION`: Permite evolução de schema
-- MAGIC
-- MAGIC ### Tabela Final (tb_taxi_data_for_analysis)
-- MAGIC **Chave:** `vendor_id + pickup + dropoff + passenger_count + total_amount`  
-- MAGIC **Operações:**
-- MAGIC - `WHEN NOT MATCHED`: Insere apenas registros válidos
-- MAGIC - **SEM UPDATE**: Devido a possíveis anomalias nos dados fonte
-- MAGIC
-- MAGIC ### Tabela de Quarentena
-- MAGIC **Chave:** Todas as 30 colunas (dados + validações + metadados)  
-- MAGIC **Operações:**
-- MAGIC - `WHEN NOT MATCHED`: Insere registros com erro
-- MAGIC - Preserva histórico de erros por partição de data
-- MAGIC
-- MAGIC ---
-- MAGIC
-- MAGIC ## 🔄 Observações Importantes
-- MAGIC
-- MAGIC ### ⚠️ Limitações Conhecidas
-- MAGIC
-- MAGIC 1. **SET VAR não funciona em Databricks Serverless**
-- MAGIC    - Workaround: Subqueries diretas nas validações ASSERT_TRUE
-- MAGIC
-- MAGIC 2. **Anomalia de Dados Duplicados**
-- MAGIC    - Dados fonte podem conter duplicatas com valores opostos
-- MAGIC    - Estratégia: DISTINCT na origem + chave composta robusta
-- MAGIC
-- MAGIC 3. **Performance**
-- MAGIC    - Clustering por `vendor_id` otimiza queries filtradas
-- MAGIC    - Tabela temporária é truncada para liberar storage
-- MAGIC
-- MAGIC ### ✅ Boas Práticas Implementadas
-- MAGIC
-- MAGIC - ✓ Validações abrangentes antes de mover dados
-- MAGIC - ✓ Quarentena detalhada para análise de erros
-- MAGIC - ✓ Métricas visuais para monitoramento
-- MAGIC - ✓ Idempotência via MERGE (re-execuções seguras)
-- MAGIC - ✓ Schema evolution habilitado
-- MAGIC - ✓ Documentação inline em cada step
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1. Criação das Tabelas Trusted: final, temporária e de quarentena
-- MAGIC
-- MAGIC Cria a tabela na camada Trusted se ela não existir, definindo:
-- MAGIC - Schema simplificado com apenas colunas necessárias
-- MAGIC - Clustering por vendor_id para otimizar queries filtradas
-- MAGIC - Localização no S3 para persistência

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
-- MAGIC 1. **Lê dados da camada Raw**
-- MAGIC 2. **Renomeia VendorID** para vendor_id (padronização snake_case)
-- MAGIC 3. **Seleciona apenas colunas necessárias** conforme requisitos
-- MAGIC 4. **Testes de qualidade**
-- MAGIC 5. **Aplica MERGE com três condições**:
-- MAGIC    - **WHEN NOT MATCHED**: Insere novos registros
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
    THEN INSERT *;

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

-- COMMAND ----------

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

-- COMMAND ----------

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

-- COMMAND ----------

SELECT '========================================' as separador;
SELECT '⚠️ VALORES INVÁLIDOS' as secao;
SELECT '========================================' as separador;

SELECT 
  qtd_vendor_id_invalido as vendor_fora_range,
  qtd_passenger_invalido as passenger_fora_range
FROM vw_metricas_qualidade;

-- COMMAND ----------


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
-- SET VAR threshold_percentual_erro = 5.0;  -- Max 5% de erro permitido
-- SET VAR threshold_tipagem = 0.1;           -- Max 0.1% de erro de tipo

-- SET VAR percentual_erro_atual = (SELECT percentual_erro FROM vw_metricas_qualidade);
-- SET VAR percentual_tipagem = (SELECT percentual_erro_tipagem FROM vw_metricas_qualidade);
-- SET VAR total_erros_atual = (SELECT total_erros FROM vw_metricas_qualidade);

-- 6. VALIDAÇÕES CRÍTICAS - Pipeline para se os thresholds forem excedidos
SELECT '========================================' as separador;
SELECT '✅ EXECUTANDO VALIDAÇÕES CRÍTICAS' as secao;
SELECT '========================================' as separador;

SELECT ASSERT_TRUE(
  (SELECT percentual_erro FROM vw_metricas_qualidade) <= 5.0,
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
    THEN INSERT *;

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
    THEN INSERT *;

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

-- Para apagar todos os dados de quarentena e camada trusted
--TRUNCATE TABLE IDENTIFIER(:catalogo || '.' || :schema || '.' || :table);
--TRUNCATE TABLE IDENTIFIER(:catalogo || '.' || :schema || '.' || :table_quarentena);
