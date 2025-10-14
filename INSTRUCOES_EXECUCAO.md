# 📖 Instruções de Execução - Pipeline NYC Taxi Data

Este documento contém instruções detalhadas para executar o pipeline completo de ingestão e análise de dados de táxi de Nova York.

## 📑 Índice

- [Pré-requisitos](#-pré-requisitos)
- [Configuração Inicial](#-configuração-inicial)
- [Execução Passo a Passo](#-execução-passo-a-passo)
- [Execução via Databricks Workflow](#-execução-via-databricks-workflow)
- [Troubleshooting](#-troubleshooting)
- [Validação dos Resultados](#-validação-dos-resultados)

---

## 🔧 Pré-requisitos

### 1. Ambiente Databricks

- ✅ Conta no **Databricks Community Edition** (ou workspace corporativo)
- ✅ Cluster configurado com:
  - Runtime: **DBR 13.3 LTS ou superior**
  - Python: **3.10+**
  - Spark: **3.4+**

### 2. Armazenamento S3

- ✅ Bucket S3 criado (ex: `s3://datalake-ifood/`)
- ✅ Credenciais AWS com permissões de leitura/escrita:
  - `aws_access_key_id`
  - `aws_secret_access_key`

### 3. Unity Catalog (Recomendado)

- ✅ Catálogo criado (ex: `ifood_catalog`)
- ✅ Schemas criados:
  - `raw_layer`
  - `trusted_layer`

### 4. Bibliotecas Python

Todas as dependências são instaladas automaticamente via `%pip install` nos notebooks.

---

## ⚙️ Configuração Inicial

### Passo 1: Criar Estrutura do Data Lake no S3

```bash
# Estrutura de diretórios no S3
s3://datalake-ifood/
├── landing_layer/
│   └── yellow/              # Arquivos Parquet originais
├── raw_layer/
│   └── tb_taxi_data_api/    # Dados brutos (Delta)
└── trusted_layer/
    └── tb_taxi_data_for_analysis/  # Dados refinados (Delta)
```

### Passo 2: Configurar Unity Catalog

Execute no SQL Editor do Databricks:

```sql
-- Criar catálogo (se não existir)
CREATE CATALOG IF NOT EXISTS ifood_catalog;

-- Criar schemas com localização externa
CREATE SCHEMA IF NOT EXISTS ifood_catalog.raw_layer
COMMENT 'Camada raw - dados brutos'
MANAGED LOCATION 's3://datalake-ifood/raw_layer/';

CREATE SCHEMA IF NOT EXISTS ifood_catalog.trusted_layer
COMMENT 'Camada trusted - dados refinados'
MANAGED LOCATION 's3://datalake-ifood/trusted_layer/';
```

### Passo 3: Configurar Secrets (Recomendado)

Para segurança, armazene credenciais AWS no Databricks Secrets:

```python
# Via CLI do Databricks
databricks secrets create-scope --scope aws-credentials
databricks secrets put --scope aws-credentials --key access-key-id
databricks secrets put --scope aws-credentials --key secret-access-key
```

---

## 🚀 Execução Passo a Passo

### Etapa 1: Extrair Dados para Landing Zone

**Arquivo:** `src/extract_to_landing.py`

#### Execução Manual no Notebook:

1. Abra o notebook no Databricks
2. Configure os widgets (parâmetros):

```python
# Criar widgets
dbutils.widgets.text("datalake_path", "s3://datalake-ifood/landing_layer")
dbutils.widgets.text("start_date", "2023-01")
dbutils.widgets.text("end_date", "2023-05")
dbutils.widgets.text("aws_access_key_id", "AKIA...")
dbutils.widgets.text("aws_secret_access_key", "******")
```

3. Execute todas as células (`Run All`)

#### O que acontece:

- ✅ Download de arquivos Parquet da NYC TLC (Jan-Mai 2023)
- ✅ Upload para `s3://datalake-ifood/landing_layer/yellow/`
- ✅ Verificação de duplicatas (idempotente)

#### Tempo estimado: **30 segundos**

---

### Etapa 2: Transformar para Raw Layer

**Arquivo:** `src/landing_to_raw.py`

#### Execução Manual no Notebook:

1. Abra o notebook no Databricks
2. Configure os widgets:

```python
dbutils.widgets.text("start_date", "2023-01")
dbutils.widgets.text("end_date", "2023-05")
dbutils.widgets.text("landing_path", "s3://datalake-ifood/landing_layer")
dbutils.widgets.text("catalogo", "ifood_catalog")
dbutils.widgets.text("schema", "raw_layer")
dbutils.widgets.text("table", "tb_taxi_data_api")
```

3. Execute todas as células (`Run All`)

#### O que acontece:

- ✅ Leitura dos Parquets da landing
- ✅ Validação e normalização de schema
- ✅ Criação da tabela Delta (se não existir)
- ✅ Inserção de dados com DELETE + INSERT (idempotente)
- ✅ Clustering por `date_reference` e `VendorID`

#### Tempo estimado: **2 minutos**

#### Verificação:

```sql
SELECT COUNT(*) AS total_registros 
FROM ifood_catalog.raw_layer.tb_taxi_data_api;

-- Resultado esperado: ~14 milhões de registros (Jan-Mai 2023)
```

---

### Etapa 3: Refinar para Trusted Layer

**Arquivo:** `src/raw_to_trusted.sql`

#### Execução Manual no Notebook:

1. Abra o notebook SQL no Databricks
2. Configure os widgets:

```sql
-- No início do notebook
CREATE WIDGET TEXT catalogo DEFAULT 'ifood_catalog';
CREATE WIDGET TEXT schema DEFAULT 'trusted_layer';
CREATE WIDGET TEXT table DEFAULT 'tb_taxi_data_for_analysis';
```

3. Execute todas as células (`Run All`)

#### O que acontece:

- ✅ Criação da tabela trusted (se não existir)
- ✅ MERGE com SELECT DISTINCT da raw
- ✅ Mantém apenas colunas necessárias:
  - `vendor_id`
  - `tpep_pickup_datetime`
  - `tpep_dropoff_datetime`
  - `passenger_count`
  - `total_amount`
- ✅ Remove duplicatas
- ✅ Clustering por `vendor_id`

#### Tempo estimado: **30 segundos**

#### Verificação:

```sql
SELECT 
  COUNT(*) AS total_registros,
  COUNT(DISTINCT vendor_id) AS vendors_unicos,
  MIN(tpep_pickup_datetime) AS primeira_corrida,
  MAX(tpep_pickup_datetime) AS ultima_corrida
FROM ifood_catalog.trusted_layer.tb_taxi_data_for_analysis;
```

---

### Etapa 4: Executar Análises

**Arquivos:** 
- `analysis/analysis_queries.sql` (SQL)
- `analysis/analysis_queries.py` (PySpark - alternativa)

#### Opção A: Análises em SQL

1. Abra `analysis_queries.sql` no Databricks
2. Execute as células sequencialmente
3. Visualize os resultados inline

**Principais queries:**

```sql
-- Pergunta 1: Média de valor total por mês
SELECT 
    YEAR(tpep_pickup_datetime) AS ano,
    MONTH(tpep_pickup_datetime) AS mes,
    ROUND(AVG(total_amount), 2) AS media_valor_total,
    COUNT(*) AS total_corridas
FROM ifood_catalog.trusted_layer.tb_taxi_data_for_analysis
WHERE total_amount > 0
GROUP BY YEAR(tpep_pickup_datetime), MONTH(tpep_pickup_datetime)
ORDER BY ano, mes;

-- Pergunta 2: Média de passageiros por hora (Maio/2023)
SELECT 
    HOUR(tpep_pickup_datetime) AS hora_do_dia,
    ROUND(AVG(passenger_count), 2) AS media_passageiros,
    COUNT(*) AS total_corridas
FROM ifood_catalog.trusted_layer.tb_taxi_data_for_analysis
WHERE YEAR(tpep_pickup_datetime) = 2023
    AND MONTH(tpep_pickup_datetime) = 5
    AND passenger_count > 0
GROUP BY HOUR(tpep_pickup_datetime)
ORDER BY hora_do_dia;
```

#### Opção B: Análises em PySpark

1. Abra `analysis_queries.py` no Databricks
2. Execute todas as células
3. Resultados são exibidos via `.show()` e logs

#### Tempo estimado: **2-5 minutos**

---

## 🔄 Execução via Databricks Workflow

### Criar Workflow Automatizado

1. Navegue para **Workflows** → **Create Job**

2. Configure as tasks na ordem:

#### Task 1: Extract to Landing
```yaml
Task name: extract_to_landing
Type: Notebook
Path: /Workspace/ifood-case/src/extract_to_landing
Cluster: Cluster compartilhado
Parameters:
  - datalake_path: s3://datalake-ifood/landing_layer
  - start_date: 2023-01
  - end_date: 2023-05
  - aws_access_key_id: {{secrets/aws-credentials/access-key-id}}
  - aws_secret_access_key: {{secrets/aws-credentials/secret-access-key}}
```

#### Task 2: Landing to Raw
```yaml
Task name: landing_to_raw
Type: Notebook
Path: /Workspace/ifood-case/src/landing_to_raw
Depends on: extract_to_landing
Cluster: Cluster compartilhado
Parameters:
  - start_date: 2023-01
  - end_date: 2023-05
  - landing_path: s3://datalake-ifood/landing_layer
  - catalogo: ifood_catalog
  - schema: raw_layer
  - table: tb_taxi_data_api
```

#### Task 3: Raw to Trusted
```yaml
Task name: raw_to_trusted
Type: Notebook
Path: /Workspace/ifood-case/src/raw_to_trusted
Depends on: landing_to_raw
Cluster: Cluster compartilhado
Parameters:
  - catalogo: ifood_catalog
  - schema: trusted_layer
  - table: tb_taxi_data_for_analysis
```

#### Task 4: Run Analysis
```yaml
Task name: run_analysis
Type: Notebook
Path: /Workspace/ifood-case/analysis/analysis_queries
Depends on: raw_to_trusted
Cluster: Cluster compartilhado
```

3. Configure Schedule (opcional):
```yaml
Schedule: Cron
Expression: 0 2 * * * (diariamente às 2h AM)
Timezone: America/Sao_Paulo
```

4. **Run Now** para executar manualmente

---

## 🐛 Troubleshooting

### Problema 1: Erro de Credenciais AWS

**Sintoma:**
```
ClientError: An error occurred (403) when calling the HeadObject operation: Forbidden
```

**Solução:**
- Verificar se as credenciais AWS estão corretas
- Confirmar permissões IAM: `s3:GetObject`, `s3:PutObject`, `s3:ListBucket`
- Validar se o bucket existe

---

### Problema 2: Tabela não encontrada

**Sintoma:**
```
[TABLE_OR_VIEW_NOT_FOUND] The table or view `ifood_catalog`.`raw_layer`.`tb_taxi_data_api` cannot be found
```

**Solução:**
```sql
-- Verificar se o catálogo existe
SHOW CATALOGS;

-- Verificar se o schema existe
SHOW SCHEMAS IN ifood_catalog;

-- Recriar schema se necessário
CREATE SCHEMA IF NOT EXISTS ifood_catalog.raw_layer
MANAGED LOCATION 's3://datalake-ifood/raw_layer/';
```

---

### Problema 3: Arquivo Parquet não encontrado

**Sintoma:**
```
Path does not exist: s3://datalake-ifood/landing_layer/yellow/yellow_tripdata_2023-01.parquet
```

**Solução:**
- Re-executar `extract_to_landing.py`
- Verificar se o download foi concluído com sucesso
- Validar se o caminho do S3 está correto

---

### Problema 4: Duplicatas na Trusted

**Sintoma:**
```
Registros duplicados detectados após MERGE
```

**Solução:**
```sql
-- Limpar a tabela e re-executar o MERGE
TRUNCATE TABLE ifood_catalog.trusted_layer.tb_taxi_data_for_analysis;

-- Re-executar raw_to_trusted.sql
```

---

### Problema 5: Performance lenta

**Sintoma:**
- Jobs demoram mais de 30 minutos

**Solução:**
1. Aumentar tamanho do cluster (mais workers)
2. Habilitar Photon:
```python
spark.conf.set("spark.databricks.photon.enabled", "true")
```
3. Verificar clustering das tabelas:
```sql
DESCRIBE DETAIL ifood_catalog.raw_layer.tb_taxi_data_api;
```

---

## ✅ Validação dos Resultados

### Checklist Final

Execute as queries abaixo para validar a execução completa:

#### 1. Landing Layer
```bash
# Via AWS CLI
aws s3 ls s3://datalake-ifood/landing_layer/yellow/ --recursive

# Deve listar 5 arquivos:
# yellow_tripdata_2023-01.parquet
# yellow_tripdata_2023-02.parquet
# yellow_tripdata_2023-03.parquet
# yellow_tripdata_2023-04.parquet
# yellow_tripdata_2023-05.parquet
```

#### 2. Raw Layer
```sql
SELECT 
  'Total de Registros' AS metrica,
  CAST(COUNT(*) AS STRING) AS valor
FROM ifood_catalog.raw_layer.tb_taxi_data_api

UNION ALL

SELECT 
  'Período Coberto' AS metrica,
  CONCAT(
    CAST(MIN(date_reference) AS STRING), 
    ' até ', 
    CAST(MAX(date_reference) AS STRING)
  ) AS valor
FROM ifood_catalog.raw_layer.tb_taxi_data_api;

-- Resultado esperado:
-- Total de Registros: ~16.186.386
-- Período Coberto: 2023-01-01 até 2023-05-01
```

#### 3. Trusted Layer
```sql
SELECT 
  COUNT(*) AS total_registros,
  COUNT(DISTINCT vendor_id) AS vendors_unicos,
  ROUND(AVG(total_amount), 2) AS media_valor,
  ROUND(AVG(passenger_count), 2) AS media_passageiros
FROM ifood_catalog.trusted_layer.tb_taxi_data_for_analysis
WHERE total_amount > 0 AND passenger_count > 0;

-- Resultado esperado:
-- total_registros: ~15.340.356
-- vendors_unicos: 2-4
-- media_valor: ~28.32
-- media_passageiros: ~1.39
```

#### 4. Análises - Pergunta 1
```sql
SELECT 
    YEAR(tpep_pickup_datetime) AS ano,
    MONTH(tpep_pickup_datetime) AS mes,
    CONCAT(
        YEAR(tpep_pickup_datetime), 
        '-', 
        LPAD(MONTH(tpep_pickup_datetime), 2, '0')
    ) AS periodo,
    ROUND(AVG(total_amount), 2) AS media_valor_total
FROM ifood_catalog.trusted_layer.tb_taxi_data_for_analysis
WHERE total_amount IS NOT NULL
    AND total_amount > 0
    AND YEAR(tpep_pickup_datetime) = 2023
    AND MONTH(tpep_pickup_datetime) <= 5
GROUP BY 
    YEAR(tpep_pickup_datetime),
    MONTH(tpep_pickup_datetime)
ORDER BY ano, mes;

-- Resultado esperado: 5 linhas (2023-01 até 2023-05)
-- Valores entre $27-30
```

#### 5. Análises - Pergunta 2
```sql
SELECT 
    HOUR(tpep_pickup_datetime) AS hora_do_dia,
    ROUND(AVG(passenger_count), 2) AS media_passageiros
FROM ifood_catalog.trusted_layer.tb_taxi_data_for_analysis
WHERE YEAR(tpep_pickup_datetime) = 2023
    AND MONTH(tpep_pickup_datetime) = 5
    AND passenger_count IS NOT NULL
    AND passenger_count > 0
GROUP BY HOUR(tpep_pickup_datetime)
ORDER BY hora_do_dia;

-- Resultado esperado: 24 linhas (hora 0-23)
-- Valores entre 1.26-1.45
```

---

## 📊 Exportar Resultados

### Opção 1: Via Databricks UI

1. Execute a query de análise
2. Clique em **Download** → **CSV**
3. Salve localmente

### Opção 2: Via Código

```python
# Exportar para CSV no DBFS
df = spark.table("ifood_catalog.trusted_layer.tb_taxi_data_for_analysis")
df.coalesce(1).write.csv("dbfs:/FileStore/resultados/taxi_data.csv", header=True)

# Download via UI: Data → DBFS → FileStore → resultados
```

### Opção 3: Para S3

```python
df.write.mode("overwrite").parquet("s3://datalake-ifood/exports/taxi_analysis/")
```

---

## 🎯 Próximos Passos

Após validação bem-sucedida:

1. ✅ **Conectar ferramentas de BI** (Tableau, Power BI, Looker)
2. ✅ **Criar dashboards** com as análises
3. ✅ **Configurar alertas** para anomalias
4. ✅ **Expandir para mais meses** alterando `start_date` e `end_date`
5. ✅ **Adicionar testes automatizados** (Great Expectations, dbt tests)
6. ✅ **Implementar CI/CD** para deploy automático

---

## 📞 Suporte

Em caso de dúvidas:

- 📧 Abra uma **issue** no repositório GitHub
- 📖 Consulte a [documentação oficial do Databricks](https://docs.databricks.com/)
- 📖 Consulte o [Data Dictionary da NYC TLC](https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf)

---

**Autor:** Vinicius  
**Data:** 2025  
**Versão:** 1.0