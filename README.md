# Case Técnico Data Architect - iFood

## 📋 Sobre o Projeto

Este projeto implementa uma solução completa de ingestão, processamento e análise de dados de corridas de táxi de Nova York (NYC TLC Trip Record Data) utilizando uma arquitetura de Data Lake em camadas.

## 🏗️ Arquitetura da Solução

A solução foi construída seguindo o padrão de arquitetura medalhão com uma Landing Zone:

### 1. **Landing Layer**
- **Localização**: `s3://datalake-ifood/landing_layer/yellow/`
- **Formato**: Parquet (arquivos originais)
- **Propósito**: Armazenamento dos dados brutos sem transformação
- **Processo**: Script `extract_to_landing.py`

### 2. **Raw Layer** (Bronze)
- **Localização**: `s3://datalake-ifood/raw_layer/tb_taxi_data_api/`
- **Formato**: Delta Lake
- **Propósito**: Dados estruturados com validações e padronizações
- **Processo**: Script `landing_to_raw.py`
- **Características**:
  - Schema enforcement
  - Validação de tipos de dados
  - Tratamento de colunas faltantes
  - Particionamento por `date_reference` e `VendorID`

### 3. **Trusted Layer** (Silver)
- **Localização**: `s3://datalake-ifood/trusted_layer/tb_taxi_data_for_analysis/`
- **Formato**: Delta Lake
- **Propósito**: Dados otimizados para consumo e análise
- **Processo**: Script `raw_to_trusted.sql`
- **Características**:
  - Apenas colunas necessárias para análise
  - Dados deduplicated
  - Otimizado para queries analíticas

## Databricks Catalog + AWS S3
- **Bucket**: `s3://datalake-ifood`
- **Catálogo**: `ifood_catalog`

## Desenho da arquitetura


                                   ┌─────────────────────────────────┐
                                   │  ☁️  AWS S3: datalake-ifood     │
                                   └─────────────────────────────────┘
                                                 │
                    ┌────────────────────────────┼────────────────────────────┐
                    │                            │                            │
                    ▼                            ▼                            ▼
        ┌───────────────────────┐   ┌───────────────────────┐   ┌───────────────────────┐
        │ 📦 Landing Layer      │   │ 📊 Raw Layer         │    │ 🎯 Trusted Layer     │
        │ landing_layer/yellow/ │   │ raw_layer/            │   │ trusted_layer/        │
        │ (Parquet files)       │   │ (Delta tables)        │   │ (Delta tables)        │
        └───────────────────────┘   └───────────────────────┘   └───────────────────────┘
                    │                            ▲                            ▲
                    │ write                      │ write                      │ write
                    │                            │                            │
    ┌───────────────▼──────────┐                 │                            │
    │ 📥 TASK 1                │                 │                            │
    │ extract_to_landing.py    │                 │                            │
    ├──────────────────────────┤                 │                            │
    │ • Download NYC TLC data  │                 │                            │
    │ • Upload to S3 Landing   │                 │                            │
    │ • Check duplicates       │                 │                            │
    └────────────┬─────────────┘                 │                            │
                 │ depends_on                    │                            │
                 │                               │                            │
    ┌────────────▼─────────────┐                 │                            │
    │ 🔄 TASK 2                │                 │                            │
    │ landing_to_raw.py        │─────────────────┘                            │
    ├──────────────────────────┤ read from Landing                            │
    │ • Read Parquet files     │ write to Raw                                 │
    │ • Validate schema        │                                              │
    │ • Apply transformations  │                                              │
    │ • Write Delta to Raw     │                                              │
    └────────────┬─────────────┘                                              │
                 │ depends_on                                                 │
                 │                                                            │
    ┌────────────▼───────────────────────────────────────────────────────── ┐ │
    │ ✨ TASK 3: raw_to_trusted.sql                                        │  │
    ├───────────────────────────────────────────────────────────────────────┤ │
    │                                                                       │ │
    │  STEP 1: CREATE TEMPORARY TABLE                                       │ │
    │  ┌─────────────────────────────────────────────────────────────────┐  │ │
    │  │ 📝 tmp_tb_taxi_data_for_analysis                                │  │ │
    │  │ Location: s3://datalake-ifood/trusted_layer/tmp/                │  │ │
    │  │                                                                 │  │ │
    │  │ • Read from Raw Layer                                           │  │ │
    │  │ • Apply type conversions                                        │  │ │
    │  │ • Create validation flags                                       │  │ │
    │  └─────────────────────────────────────────────────────────────────┘  │ │
    │                                   │                                   │ │
    │                                   │                                   │ │
    │  STEP 2: DATA QUALITY VALIDATION                                      │ │
    │                                   │                                   │ │
    │              ┌────────────────────┼────────────────────┐              │ │
    │              │                    │                    │              │ │
    │              ▼                    │                    ▼              │ │
    │  ┌────────────────────┐           │        ┌────────────────────┐     │ │
    │  │ ✅ tem_erro = 0    │           │        │ ❌ tem_erro = 1   │     │ │
    │  │ (Valid Records)    │           │        │ (Invalid Records)  │    │ │
    │  └──────────┬─────────┘           │        └──────────┬─────────┘    │ │
    │             │                     │                   │              │ │
    │             │                     │                   │              │ │
    │             │        ┌────────────▼───────────┐       │              │ │
    │             │        │ 📊 Quality Metrics     │       │              │ │
    │             │        │ • Total records        │       │              │ │
    │             │        │ • Error rate           │       │              │ │
    │             │        │ • Type validation      │       │              │ │
    │             │        │ • Business rules       │       │              │ │
    │             │        │                        │       │              │ │
    │             │        │ ASSERT validations     │       │              │ │
    │             │        │ (threshold checks)     │       │              │ │
    │             │        └────────────────────────┘       │              │ │
    │             │                     │                   │              │ │
    │             │                     │ ✅ PASS           │              │ │
    │             │                     │                   │              │ │
    │  STEP 3:    │                     ▼                   │  STEP 4:     │ │
    │  INSERT     │                                         │  INSERT      │ │
    │             │                                         │              │ │
    │             ▼                                         ▼              │ │
    │  ┌──────────────────────┐              ┌──────────────────────────┐  │ │
    │  │ 🎯 tb_taxi_data_     │              │ 🚨 tb_taxi_data_         │ │ │
    │  │    for_analysis      │              │    quarentena            │ │ │
    │  ├──────────────────────┤              ├──────────────────────────┤ │ │
    │  │ • vendor_id          │              │ • All original fields    │ │ │
    │  │ • tpep_pickup_*      │              │ • Validation flags       │ │ │
    │  │ • tpep_dropoff_*     │              │ • motivos_erro           │ │ │
    │  │ • passenger_count    │              │ • tipo_erro              │ │ │
    │  │ • total_amount       │              │ • data_hora_quarentena   │ │ │
    │  │ • trip_duration      │              │ • data_particao          │ │ │
    │  │ • pickup_date        │              │                          │ │ │
    │  │ • data_hora_carga    │              │ Categories:              │ │ │
    │  │ • data_particao      │              │ • ERRO_TIPAGEM           │ │ │
    │  │                      │              │ • ERRO_NULO              │ │ │
    │  │ ✅ Analysis Ready    │              │ • ERRO_LOGICA_NEGOCIO    │ │ │
    │  └──────────────────────┘              │ • ERRO_VALIDACAO_RANGE   │ │ │
    │             │                          └──────────────────────────┘ │ │
    │             │                                     │                 │ │
    └─────────────┼─────────────────────────────────────┼─────────────────┘ │
                  │                                     │                   │
                  │ write to Trusted                    │ write to Trusted  │
                  └─────────────────┬───────────────────┘                   │
                                    │                                       │
                                    ▼                                       │
                      ┌─────────────────────────────────────────────────────┘
                      │
                      ▼
        ┌─────────────────────────────────────────────────────────────────┐
        │  🗄️  UNITY CATALOG: ifood_catalog                               │
        ├─────────────────────────────────────────────────────────────────┤
        │                                                                  │
        │  📋 Schema: raw_layer                                            │
        │     └─ Table: tb_taxi_data_api                                   │
        │        ├─ Location: s3://.../raw_layer/                          │
        │        ├─ Format: Delta                                          │
        │        ├─ Cluster by: date_reference, VendorID                   │
        │        └─ Columns: 21 (all original fields)                      │
        │                                                                  │
        │  📋 Schema: trusted_layer                                        │
        │     ├─ Table: tmp_tb_taxi_data_for_analysis (TEMPORARY)          │
        │     │  ├─ Location: s3://.../trusted_layer/tmp/                  │
        │     │  ├─ Format: Delta                                          │
        │     │  └─ Contains: Raw data + validation flags                  │
        │     │                                                            │
        │     ├─ Table: tb_taxi_data_for_analysis (FINAL)                  │
        │     │  ├─ Location: s3://.../trusted_layer/                      │
        │     │  ├─ Format: Delta                                          │
        │     │  ├─ Cluster by: vendor_id                                  │
        │     │  └─ Columns: 5 core + metadata (valid records only)        │
        │     │                                                            │
        │     └─ Table: tb_taxi_data_quarentena (QUARANTINE)               │
        │        ├─ Location: s3://.../trusted_layer/quarentena/           │
        │        ├─ Format: Delta                                          │
        │        ├─ Partition by: data_particao_quarentena, tipo_erro      │
        │        └─ Contains: Invalid records + error details              │
        │                                                                  │
        └──────────────────────────────────────────────────────────────────┘
                                    │
                                    │ query via SQL
                                    │
                      ┌─────────────┴───────────────┐
                      │                             │
                      ▼                             ▼
        ┌──────────────────────────┐  ┌──────────────────────────────┐
        │  👥 Data Consumers       │  │  🔧 Data Quality Team        │
        ├──────────────────────────┤  ├──────────────────────────────┤
        │ • Analysts (SQL queries) │  │ • Monitor quarantine table   │
        │ • Data Scientists        │  │ • Analyze error patterns     │
        │ • Dashboards (BI tools)  │  │ • Fix data quality issues    │
        │ • ML Models              │  │ • Update validation rules    │
        └──────────────────────────┘  └──────────────────────────────┘
                  │                                  │
                  │                                  │
                  └────────── Query only ────────────┘
                        tb_taxi_data_for_analysis
                        (Clean, validated data)

📅 Schedule: Manual ou Cron

🖥️  Cluster: Serverless

⏱️  Total Duration: ~3 minutes (for 5 days)

## 🛠️ Tecnologias Utilizadas

- **Apache Spark (PySpark)**: Processamento distribuído de dados
- **Delta Lake**: Formato de armazenamento ACID para Data Lakes
- **Databricks**: Plataforma de execução e orquestração
- **AWS S3**: Armazenamento de objetos
- **Unity Catalog**: Governança e metadados

## 📦 Estrutura do Repositório

```
ifood-case/
├── src/
│   ├── extract_to_landing.py      # Extração de dados da API NYC TLC para Landing
│   ├── landing_to_raw.py          # Transformação Landing → Raw com validações
│   └── raw_to_trusted.sql         # Transformação Raw → Trusted (camada de consumo)
├── analysis/
│   └── analise_dados.sql          # Queries analíticas solicitadas
├── README.md                       # Este arquivo
└── requirements.txt                # Dependências Python
```

## 🚀 Como Executar

### Pré-requisitos

1. **Databricks Workspace** (Community Edition ou superior)
2. **AWS S3 Bucket** configurado
3. **Credenciais AWS** (Access Key ID e Secret Access Key)
4. **Unity Catalog** configurado no Databricks

### Configuração Inicial

#### 1. Criar o Catálogo e Schemas

Execute o notebook `query_mapping_external_location.sql` para criar as estruturas necessárias:

```sql
-- Criar schema para camada Raw
CREATE SCHEMA ifood_catalog.raw_layer
COMMENT 'Camada raw'
MANAGED LOCATION 's3://datalake-ifood/raw_layer/';

-- Criar schema para camada Trusted
CREATE SCHEMA ifood_catalog.trusted_layer
COMMENT 'Camada trusted'
MANAGED LOCATION 's3://datalake-ifood/trusted_layer/';
```

#### 2. Criar Job no Databricks

Crie um Databricks Job com as seguintes tasks encadeadas:

**Task 1: Extract to Landing**
```
Notebook: extract_to_landing.py
Parâmetros:
  - aws_access_key_id: <sua_access_key>
  - aws_secret_access_key: <sua_secret_key>
  - datalake_path: s3://datalake-ifood/landing_layer
  - start_date: 2023-01
  - end_date: 2023-05
```

**Task 2: Landing to Raw** (depende de Task 1)
```
Notebook: landing_to_raw.py
Parâmetros:
  - start_date: 2023-01
  - end_date: 2023-05
  - catalogo: ifood_catalog
  - schema: raw_layer
  - table: tb_taxi_data_api
```

**Task 3: Raw to Test to Trusted/Quarentena** (depende de Task 2)
```
Notebook: raw_to_trusted.sql
Parâmetros:
  - catalogo: ifood_catalog
  - schema: trusted_layer
  - table: tb_taxi_data_for_analysis
```

### Execução Manual (Databricks Notebooks)

1. Importe os notebooks para o Databricks Workspace
2. Configure os widgets/parâmetros conforme especificado acima
3. Execute na ordem: `extract_to_landing.py` → `landing_to_raw.py` → `raw_to_trusted.sql`

## 📊 Análises Disponíveis

O arquivo `analysis/analise_dados.sql` contém as seguintes análises:

### 1. Média de valor total por mês
Calcula a média do `total_amount` recebido mensalmente considerando todos os táxis yellow da frota.

### 2. Média de passageiros por hora do dia em Maio/2023
Calcula a média de `passenger_count` para cada hora do dia no mês de maio de 2023.

## 🔍 Decisões Técnicas

### Escolha do Delta Lake
- **ACID Transactions**: Garante consistência dos dados
- **Time Travel**: Permite auditoria e rollback
- **Schema Evolution**: Facilita adaptação a mudanças no schema
- **Otimização de Performance**: Z-ordering e clustering

### Validações Implementadas

1. **Validação de Datas**: Formato YYYY-MM obrigatório, datas futuras bloqueadas
2. **Schema Enforcement**: Validação rigorosa de tipos de dados
3. **Tratamento de Colunas**: 
   - Renomear colunas com case sensitivity incorreto
   - Criar colunas faltantes com valores NULL
4. **Deduplicação**: Remoção de registros duplicados na camada Trusted
5. **Idempotência**: Processo DELETE antes de INSERT evita duplicação

### Clustering Strategy

- **Raw Layer**: Clusterizado por `date_reference` e `VendorID` para otimizar queries filtradas por período e fornecedor
- **Trusted Layer**: Clusterizado por `vendor_id` para otimizar joins e agregações

## 📈 Monitoramento e Logs

O projeto utiliza a biblioteca `loguru` para logging estruturado:

- ✅ **SUCCESS**: Dados persistidos com sucesso
- ⚠️ **WARNING**: Arquivo já existe no bucket (evita duplicação)
- ❌ **ERROR**: Falhas no processamento (com detalhamento do erro)
- ℹ️ **INFO**: Eventos importantes do pipeline

## 🔒 Segurança

- Credenciais AWS passadas via widgets do Databricks (nunca hardcoded)
- Uso de IAM roles recomendado para ambientes produtivos
- Unity Catalog fornece governança de acesso aos dados

## 📝 Dependências

```
loguru==0.7.2
boto3==1.34.0
python-dateutil==2.8.2
requests==2.31.0
pytz==2023.3
```

Instale com: `%pip install -r requirements.txt`

## 🎯 Resultados Esperados

Após execução completa do pipeline:

1. **5 arquivos Parquet** na Landing Layer (Jan-Mai 2023)
2. **Tabela Delta `tb_taxi_data_api`** na Raw Layer com todas as colunas originais
3. **Tabela Delta `tb_taxi_data_for_analysis`** na Trusted Layer com apenas as 5 colunas necessárias:
   - `vendor_id`
   - `tpep_pickup_datetime`
   - `tpep_dropoff_datetime`
   - `passenger_count`
   - `total_amount`

## 🧪 Testes Implementados

### Testes de Schema
- Verificação de colunas esperadas vs. colunas presentes
- Validação de tipos de dados
- Tratamento de colunas extras ou faltantes

### Testes de Dados
- DataFrame não vazio após transformações
- Datas no formato correto
- Valores nulos controlados

## 🐛 Troubleshooting

### Erro: "Colunas faltando"
- Verifique se o schema dos arquivos Parquet corresponde ao esperado
- Execute novamente a Task 2 com `treat_and_test` habilitado

### Erro: "Bucket não encontrado"
- Verifique as credenciais AWS
- Confirme que o bucket existe e as permissões estão corretas

### Erro: "Data já existe"
- Comportamento esperado (idempotência)
- O processo DELETE garante que dados antigos sejam removidos antes de inserir novos

## 👥 Contato

Para dúvidas ou sugestões, abra uma issue no repositório.

## 📄 Licença

Este projeto foi desenvolvido como parte de um desafio técnico para o iFood.