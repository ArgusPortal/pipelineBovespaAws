
# Pipeline de Dados da B3 na AWS - Tech Challenge FIAP



https://g.co/gemini/share/269056fca3f3 - `Link para página de apresentação`



## 1. Introdução
Este repositório contém a implementação de um pipeline de dados **serverless na AWS**, desenvolvido como parte do Tech Challenge da Fase 2 da Pós-graduação em **Machine Learning Engineering da FIAP**.

O projeto consiste em um fluxo de **ETL (Extração, Transformação e Carga)** completo e automatizado, que:
1. Coleta a composição diária do índice Ibovespa (IBOV) do site da B3
2. Processa e enriquece os dados
3. Armazena em um Data Lake
4. Disponibiliza para análise via SQL

## 2. Arquitetura do Pipeline
Arquitetura **event-driven** com componentes desacoplados para garantir resiliência e manutenibilidade:

[![arq.png](https://i.postimg.cc/pXs0L4Cn/arq.png)](https://postimg.cc/xcbGssW0)

## 3. Requisitos do Projeto e Implementação

### Requisito 1: Scrap de dados do site da B3
✅ **Status:** Atendido  
🔧 **Implementação:**  
Função Lambda (`scrapb3`) em Python 3.10 usando:
- `requests` para conexão com API da B3
- Tratamento de paginação
- Pandas para manipulação inicial

### Requisito 2: Ingestão em S3 em Parquet
✅ **Status:** Atendido  
🔧 **Implementação:**  
- Serialização com `pyarrow`
- Formato Parquet + compressão Snappy
- Partição diária no estilo Hive: `s3://.../raw/date=2025-07-30/`

### Requisito 3: S3 Trigger → Lambda → Glue
✅ **Status:** Atendido  
🔧 **Implementação:**  
- Gatilho S3 configurado para `s3:ObjectCreated:*`
- Aciona Lambda `orquestrador-glue` ao detectar novo arquivo em `raw/`

### Requisito 4: Lambda para iniciar Glue Job
✅ **Status:** Atendido  
🔧 **Implementação:**  
Lambda `orquestrador-glue` em Python:
- Chama API Glue via `boto3.glue_client.start_job_run()`
- Executa purge nos diretórios de destino antes do job

### Requisito 5: Glue Job no modo visual
✅ **Status:** Atendido  
🔧 **Implementação:**  
Job `processar-dados-bovespa-visual` no Glue Studio com:

| Transformação          | Técnica                          |
|------------------------|----------------------------------|
| Agrupamento numérico   | Nó "Aggregate" por `type`        |
| Renomear colunas       | Nós "Rename Field"               |
| Cálculo com datas      | Custom Transform (PySpark)       |

**Detalhes das transformações:**  
```python
# Cálculo de timestamp (Custom Transform)
df = df.withColumn("timestamp_processamento", current_timestamp())
df = df.withColumn("lag_processamento_segundos", 
                  col("timestamp_processamento").cast("long") - col("data_coleta").cast("long"))
```

### Requisito 6: Dados refinados em Parquet particionados
✅ **Status:** Atendido  
🔧 **Implementação:**
- Salvo em `s3://.../refined/`
- Particionamento por: `ano`, `mes`, `dia`, `codigo_acao`

### Requisito 7: Catalogação automática no Glue Catalog
✅ **Status:** Atendido  
🔧 **Implementação:** Sinks configurados com:
- "Create/update table in Data Catalog"
- Tabelas criadas: `dados_bovespa_refinados_visual` e `sumarizacao_por_tipo`

### Requisito 8: Dados disponíveis no Athena
✅ **Status:** Atendido  
🔧 **Implementação:** Validação via consultas:
```sql
SELECT * FROM dados_bovespa_refinados_visual LIMIT 10;
```

### Requisito 9: Notebook para visualização (Opcional)
✅ **Status:** Atendido  
🔧 **Implementação:** Notebook Athena com:
- Consultas via `boto3`
- Visualizações em ASCII (devido a limitações do ambiente)
- Gráficos de Top 10 ações e participação por tipo

## 4. Tecnologias Utilizadas

| Categoria            | Tecnologias/AWS Services           |
|----------------------|------------------------------------|
| Computação Serverless| AWS Lambda                         |
| Armazenamento        | Amazon S3                          |
| ETL                  | AWS Glue (Studio + PySpark)        |
| Catalogação          | AWS Glue Data Catalog              |
| Orquestração         | EventBridge + S3 Triggers          |
| Análise              | Amazon Athena                      |
| IAM                  | AWS Identity and Access Management |
| Linguagens           | Python 3.10                        |
| Bibliotecas          | Boto3, Pandas, PyArrow             |

## 5. Autor
**Argus Cordeiro Sales Portal**  
Turma MLET5 - FIAP
