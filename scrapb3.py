import base64
import io
import logging
import os
import time
from datetime import date

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests
from botocore.exceptions import BotoCoreError, ClientError

# O logger do Lambda já é configurado, então podemos apenas obter a instância
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# A URL base da API da B3
B3_API_BASE_URL = "https://sistemaswebb3-listados.b3.com.br/indexProxy/indexCall/GetPortfolioDay/"

# O prefixo para organizar os dados no S3
S3_KEY_PREFIX = "raw"

def _encode_api_payload(page: int, page_size: int = 120, index: str = "IBOV") -> str:
    """Codifica o payload da requisição para a API da B3 em Base64."""
    payload = f'{{"language":"pt-br","pageNumber":{page},"pageSize":{page_size},"index":"{index}","segment":"1"}}'
    return base64.b64encode(payload.encode("utf-8")).decode("utf-8")


def fetch_b3_index_composition() -> pd.DataFrame:
    """
    Busca a composição completa de um índice na B3, tratando a paginação e
    corrigindo a coluna de participação acumulada (partacum).
    """
    collected_results, current_page = [], 1
    # Variável para armazenar o valor da participação acumulada da última página
    participacao_acumulada = None

    logger.info("Iniciando coleta de dados da composição do IBOV na B3.")

    while True:
        try:
            payload = _encode_api_payload(current_page)
            request_url = f"{B3_API_BASE_URL}{payload}"
            response = requests.get(request_url, headers={"User-Agent": "Mozilla/5.0"}, timeout=30)
            response.raise_for_status()
            
            json_response = response.json()
            data = json_response.get("results", [])
            
            if not data:
                logger.info("Não foram encontrados mais resultados. Finalizando a coleta.")
                # Captura o valor de partAcum da última resposta que continha dados
                if json_response.get("page"):
                    participacao_acumulada = json_response["page"].get("partAcum")
                break
            
            collected_results.extend(data)
            logger.info(f"Página {current_page} coletada com sucesso ({len(data)} ativos).")
            current_page += 1
            time.sleep(0.2)
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Erro de rede ao tentar acessar a API da B3: {e}")
            return pd.DataFrame()

    if not collected_results:
        logger.warning("A coleta foi concluída, mas nenhum dado foi retornado pela API.")
        return pd.DataFrame()

    ibov_dataframe = pd.DataFrame(collected_results)
    
    # Preenche a coluna 'partacum' com o valor capturado da última página
    if participacao_acumulada:
        logger.info(f"Preenchendo a participação acumulada com o valor: {participacao_acumulada}")
        ibov_dataframe['partAcum'] = participacao_acumulada
    else:
        logger.warning("Não foi possível encontrar o valor da participação acumulada (partAcum).")
        ibov_dataframe['partAcum'] = None # Garante que a coluna exista, mesmo que nula

    ibov_dataframe["data_coleta"] = pd.to_datetime(date.today())
    ibov_dataframe['data_coleta'] = ibov_dataframe['data_coleta'].dt.date
    
    logger.info(f"Coleta finalizada. Total de {len(ibov_dataframe)} ativos encontrados.")
    return ibov_dataframe

def convert_df_to_parquet_buffer(dataframe: pd.DataFrame) -> io.BytesIO:
    """Converte um DataFrame do Pandas para um buffer de bytes em formato Parquet."""
    in_memory_buffer = io.BytesIO()
    arrow_table = pa.Table.from_pandas(dataframe, preserve_index=False)
    pq.write_table(arrow_table, in_memory_buffer)
    in_memory_buffer.seek(0)
    return in_memory_buffer


def upload_buffer_to_s3(buffer: io.BytesIO, bucket: str, key: str, region: str) -> bool:
    """Realiza o upload de um buffer de bytes para um bucket do S3."""
    try:
        s3_client = boto3.client("s3", region_name=region)
        s3_client.upload_fileobj(buffer, bucket, key)
        logger.info(f"Upload concluído com sucesso para: s3://{bucket}/{key}")
        return True
    except (BotoCoreError, ClientError) as e:
        logger.error(f"Falha no upload para o S3: {e}")
        return False

def lambda_handler(event, context):
    """
    Função principal que o AWS Lambda executa.
    Orquestra o processo de ETL.
    """
    logger.info("--- Início do Processo de ETL para a carteira do IBOV ---")

    # É uma boa prática obter configurações das variáveis de ambiente do Lambda
    aws_region = os.environ.get('AWS_REGION', 'us-east-1')
    s3_bucket_name = os.environ.get('S3_BUCKET_NAME', 'bovespa-290892317785')

    # 1. Extração (Extract)
    ibov_dataframe = fetch_b3_index_composition()

    if ibov_dataframe.empty:
        logger.warning("DataFrame vazio. O processo será encerrado sem upload.")
        return {'statusCode': 200, 'body': 'Nenhum dado para processar.'}

    # 2. Transformação (Transform)
    parquet_buffer = convert_df_to_parquet_buffer(ibov_dataframe)

    # 3. Carregamento (Load)
    today_iso = date.today().isoformat()
    s3_object_key = f"{S3_KEY_PREFIX}/date={today_iso}/ibov_composition.parquet"
    
    success = upload_buffer_to_s3(parquet_buffer, s3_bucket_name, s3_object_key, aws_region)

    if success:
        logger.info("--- Fim do Processo de ETL ---")
        return {'statusCode': 200, 'body': f'Dados salvos com sucesso em {s3_object_key}'}
    else:
        logger.error("--- Processo de ETL falhou ---")
        return {'statusCode': 500, 'body': 'Falha no upload para o S3.'}