import boto3
import os
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Pega o nome do Job do Glue a partir das variáveis de ambiente
GLUE_JOB_NAME = os.environ.get('GLUE_JOB_NAME')
# Pega o nome do bucket a partir do evento, mas podemos ter um padrão
BUCKET_NAME = os.environ.get('BUCKET_NAME')

s3_client = boto3.client('s3')
glue_client = boto3.client('glue')

def clean_s3_prefix(bucket, prefix):
    """Função para deletar todos os objetos dentro de um prefixo (pasta) no S3."""
    # Garante que o prefixo termine com /
    if not prefix.endswith('/'):
        prefix += '/'
        
    logger.info(f"Iniciando limpeza da pasta: s3://{bucket}/{prefix}")
    
    # Lista objetos na pasta
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    
    if 'Contents' not in response:
        logger.info("Nenhum objeto encontrado para deletar.")
        return

    # Prepara a lista de objetos para deletar
    objects_to_delete = [{'Key': obj['Key']} for obj in response['Contents']]
    
    # Deleta os objetos em lote
    s3_client.delete_objects(
        Bucket=bucket,
        Delete={'Objects': objects_to_delete}
    )
    
    logger.info(f"Limpeza de s3://{bucket}/{prefix} concluída com sucesso.")


def lambda_handler(event, context):
    """
    Orquestra o pipeline:
    1. Limpa as pastas de destino no S3.
    2. Inicia o Job do Glue.
    """
    if not GLUE_JOB_NAME or not BUCKET_NAME:
        logger.error("Erro: Variáveis de ambiente GLUE_JOB_NAME e BUCKET_NAME devem ser definidas.")
        return
        
    # Pega o caminho do arquivo que acionou o evento para passar ao Glue
    source_key = event['Records'][0]['s3']['object']['key']
    source_s3_path = f"s3://{BUCKET_NAME}/{source_key}"
    
    try:
        # 1. Limpa as pastas de destino
        clean_s3_prefix(BUCKET_NAME, "refined")
        clean_s3_prefix(BUCKET_NAME, "summarized_by_type") # Ou o nome da sua pasta sumarizada
        
        # 2. Inicia o Job do Glue
        logger.info(f"Iniciando o Job do Glue: {GLUE_JOB_NAME}")
        response = glue_client.start_job_run(
            JobName=GLUE_JOB_NAME,
            Arguments={'--source_s3_path': source_s3_path}
        )
        logger.info(f"Job iniciado com sucesso. RunId: {response['JobRunId']}")
        
        return {'status': 'SUCESSO'}
        
    except Exception as e:
        logger.error(f"Ocorreu um erro na orquestração: {e}")
        raise e