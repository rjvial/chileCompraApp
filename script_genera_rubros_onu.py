import os
from dotenv import load_dotenv
import pandas as pd
import boto3
from botocore.config import Config
import funcionesAws as fa


fileDir = "I:\\Mi unidad\\Python\\chileCompraApp\\data\\"

load_dotenv(".env")

INSTANCIA_EC2 = 'Neo4j-EC2'

# Crea conexiones S3 y Athena vía boto3
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")
AWS_REGION = os.getenv("AWS_REGION")
DB_NAME = 'iceberg_db' #Nombre base para crear bases de datos

athena_client = boto3.client(
    "athena", #Nombre del tipo de servicio de aws
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name=AWS_REGION
)
s3_config = Config(
    connect_timeout=10,
    read_timeout=60,
    retries={"max_attempts": 3},
    tcp_keepalive=True,
)
s3_client = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name=AWS_REGION,
    config=s3_config,
)
ec2_client = boto3.client('ec2', region_name=AWS_REGION)

athena_bucket = 'tender-engines'
athena_output = 'query-results'
athena_catalog_name = 'AwsDataCatalog'


# Configuration dictionaries
aws_config = {
    'athena_client': athena_client,
    's3_client': s3_client,
    'athena_bucket': athena_bucket,
    'athena_output': athena_output,
    'db_name': DB_NAME
}

file_config = {
    'local_dir': fileDir,
    's3_bucket': athena_bucket,
    's3_prefix': 'kg/'
}



df_ungm = pd.read_excel('unspcs-clasificador-de-bienes-y-servicios-de-naciones-unidas-en-espanol.xlsx')
df_ungm = df_ungm.rename(columns={
    'Código segmento': 'cod_segmento',
    'Nombre Segmento': 'nombre_segmento',
    'Código Familia': 'cod_familia',
    'Nombre Familia': 'nombre_familia',
    'Código Clase': 'cod_clase',
    'Nombre Clase': 'nombre_clase',
    'Código Producto': 'cod_producto',
    'Nombre Producto': 'nombre_producto'
})

OUTPUT_FILE = "unspsc_clasificador.csv"
LOCAL_PATH = os.path.join("I:\\Mi unidad\\Python\\chileCompraApp\\data\\", OUTPUT_FILE)
df_ungm.to_csv(LOCAL_PATH, index=False, encoding="utf-8-sig")
fa.upload_file(s3_client, LOCAL_PATH, 'tender-engines', f'raw-data/unspsc/{OUTPUT_FILE}')
os.remove(LOCAL_PATH)





