import os
import zipfile
from dotenv import load_dotenv
import pandas as pd
import boto3
from botocore.config import Config
import funcionesAws as fa
import funcionesNeo4jEC2 as fne
import funcionesUploadNeo4j as fun


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



#######################################################################################
# Merge productos convenio marco and upload to S3
#######################################################################################

DATA_DIR = fileDir
S3_BUCKET = "tender-engines"
S3_PREFIX = "raw-data/convenio_marco/"

# # ── 1. Upload all ZIPs to S3 ─────────────────────────────────────────────────

# print(f"\n=== Uploading ZIPs to s3://{S3_BUCKET}/{S3_PREFIX} ===\n")

# for zip_name in sorted(os.listdir(DATA_DIR)):
#     if not zip_name.endswith(".zip"):
#         continue
#     local_path = os.path.join(DATA_DIR, zip_name)
#     s3_key = f"{S3_PREFIX}{zip_name}"
#     if fa.check_if_file_exists(s3_client, S3_BUCKET, s3_key) == 'SUCCEEDED':
#         print(f"  Skipping {zip_name} (already in S3)")
#         continue
#     fa.upload_file(s3_client, local_path, S3_BUCKET, s3_key)


# ── 2. Merge all CSVs from ZIP files ─────────────────────────────────────────

print("=== Merging producto convenio marco ZIPs ===\n")

dfs = []
for zip_name in sorted(os.listdir(DATA_DIR)):
    if not zip_name.endswith(".zip"):
        continue
    zip_path = os.path.join(DATA_DIR, zip_name)
    with zipfile.ZipFile(zip_path) as zf:
        for csv_name in zf.namelist():
            if not csv_name.endswith(".csv"):
                continue
            with zf.open(csv_name) as f:
                df = pd.read_csv(f, encoding="utf-8-sig", on_bad_lines="skip")
            df["source_zip"] = zip_name
            df["source_csv"] = csv_name
            dfs.append(df)
            print(f"  {zip_name}/{csv_name}: {len(df)} rows")

df_merged = pd.concat(dfs, ignore_index=True)


df_convenios = (
    df_merged[['ID CONVENIO MARCO', 'NÚMERO LICITACIÓN']]
    .dropna(subset=['ID CONVENIO MARCO'])
    .drop_duplicates()
    .rename(columns={'ID CONVENIO MARCO': 'convenio_marco_id', 'NÚMERO LICITACIÓN': 'licitacion_id'})
    .reset_index(drop=True)
)
print(f"Distinct convenios: {len(df_convenios)}")

df_proveedores = (
    df_merged[['ID PROVEEDOR', 'NOMBRE PROVEEDOR', 'RUT PROVEEDOR']]
    .drop_duplicates()
    .rename(columns={'ID PROVEEDOR': 'proveedor_id', 'NOMBRE PROVEEDOR': 'nombre_proveedor', 'RUT PROVEEDOR': 'rut_proveedor'})
    .reset_index(drop=True)
)
print(f"Distinct proveedores: {len(df_proveedores)}")

df_productos = (
    df_merged[['ID PRODUCTO', 'CÓDIGO ONU', 'PRODUCTO', 'ID TIPO PRODUCTO', 'TIPO PRODUCTO',
               'REGIÓN', 'MARCA', 'MODELO', 'MEDIDA']]
    .drop_duplicates()
    .rename(columns={
        'ID PRODUCTO': 'producto_id', 'CÓDIGO ONU': 'codigo_onu', 'PRODUCTO': 'nombre_producto',
        'ID TIPO PRODUCTO': 'tipo_producto_id', 'TIPO PRODUCTO': 'tipo_producto',
        'REGIÓN': 'region', 'MARCA': 'marca', 'MODELO': 'modelo', 'MEDIDA': 'medida'
    })
    .reset_index(drop=True)
)
print(f"Distinct productos: {len(df_productos)}")

df_convenio_producto = (
    df_merged[['ID CONVENIO MARCO', 'ID PRODUCTO']]
    .drop_duplicates()
    .rename(columns={'ID CONVENIO MARCO': 'convenio_marco_id', 'ID PRODUCTO': 'producto_id'})
    .reset_index(drop=True)
)
print(f"Distinct convenio-producto: {len(df_convenio_producto)}")

df_proveedor_producto = (
    df_merged[['ID PROVEEDOR', 'ID PRODUCTO', 'STOCK', 'PRECIO EN TIENDA', 'PRECIO REGULAR']]
    .drop_duplicates()
    .rename(columns={'ID PROVEEDOR': 'proveedor_id', 'ID PRODUCTO': 'producto_id', 'STOCK': 'stock_flag', 'PRECIO EN TIENDA': 'precio_tienda',
        'PRECIO REGULAR': 'precio_regular'})
    .reset_index(drop=True)
)
print(f"Distinct proveedor-producto: {len(df_proveedor_producto)}")


OUTPUT_FILE = "convenios_marco.csv"
df_convenios.to_csv(f"data\\{OUTPUT_FILE}", index=False, encoding="utf-8-sig")
s3_kg_key = f"kg/{OUTPUT_FILE}"
print(f"\n=== Uploading to s3://{S3_BUCKET}/{s3_kg_key} ===\n")
fa.upload_file(s3_client, f"data\\{OUTPUT_FILE}", S3_BUCKET, s3_kg_key)

OUTPUT_FILE = "proveedores_convenios_marco.csv"
df_proveedores.to_csv(f"data\\{OUTPUT_FILE}", index=False, encoding="utf-8-sig")
s3_kg_key = f"kg/{OUTPUT_FILE}"
print(f"\n=== Uploading to s3://{S3_BUCKET}/{s3_kg_key} ===\n")
fa.upload_file(s3_client, f"data\\{OUTPUT_FILE}", S3_BUCKET, s3_kg_key)

OUTPUT_FILE = "productos_convenios_marco.csv"
df_productos.to_csv(f"data\\{OUTPUT_FILE}", index=False, encoding="utf-8-sig")
s3_kg_key = f"kg/{OUTPUT_FILE}"
print(f"\n=== Uploading to s3://{S3_BUCKET}/{s3_kg_key} ===\n")
fa.upload_file(s3_client, f"data\\{OUTPUT_FILE}", S3_BUCKET, s3_kg_key)

OUTPUT_FILE = "rel_convenios_productos.csv"
df_convenio_producto.to_csv(f"data\\{OUTPUT_FILE}", index=False, encoding="utf-8-sig")
s3_kg_key = f"kg/{OUTPUT_FILE}"
print(f"\n=== Uploading to s3://{S3_BUCKET}/{s3_kg_key} ===\n")
fa.upload_file(s3_client, f"data\\{OUTPUT_FILE}", S3_BUCKET, s3_kg_key)

OUTPUT_FILE = "rel_proveedores_productos.csv"
df_proveedor_producto.to_csv(f"data\\{OUTPUT_FILE}", index=False, encoding="utf-8-sig")
s3_kg_key = f"kg/{OUTPUT_FILE}"
print(f"\n=== Uploading to s3://{S3_BUCKET}/{s3_kg_key} ===\n")
fa.upload_file(s3_client, f"data\\{OUTPUT_FILE}", S3_BUCKET, s3_kg_key)


# ── 3. Uploading to EC2 ─────────────────────────────────────────

print("=== Uploading to EC2 ===\n")


NEO3J_IMPORT_DIR = '/var/lib/neo4j/import'

REGION      = 'us-east-1'
TAG_KEY     = 'Name'
TAG_VALUE   = INSTANCIA_EC2
KEY_PATH    = 'neo4j-key-pair.pem'
SSH_USER    = 'ec2-user'
SSH_PORT    = 22
S3_BUCKET   = 'tender-engines'
S3_PREFIX   = 'kg/'
SPECIFIC_S3_KEY = None
TARGET_FILES  = []  # Empty list: delete all files and import all from S3

instance_id, public_ip, state = fne.find_instance_by_name(ec2_client, INSTANCIA_EC2)

# transfer_csvs_to_neo4j_import now handles deletion internally
fne.transfer_csvs_to_neo4j_import(
    public_ip, s3_client, S3_BUCKET, S3_PREFIX, SPECIFIC_S3_KEY, TARGET_FILES, SSH_USER, SSH_PORT, KEY_PATH, NEO3J_IMPORT_DIR)



