import os
import io
import zipfile
from dotenv import load_dotenv
import pandas as pd
import boto3
from botocore.config import Config
import funcionesAws as fa
import funcionesNeo4jEC2 as fne


def force_int_columns(df):
    for col in df.select_dtypes(include='number').columns:
        if df[col].dropna().apply(lambda x: x == int(x)).all():
            df[col] = df[col].fillna(0).astype(int)
    for col in df.select_dtypes(include='object').columns:
        df[col] = df[col].str.replace(r'[\r\n]+', ' ', regex=True).str.strip()
    return df


fileDir = "I:\\Mi unidad\\Python\\chileCompraApp\\data\\"

load_dotenv("secrets.env")

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

DATA_DIR = fileDir
S3_BUCKET = "tender-engines"

#######################################################################################
# Merge productos convenio marco and upload to S3
#######################################################################################

print("=== Merging transacciones convenio marco ZIPs from S3 ===\n")
S3_PREFIX = "raw-data/convenio_marco/"
dfs = []
response = s3_client.list_objects_v2(Bucket=S3_BUCKET, Prefix=S3_PREFIX, Delimiter='/')
for obj in sorted(response.get('Contents', []), key=lambda x: x['Key']):
    s3_key = obj['Key']
    if not s3_key.endswith(".zip"):
        continue
    zip_name = s3_key.split('/')[-1]
    print(f"  Downloading {s3_key}...")
    zip_obj = s3_client.get_object(Bucket=S3_BUCKET, Key=s3_key)
    zip_bytes = io.BytesIO(zip_obj['Body'].read())
    with zipfile.ZipFile(zip_bytes) as zf:
        for csv_name in zf.namelist():
            if not csv_name.endswith(".csv"):
                continue
            with zf.open(csv_name) as f:
                try:
                    df = pd.read_csv(f, encoding="utf-8-sig", sep=",", on_bad_lines="skip")
                except UnicodeDecodeError:
                    f.seek(0)
                    df = pd.read_csv(f, encoding="latin-1", sep=",", on_bad_lines="skip")
            df["source_zip"] = zip_name
            df["source_csv"] = csv_name
            dfs.append(df)
            print(f"  {zip_name}/{csv_name}: {len(df)} rows")

df_cm_actuales = pd.concat(dfs, ignore_index=True)
df_producto_cod_onu = (
    df_cm_actuales[['ID PRODUCTO', 'CÓDIGO ONU']]
    .drop_duplicates()
    .rename(columns={'ID PRODUCTO': 'producto_detallado_id', 'CÓDIGO ONU': 'codigo_onu'})
    .dropna()
    .reset_index(drop=True)
)
print(f"Distinct rel_producto_codigo_onu: {len(df_producto_cod_onu)}")
OUTPUT_FILE = "rel_producto_codigo_onu.csv"
df_convenio_marco = force_int_columns(df_producto_cod_onu)
df_convenio_marco.to_csv(f"data\\{OUTPUT_FILE}", index=False, encoding="utf-8-sig", sep="|")
s3_kg_key = f"kg/{OUTPUT_FILE}"
print(f"\n=== Uploading to s3://{S3_BUCKET}/{s3_kg_key} ===\n")
fa.upload_file(s3_client, f"data\\{OUTPUT_FILE}", S3_BUCKET, s3_kg_key)


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
# a=1


print("=== Merging transacciones convenio marco ZIPs from S3 ===\n")
S3_PREFIX = "raw-data/convenio_marco/transacciones/"
dfs = []
response = s3_client.list_objects_v2(Bucket=S3_BUCKET, Prefix=S3_PREFIX)
for obj in sorted(response.get('Contents', []), key=lambda x: x['Key']):
    s3_key = obj['Key']
    if not s3_key.endswith(".zip"):
        continue
    zip_name = s3_key.split('/')[-1]
    print(f"  Downloading {s3_key}...")
    zip_obj = s3_client.get_object(Bucket=S3_BUCKET, Key=s3_key)
    zip_bytes = io.BytesIO(zip_obj['Body'].read())
    with zipfile.ZipFile(zip_bytes) as zf:
        for csv_name in zf.namelist():
            if not csv_name.endswith(".csv"):
                continue
            with zf.open(csv_name) as f:
                try:
                    df = pd.read_csv(f, encoding="utf-8-sig", sep=";", on_bad_lines="skip", low_memory=False)
                except UnicodeDecodeError:
                    f.seek(0)
                    df = pd.read_csv(f, encoding="latin-1", sep=";", on_bad_lines="skip", low_memory=False)
            df["source_zip"] = zip_name
            df["source_csv"] = csv_name
            dfs.append(df)
            print(f"  {zip_name}/{csv_name}: {len(df)} rows")

df_transacciones = pd.concat(dfs, ignore_index=True)

# Remove all double quotes from string columns
str_cols = df_transacciones.select_dtypes(include='object').columns
df_transacciones[str_cols] = df_transacciones[str_cols].apply(lambda col: col.str.replace('"', '', regex=False))

df_transacciones = (
    df_transacciones
    .drop_duplicates()
    .rename(columns={'Nro Licitacion Publica': 'licitacion_id', 'Id Convenio Marco': 'convenio_marco_id', 'Convenio Marco': 'nombre_convenio_marco', 
                     'CodigoOC': 'id_oc', 'NombreOC': 'nombre_oc', 'Fecha Envio OC': 'fecha_envio_oc', 'EstadoOC': 'estado_oc',
                     'Proviene de Gran Compra': 'gran_compra_flag', 'idGranCompra': 'id_gran_compra', 'Especificacion del Comprador': 'especificacion_comprador',
                        'IDProductoCM': 'producto_detallado_id', 'Producto': 'nombre_producto', 'Nombre Producto ONU': 'nombre_producto_onu',
                        'Tipo de Producto': 'tipo_producto', 'Marca': 'marca', 'Modelo': 'modelo', 'Precio Unitario': 'precio_unitario',
                        'Cantidad': 'cantidad', 'TotaLinea(Neto)': 'total_linea_neto', 'Moneda': 'moneda', 'Monto Total OC Neto': 'monto_total_oc_neto',
                        'Descuento Global OC': 'descuento_global_oc', 'Cargos Adicionales OC': 'cargos_adicionales_oc', 'Subtotal OC': 'subtotal_oc',
                        'Impuestos': 'impuestos', 'Monto Total OC': 'monto_total_oc', 'Rut Unidad de Compra': 'rut_unidad_compra',
                        'Unidad de Compra': 'unidad_compra', 'Razon Social Comprador': 'razon_social_comprador', 'Direccion Unidad Compra': 'direccion_unidad_compra',
                        'Comuna Unidad Compra': 'comuna_unidad_compra', 'Region Unidad de Compra': 'region_unidad_compra', 'Institucion': 'institucion',
                        'Sector': 'sector', 'Rut Proveedor': 'rut_proveedor', 'Nombre Proveedor Sucursal': 'nombre_proveedor_sucursal',
                        'Nombre Empresa': 'nombre_empresa', 'Comuna del Proveedor': 'comuna_proveedor', 'Region del Proveedor': 'region_proveedor',
                        'Observaciones': 'observaciones', 'Forma de Pago': 'forma_pago', 'Orgcode_Comprador': 'orgcode_comprador'})
    .reset_index(drop=True)
)


df_convenio_marco = (
    df_transacciones[['convenio_marco_id', 'nombre_convenio_marco', 'licitacion_id']]
    .drop_duplicates()
    .dropna()
    .reset_index(drop=True)
)
df_convenio_marco['convenio_marco_id'] = df_convenio_marco['convenio_marco_id'].astype(int)
print(f"Distinct convenios marco: {len(df_convenio_marco)}")
OUTPUT_FILE = "convenios_marco.csv"
df_convenio_marco = force_int_columns(df_convenio_marco)
df_convenio_marco.to_csv(f"data\\{OUTPUT_FILE}", index=False, encoding="utf-8-sig", sep="|")
s3_kg_key = f"kg/{OUTPUT_FILE}"
print(f"\n=== Uploading to s3://{S3_BUCKET}/{s3_kg_key} ===\n")
fa.upload_file(s3_client, f"data\\{OUTPUT_FILE}", S3_BUCKET, s3_kg_key)


df_oc = (
    df_transacciones[['id_oc', 'nombre_oc', 'fecha_envio_oc', 'gran_compra_flag', 'id_gran_compra', 'estado_oc', 'moneda', 'monto_total_oc_neto', 
                      'descuento_global_oc','cargos_adicionales_oc','subtotal_oc', 'impuestos','monto_total_oc', 
                      'rut_unidad_compra', 'rut_proveedor','forma_pago']]
    .drop_duplicates()
    .reset_index(drop=True)
)
print(f"Distinct oc: {len(df_oc)}")
OUTPUT_FILE = "ordenes_compra.csv"
df_oc = force_int_columns(df_oc)
df_oc.to_csv(f"data\\{OUTPUT_FILE}", index=False, encoding="utf-8-sig", sep="|")
s3_kg_key = f"kg/{OUTPUT_FILE}"
print(f"\n=== Uploading to s3://{S3_BUCKET}/{s3_kg_key} ===\n")
fa.upload_file(s3_client, f"data\\{OUTPUT_FILE}", S3_BUCKET, s3_kg_key)


df_linea_transaccion = (
    df_transacciones[['id_oc', 'producto_detallado_id', 'cantidad', 'precio_unitario', 'total_linea_neto']]
    .drop_duplicates()
    .reset_index(drop=True)
)
df_linea_transaccion['precio_unitario'] = df_linea_transaccion['precio_unitario'].astype(str).str.replace(',', '.', regex=False).astype(float).astype(int)
df_linea_transaccion['total_linea_neto'] = df_linea_transaccion['total_linea_neto'].astype(str).str.replace(',', '.', regex=False).astype(float).astype(int)
mask = df_linea_transaccion['cantidad'].isna() & (df_linea_transaccion['precio_unitario'] != 0)
df_linea_transaccion.loc[mask, 'cantidad'] = (df_linea_transaccion.loc[mask, 'total_linea_neto'] / df_linea_transaccion.loc[mask, 'precio_unitario']).astype(int)
df_linea_transaccion['cantidad'] = df_linea_transaccion['cantidad'].astype(str).str.replace(',', '.', regex=False).astype(float).fillna(0).astype(int)
df_linea_transaccion['id_linea_transaccion'] = df_linea_transaccion.groupby('id_oc').cumcount().add(1).astype(str)
df_linea_transaccion['id_linea_transaccion'] = df_linea_transaccion['id_oc'].astype(str) + '_' + df_linea_transaccion['id_linea_transaccion']
print(f"Distinct linea transaccion: {len(df_linea_transaccion)}")
OUTPUT_FILE = "lineas_transaccion.csv"
df_linea_transaccion = force_int_columns(df_linea_transaccion)
df_linea_transaccion.to_csv(f"data\\{OUTPUT_FILE}", index=False, encoding="utf-8-sig", sep="|")
s3_kg_key = f"kg/{OUTPUT_FILE}"
print(f"\n=== Uploading to s3://{S3_BUCKET}/{s3_kg_key} ===\n")
fa.upload_file(s3_client, f"data\\{OUTPUT_FILE}", S3_BUCKET, s3_kg_key)


df_producto = (
    df_transacciones[['producto_detallado_id', 'convenio_marco_id', 'nombre_producto', 'nombre_producto_onu', 'tipo_producto', 'marca', 'modelo', 'especificacion_comprador']]
    .drop_duplicates()
    .reset_index(drop=True)
)
print(f"Distinct producto: {len(df_producto)}")
OUTPUT_FILE = "productos.csv"
df_producto = force_int_columns(df_producto)
df_producto.to_csv(f"data\\{OUTPUT_FILE}", index=False, encoding="utf-8-sig", sep="|")
s3_kg_key = f"kg/{OUTPUT_FILE}"
print(f"\n=== Uploading to s3://{S3_BUCKET}/{s3_kg_key} ===\n")
fa.upload_file(s3_client, f"data\\{OUTPUT_FILE}", S3_BUCKET, s3_kg_key)


df_proveedor = (
    df_transacciones[['rut_proveedor', 'nombre_proveedor_sucursal', 'nombre_empresa', 'comuna_proveedor', 'region_proveedor']]
    .drop_duplicates()
    .reset_index(drop=True)
)
print(f"Distinct proveedor: {len(df_proveedor)}")
OUTPUT_FILE = "proveedores.csv"
df_proveedor = force_int_columns(df_proveedor)
df_proveedor.to_csv(f"data\\{OUTPUT_FILE}", index=False, encoding="utf-8-sig", sep="|")
s3_kg_key = f"kg/{OUTPUT_FILE}"
print(f"\n=== Uploading to s3://{S3_BUCKET}/{s3_kg_key} ===\n")
fa.upload_file(s3_client, f"data\\{OUTPUT_FILE}", S3_BUCKET, s3_kg_key)


df_producto_proveedor = (
    df_transacciones[['producto_detallado_id', 'rut_proveedor']]
    .drop_duplicates()
    .reset_index(drop=True)
)
print(f"Distinct proveedor: {len(df_proveedor)}")
OUTPUT_FILE = "rel_producto_proveedor.csv"
df_proveedor_producto = force_int_columns(df_producto_proveedor)
df_proveedor_producto.to_csv(f"data\\{OUTPUT_FILE}", index=False, encoding="utf-8-sig", sep="|")
s3_kg_key = f"kg/{OUTPUT_FILE}"
print(f"\n=== Uploading to s3://{S3_BUCKET}/{s3_kg_key} ===\n")
fa.upload_file(s3_client, f"data\\{OUTPUT_FILE}", S3_BUCKET, s3_kg_key)



df_comprador = (
    df_transacciones[['rut_unidad_compra', 'unidad_compra', 'comuna_unidad_compra', 'region_unidad_compra', 'institucion', 'sector', 'orgcode_comprador']]
    .drop_duplicates()
    .reset_index(drop=True)
)
print(f"Distinct comprador: {len(df_comprador)}")
OUTPUT_FILE = "compradores.csv"
df_comprador = force_int_columns(df_comprador)
df_comprador.to_csv(f"data\\{OUTPUT_FILE}", index=False, encoding="utf-8-sig", sep="|")
s3_kg_key = f"kg/{OUTPUT_FILE}"
print(f"\n=== Uploading to s3://{S3_BUCKET}/{s3_kg_key} ===\n")
fa.upload_file(s3_client, f"data\\{OUTPUT_FILE}", S3_BUCKET, s3_kg_key)


df_ungm = fa.pd_read_s3_csv('tender-engines', 'raw-data/unspsc/unspsc_clasificador.csv', s3_client)

# Remove all double quotes from string columns
str_cols = df_ungm.select_dtypes(include='object').columns
df_ungm[str_cols] = df_ungm[str_cols].apply(lambda col: col.str.replace('"', '', regex=False))

df_segmento = (
    df_ungm[['cod_segmento', 'nombre_segmento']]
    .drop_duplicates()
    .reset_index(drop=True)
)
print(f"Distinct segmentos: {len(df_segmento)}")
OUTPUT_FILE = "segmentos_ungm.csv"
df_segmento = force_int_columns(df_segmento)
df_segmento.to_csv(f"data\\{OUTPUT_FILE}", index=False, encoding="utf-8-sig", sep="|")
s3_kg_key = f"kg/{OUTPUT_FILE}"
print(f"\n=== Uploading to s3://{S3_BUCKET}/{s3_kg_key} ===\n")
fa.upload_file(s3_client, f"data\\{OUTPUT_FILE}", S3_BUCKET, s3_kg_key)

df_familia = (
    df_ungm[['cod_familia', 'nombre_familia']]
    .drop_duplicates()
    .reset_index(drop=True)
)
print(f"Distinct familias: {len(df_familia)}")
OUTPUT_FILE = "familias_ungm.csv"
df_familia = force_int_columns(df_familia)
df_familia.to_csv(f"data\\{OUTPUT_FILE}", index=False, encoding="utf-8-sig", sep="|")
s3_kg_key = f"kg/{OUTPUT_FILE}"
print(f"\n=== Uploading to s3://{S3_BUCKET}/{s3_kg_key} ===\n")
fa.upload_file(s3_client, f"data\\{OUTPUT_FILE}", S3_BUCKET, s3_kg_key)

df_clase = (
    df_ungm[['cod_clase', 'nombre_clase']]
    .drop_duplicates()
    .reset_index(drop=True)
)
print(f"Distinct clases: {len(df_clase)}")
OUTPUT_FILE = "clases_ungm.csv"
df_clase = force_int_columns(df_clase)
df_clase.to_csv(f"data\\{OUTPUT_FILE}", index=False, encoding="utf-8-sig", sep="|")
s3_kg_key = f"kg/{OUTPUT_FILE}"
print(f"\n=== Uploading to s3://{S3_BUCKET}/{s3_kg_key} ===\n")
fa.upload_file(s3_client, f"data\\{OUTPUT_FILE}", S3_BUCKET, s3_kg_key)

df_producto = (
    df_ungm[['cod_producto', 'nombre_producto']]
    .drop_duplicates()
    .reset_index(drop=True)
)
print(f"Distinct productos: {len(df_producto)}")
OUTPUT_FILE = "productos_ungm.csv"
df_producto = force_int_columns(df_producto)
df_producto.to_csv(f"data\\{OUTPUT_FILE}", index=False, encoding="utf-8-sig", sep="|")
s3_kg_key = f"kg/{OUTPUT_FILE}"
print(f"\n=== Uploading to s3://{S3_BUCKET}/{s3_kg_key} ===\n")
fa.upload_file(s3_client, f"data\\{OUTPUT_FILE}", S3_BUCKET, s3_kg_key)

a=1



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



