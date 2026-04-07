import pandas as pd
import boto3, io, time, os, re


def pd_read_s3_csv(bucket, file_name, s3_client, header_flag=True):
    obj = s3_client.get_object(Bucket= bucket, Key= file_name) 
    if header_flag:
        df = pd.read_csv(obj['Body'])
    else:
        df = pd.read_csv(obj['Body'], header=None)
    return df

def pd_read_s3_parquet(bucket, file_name, s3_client=None, **args):
    # Descarga un archivo parquet desde S3 y lo entrega como Dataframe
    if s3_client is None:
        s3_client = boto3.client('s3')
    obj = s3_client.get_object(Bucket=bucket, Key=f'{file_name}')
    return pd.read_parquet(io.BytesIO(obj['Body'].read()), **args)

def create_athena_database(athena_client, database_name, bucket):
    # Crea una base de datos database_name
    query = f"CREATE DATABASE IF NOT EXISTS {database_name};"
    response = athena_client.start_query_execution(
        QueryString=query,
        ResultConfiguration={
            'OutputLocation': f's3://{bucket}/query-results/'
        }
    )

def execute_athena_query(athena_client, query, database_name, bucket, output):
    # Ejecuta una query en la base de datos database_name
    response = athena_client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            'Database': database_name
        },
        ResultConfiguration={
            'OutputLocation': f's3://{bucket}/{output}/'
        }
    )
    queryExecutionId = response['QueryExecutionId']
    status = get_execution_response(athena_client, queryExecutionId)
    contador = 0
    while contador < 100 and status != 'SUCCEEDED':
        time.sleep(2)
        status = get_execution_response(athena_client, queryExecutionId)
        contador = contador + 1
    print(f'Status Execute_athena_query: {status}')    
    return queryExecutionId

def create_s3_folder(s3_client, athena_bucket, nombre_folder):
    # Crea un folder S3 dentro de un bucket específico
    s3_client.put_object(Bucket=athena_bucket, Key=(nombre_folder+'/'))

def create_s3_bucket(s3_client, athena_bucket):
    response = s3_client.create_bucket(
        Bucket=athena_bucket)

def get_execution_response(athena_client, queryExecutionId):
    # Obtiene el estatus de la ejecución de una query
    status = athena_client.get_query_execution(
        QueryExecutionId=queryExecutionId)['QueryExecution']['Status']['State']
    return status

def query_to_dataframe(query, bucket, output, database_name, athena_client, s3_client):
    # Ejecuta una query y entrega el resultado en un Dataframe
    queryExecutionId = execute_athena_query(athena_client, query, database_name, bucket, output)
    try:
        temp_file_location: str = "temp_query_results.csv"
        s3_client.download_file(
            bucket,
            f"{output}/{queryExecutionId}.csv",
            temp_file_location
        )
        df = pd.read_csv(temp_file_location)
        os.remove(temp_file_location)
        return df
    except:
        return print('Error!')

def check_if_file_exists(s3_client, bucket, s3_file):
    # Chequea si archivo s3_file del bucket existe o no
    try:
        s3_client.head_object(Bucket=bucket, Key=s3_file)
        return 'SUCCEEDED'
    except:
        return 'ERROR'

def upload_file(s3_client, local_file, bucket, s3_file, max_retries=3):
    # Sube archivo local_file a un bucket en S3 con el nombre s3_file
    from boto3.s3.transfer import TransferConfig
    config = TransferConfig(
        multipart_threshold=8 * 1024 * 1024,   # 8 MB
        multipart_chunksize=8 * 1024 * 1024,
        max_concurrency=4,
        num_download_attempts=max_retries,
    )
    response = None
    for attempt in range(max_retries):
        try:
            response = s3_client.upload_file(local_file, bucket, s3_file, Config=config)
            break
        except Exception as e:
            if attempt < max_retries - 1:
                wait = 2 ** (attempt + 1)
                print(f'  Upload failed (attempt {attempt+1}/{max_retries}): {e}. Retrying in {wait}s...')
                time.sleep(wait)
            else:
                raise
    time.sleep(2)
    status = check_if_file_exists(s3_client, bucket, s3_file)
    contador = 0
    while contador < 10 and status != 'SUCCEEDED':
        contador = contador + 1
        time.sleep(2)
        status = check_if_file_exists(s3_client, bucket, s3_file)
    print(f'Status Upload archivo {s3_file}: {status}')
    return response

def check_if_table_exists(athena_client, athena_catalog_name, nombre_tabla, db_name):
    # Chequea si existe o no una tabla específica 
    try: 
        response = athena_client.get_table_metadata(
        CatalogName=athena_catalog_name,
        DatabaseName=db_name,
        TableName=nombre_tabla
        )
        response['TableMetadata']['Parameters']['location']
        satus = 'EXISTS'
        return satus
    except:
        satus = 'ERROR'
        return satus


def genera_tablas(athena_client, athena_catalog_name, nombre_tabla, direccion, columnas_tabla_con_tipo, columnas_particion,
                  DB_NAME, athena_bucket, athena_output, iceberg_flag = False):
    # Genera una tabla Athena y una Iceberg a partir de datos en S3. En caso que los datos no hayan sido subidos
    # a S3, realiza el proceso de descarga y posterior upload de los datos a S3.

    status_table = check_if_table_exists(athena_client, athena_catalog_name, nombre_tabla, f'{DB_NAME}')
    if status_table != 'EXISTS':  #Si No existe la tabla, se realiza todo el proceso
        local_file_name = f'{nombre_tabla}.gzip'
        
        # Se crea la tabla en Athena y se Pobla con datos inmediatamente
        print(f'Creando tabla {DB_NAME}.{nombre_tabla} en Athena')
        if columnas_particion == '':
            query_create_athena_table = f"""
            CREATE EXTERNAL TABLE IF NOT EXISTS {DB_NAME}.{nombre_tabla} ( {columnas_tabla_con_tipo}
                )
            STORED AS PARQUET
            LOCATION
            's3://{athena_bucket}/{direccion}/';
            """
        else:
            query_create_athena_table = f"""
            CREATE EXTERNAL TABLE IF NOT EXISTS {DB_NAME}.{nombre_tabla} ( {columnas_tabla_con_tipo}
                )
            PARTITIONED BY (
                {columnas_particion}
            )
            STORED AS PARQUET
            LOCATION
            's3://{athena_bucket}/{direccion}/';
            """

        execute_athena_query(athena_client, query_create_athena_table, f'{DB_NAME}', athena_bucket, athena_output)

    else: #Si la tabla fue creada previamente
        print(f'Tabla {nombre_tabla} ya existe. No se volverá a crear')

    query_repair = f"""
    MSCK REPAIR TABLE {nombre_tabla}
    """
    execute_athena_query(athena_client, query_repair, f'{DB_NAME}', athena_bucket, athena_output)

    if iceberg_flag:
        genera_tabla_iceberg_desde_tabla_athena(athena_client, nombre_tabla, 'iceberg_db', DB_NAME, athena_bucket, athena_output)
        

def genera_tabla_iceberg_desde_tabla_athena(athena_client, nombre_tabla, iceberg_db, db_name, athena_bucket, athena_output):
    query = f"""
    CREATE TABLE {iceberg_db}.{nombre_tabla} WITH (
    table_type = 'ICEBERG',
    is_external = false,
    location = 's3://landengines-data/iceberg/{nombre_tabla}/'
    ) AS
    SELECT * FROM {db_name}.{nombre_tabla}
    """
    queryExecutionId = execute_athena_query(athena_client, query, 'iceberg_db', athena_bucket, athena_output)
    print(f'Creando tabla {iceberg_db}.{nombre_tabla} en Iceberg')

    return queryExecutionId


def extract_unique_folders(s3_client, s3_bucket, s3_dir, pattern=''):
    response = s3_client.list_objects_v2(Bucket=s3_bucket, Prefix=s3_dir, Delimiter='/')
    if not pattern:
        if 'CommonPrefixes' in response:
            subfolders = [prefix['Prefix'] for prefix in response['CommonPrefixes']]
        else:
            print("No subfolders found.")
    else:
        pattern = re.compile(pattern)
        # Check for common prefixes (subfolders)
        if 'CommonPrefixes' in response:
            subfolders_ = [prefix['Prefix'] for prefix in response['CommonPrefixes']]
            # Extract the year using the regex pattern
            subfolders = [pattern.search(folder).group(1) for folder in subfolders_ if pattern.search(folder)]
        else:
            print("No subfolders found.")
    return subfolders