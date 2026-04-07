import pandas as pd
import funcionesAws as fa
import funcionesNeo4j as fn


def save_and_upload_csv(df, file_name, file_config, aws_config):
    """Save DataFrame to CSV and upload to S3"""
    fileDir_full = file_config['local_dir'] + file_name
    df.to_csv(fileDir_full, index=False, sep="|")
    s3_file_name = f"{file_config['s3_prefix']}{file_name}"
    fa.upload_file(aws_config['s3_client'], fileDir_full, file_config['s3_bucket'], s3_file_name)


def query_process_save(query, file_name, aws_config, file_config, process_fn=None):
    """Execute query, optionally process DataFrame, save and upload

    Args:
        query: SQL query string
        file_name: Output CSV file name
        aws_config: Dict with keys: athena_client, s3_client, athena_bucket, athena_output, db_name
        file_config: Dict with keys: local_dir, s3_bucket, s3_prefix
        process_fn: Optional function to process DataFrame before saving
    """
    df = fa.query_to_dataframe(
        query,
        aws_config['athena_bucket'],
        aws_config['athena_output'],
        aws_config['db_name'],
        aws_config['athena_client'],
        aws_config['s3_client']
    )
    if df is None:
        raise ValueError(f"Query failed to return data for file: {file_name}. Check the error message above.")
    if process_fn:
        df = process_fn(df)
    save_and_upload_csv(df, file_name, file_config, aws_config)
    return df

def neo4j_process_save(query, file_name, neo4j_config, file_config, aws_config, process_fn=None):
    """Execute Neo4j Cypher query, optionally process DataFrame, save and upload

    Args:
        query: Cypher query string
        file_name: Output CSV file name
        neo4j_config: Dict with key: conn_neo4j (Neo4jConnection object)
        file_config: Dict with keys: local_dir, s3_bucket, s3_prefix
        aws_config: Dict with key: s3_client (needed for S3 upload)
        process_fn: Optional function to process DataFrame before saving
    """
    df = fn.neo4jToDataframe(query, neo4j_config['conn_neo4j'])
    if df is None or df.empty:
        raise ValueError(f"Neo4j query failed to return data for file: {file_name}.")
    if process_fn:
        df = process_fn(df)
    save_and_upload_csv(df, file_name, file_config, aws_config)
    return df

def process_by_comunas(comunas, query_fn, file_name, aws_config, file_config, process_fn=None):
    """Process data by iterating through comunas"""
    dfs = []
    for c in comunas:
        print(f'Procesando comuna: {c}')
        query = query_fn(c)
        df_c = fa.query_to_dataframe(
            query,
            aws_config['athena_bucket'],
            aws_config['athena_output'],
            aws_config['db_name'],
            aws_config['athena_client'],
            aws_config['s3_client']
        )
        if df_c is None:
            raise ValueError(f"Query failed to return data for comuna: {c}, file: {file_name}. Check the error message above.")
        if process_fn:
            df_c = process_fn(df_c)
        dfs.append(df_c)

    df_final = pd.concat(dfs, ignore_index=True)
    save_and_upload_csv(df_final, file_name, file_config, aws_config)
    return df_final


def process_entrada_relation(table_name, fecha_var, file_prefix, node_suffix, aws_config, file_config):
    """Process entrada relations (ini and ter) for POI, subway, bus_stop, predio"""
    for node_type in ['ini', 'ter']:
        print(f'Genera CSV Relacion {file_prefix} -> Nodo_Calle {node_type.capitalize()}')
        id_col = 'osmid' if table_name != 'entrada_predio' else 'codigo_predial'
        query = f"""
        SELECT {id_col}, id_nodo_{node_type}, dist_nodo_{node_type}
        FROM {table_name}
        WHERE fecha = '{fecha_var}'
        ORDER BY {id_col}
        """
        query_process_save(query, f"rel_{node_suffix}_to_nodo_calle_{node_type}.csv", aws_config, file_config)
