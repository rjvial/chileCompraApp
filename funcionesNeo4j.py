from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable, SessionExpired
import pandas as pd
import json
import time

class Neo4jConnection:
    def __init__(
        self,
        uri: str,
        user: str,
        pwd: str,
        trusted_certificates=None,
        **driver_kwargs
    ):
        """
        :param uri:          Connection URI. Use neo4j+s:// for secure, neo4j:// for unsecured, bolt+s:// for secure bolt, bolt:// for unsecured bolt
        :param user:         username
        :param pwd:          password
        :param trusted_certificates: Path to certificate file or None to use system CAs (neo4j 6.x)
        :param driver_kwargs: any other kwargs you want to pass to GraphDatabase.driver()

        Note: In neo4j driver 6.x, encryption is controlled by the URI scheme:
        - neo4j+s:// or bolt+s:// = encrypted with system CAs
        - neo4j+ssc:// or bolt+ssc:// = encrypted, self-signed certificates allowed
        - neo4j:// or bolt:// = unencrypted
        """
        self.__uri = uri
        self.__user = user
        self.__pwd = pwd

        opts = {}

        # In neo4j 6.x, trusted_certificates can be used to specify custom CA certificates
        if trusted_certificates is not None:
            opts["trusted_certificates"] = trusted_certificates

        opts.update(driver_kwargs)

        # Add connection pool settings to handle timeouts better
        if "max_connection_lifetime" not in opts:
            opts["max_connection_lifetime"] = 3600  # 1 hour
        if "max_connection_pool_size" not in opts:
            opts["max_connection_pool_size"] = 50
        if "connection_acquisition_timeout" not in opts:
            opts["connection_acquisition_timeout"] = 60  # seconds

        try:
            self.__driver = GraphDatabase.driver(
                self.__uri,
                auth=(self.__user, self.__pwd),
                **opts
            )
        except Exception as e:
            raise RuntimeError(f"Failed to create Neo4j driver: {e}") from e

    def close(self):
        if self.__driver is not None:
            self.__driver.close()

    def query(self, cypher: str, db: str | None = None, max_retries: int = 3):
        """
        Execute a Cypher query and return a list of neo4j.Record objects.
        Includes retry logic for transient connection failures.
        """
        assert self.__driver is not None, "Driver not initialized!"

        last_exception = None
        for attempt in range(max_retries):
            session = None
            try:
                # Verify connectivity before creating session
                self.__driver.verify_connectivity()

                session = self.__driver.session(database=db) if db else self.__driver.session()
                return list(session.run(cypher))
            except (ServiceUnavailable, SessionExpired, OSError) as e:
                last_exception = e
                if session:
                    session.close()

                if attempt < max_retries - 1:
                    wait_time = 2 ** attempt  # Exponential backoff: 1s, 2s, 4s
                    print(f"Connection failed (attempt {attempt + 1}/{max_retries}). Retrying in {wait_time}s...")
                    time.sleep(wait_time)
                else:
                    print(f"All {max_retries} connection attempts failed.")
                    raise
            except Exception as e:
                # For non-connection errors, fail immediately
                if session:
                    session.close()
                raise
            finally:
                if session:
                    session.close()

        # This should never be reached, but just in case
        raise last_exception if last_exception else RuntimeError("Query failed for unknown reason")

def neo4jToDataframe(query: str, conn_neo4j):
    """
    Run `query` on `conn_neo4j` and return the results as a pandas DataFrame.
    """
    records = conn_neo4j.query(query)
    if not records:
        return pd.DataFrame()
    
    cols = records[0].keys()
    data = [{col: record.get(col) for col in cols} for record in records]
    return pd.DataFrame(data)


def neo4jToJson(query: str, conn_neo4j):
    """
    Run `query` on `conn_neo4j` and return the results as JSON.
    """
    records = conn_neo4j.query(query)
    if not records:
        return json.dumps([])
    
    cols = records[0].keys()
    data = [{col: record.get(col) for col in cols} for record in records]
    return json.dumps(data, indent=2, default=str)

def neo4jToDict(query: str, conn_neo4j, group_var):
    """
    Run `query` on `conn_neo4j` and return the results grouped by group_var.
    """
    records = conn_neo4j.query(query)
    if not records:
        return {}
    
    cols = records[0].keys()
    grouped = {}
    
    for record in records:
        item = {col: record.get(col) for col in cols}
        group_variable = item.get(group_var)
        
        if group_variable not in grouped:
            grouped[group_variable] = []
        
        grouped[group_variable].append(item)
    
    return grouped


def neo4jToNestedDict(
    query: str,
    conn_neo4j,
    grouping_keys: list  # e.g. ["nombre_variante", "nombre_requerimiento"]
    ) -> dict:
    """
    Run `query` on `conn_neo4j` and return a nested dict whose levels
    follow the order of `grouping_keys`, with all nested Node-like
    objects converted into plain dicts.
    """
    records = conn_neo4j.query(query)
    if not records:
        return {}

    def clean(value):
        # Recursively unwrap lists, dicts, or Node-like objects (_properties)
        if isinstance(value, list):
            return [clean(v) for v in value]
        if isinstance(value, dict):
            return {k: clean(v) for k, v in value.items()}
        if hasattr(value, "_properties"):
            return clean(value._properties)
        return value

    grouped: dict = {}
    last_group_key = grouping_keys[-1]

    for record in records:
        # Build a cleaned row of all columns
        row = {}
        for col in record.keys():
            val = clean(record.get(col))

            # If it's a list of dicts keyed by the innermost grouping key,
            # build a lookup or unwrap singletons.
            if (
                isinstance(val, list)
                and val
                and all(isinstance(item, dict) and last_group_key in item for item in val)
            ):
                lookup = {item[last_group_key]: item for item in val}
                row[col] = next(iter(lookup.values())) if len(lookup) == 1 else lookup
            else:
                row[col] = val

        # Drill down into grouped according to grouping_keys
        current_level = grouped
        for key in grouping_keys[:-1]:
            key_val = record.get(key)
            current_level = current_level.setdefault(key_val, {})

        final_key = record.get(last_group_key)
        current_level[final_key] = row

    return grouped



def neo4jQuery(query: str, conn_neo4j):
    """
    Execute a Cypher query on `conn_neo4j` without converting to DataFrame,
    returning the raw list of neo4j.Record objects.
    """
    return conn_neo4j.query(query)


def generate_cypher_query(
    df,
    node_label,
    id_column,
    file_placeholder=':file',
    batch_size=500,
    parallel=False,
    location_columns=None,
    exclude_columns=None,
    column_mapping=None,
    id_column_neo4j=None,
    separator='|'
):
    """
    Generate a Cypher MERGE query with automatic type detection from DataFrame.

    Args:
        df: pandas DataFrame with the data (used for type inference)
        node_label: Neo4j node label (e.g., 'Cabida', 'Edificacion_Colectiva')
        id_column: Column in CSV to use as unique identifier in MERGE
        file_placeholder: Placeholder for CSV file path (default: ':file')
        batch_size: Batch size for apoc.periodic.iterate (default: 500)
        parallel: Whether to run in parallel mode (default: False)
        location_columns: Dict for point() generation, e.g., {'latitud': 'latitude', 'longitud': 'longitude'}
                         Will generate: a.location = point({latitude: toFloat(row.latitud), longitude: toFloat(row.longitud)})
        exclude_columns: List of columns to exclude from SET clause
        column_mapping: Dict to rename columns, e.g., {'n_dorm': 'num_dorm', 'n_banos': 'num_banos'}
                       Maps CSV column name -> Neo4j property name
        id_column_neo4j: Neo4j property name for id if different from CSV column (e.g., 'id_ei' when CSV has 'id_edificacion')

    Returns:
        str: Complete Cypher query string

    Example:
        query = generate_cypher_query(
            df=df_edificios,
            node_label='Edificacion_Colectiva',
            id_column='id_ec',
            location_columns={'latitud': 'latitude', 'longitud': 'longitude'},
            exclude_columns=['temp_column'],
            column_mapping={'n_dorm': 'num_dorm'}
        )
        query = query.replace(':file', 'file:///edificacion_colectiva.csv')
        fn.neo4jQuery(query, conn_neo4j)
    """
    import pandas as pd

    exclude_columns = exclude_columns or []
    column_mapping = column_mapping or {}

    def get_cypher_conversion(column_name, df):
        dtype_str = str(df[column_name].dtype)

        if dtype_str in ['int64', 'int32', 'int16', 'int8', 'Int64', 'Int32', 'Int16', 'Int8']:
            return f"toInteger(row.{column_name})"
        elif dtype_str in ['float64', 'float32', 'Float64', 'Float32']:
            non_null_values = df[column_name].dropna()
            if len(non_null_values) > 0:
                if (non_null_values % 1 == 0).all():
                    return f"toInteger(row.{column_name})"
            return f"toFloat(row.{column_name})"
        elif dtype_str == 'bool':
            return f"toBoolean(row.{column_name})"
        elif dtype_str == 'object':
            non_null_values = df[column_name].dropna()
            if len(non_null_values) == 0:
                return f"row.{column_name}"
            try:
                sample_size = min(100, len(non_null_values))
                sample_values = non_null_values.head(sample_size)
                numeric_values = pd.to_numeric(sample_values, errors='raise')
                if (numeric_values % 1 != 0).any():
                    return f"toFloat(row.{column_name})"
                else:
                    return f"toInteger(row.{column_name})"
            except (ValueError, TypeError):
                return f"row.{column_name}"
        else:
            return f"row.{column_name}"

    if id_column not in df.columns:
        available_columns = df.columns.tolist()
        raise KeyError(f"Column '{id_column}' not found in DataFrame. Available columns: {available_columns}")

    # Use id_column_neo4j for the Neo4j property name if specified
    id_prop_name = id_column_neo4j if id_column_neo4j else id_column
    id_conversion = get_cypher_conversion(id_column, df)

    set_clauses = []

    # Determine which columns to use for location (to exclude from regular SET)
    location_source_cols = set(location_columns.keys()) if location_columns else set()

    for col in df.columns:
        if col in exclude_columns or col in location_source_cols:
            continue
        conversion = get_cypher_conversion(col, df)
        # Use mapped column name if specified, otherwise use original
        neo4j_prop = column_mapping.get(col, col)
        set_clauses.append(f"a.{neo4j_prop} = {conversion}")

    # Add location point if specified
    if location_columns:
        lat_col = None
        lon_col = None
        for src_col, point_key in location_columns.items():
            if point_key == 'latitude':
                lat_col = src_col
            elif point_key == 'longitude':
                lon_col = src_col

        if lat_col and lon_col:
            set_clauses.append(
                f"a.location = point({{latitude: toFloat(row.{lat_col}), longitude: toFloat(row.{lon_col})}})"
            )

    parallel_str = str(parallel).lower()
    set_clause_str = ', '.join(set_clauses)

    # Build query as a single string without problematic newlines
    query = (
        f"CALL apoc.periodic.iterate("
        f"\"LOAD CSV WITH HEADERS FROM '{file_placeholder}' AS row FIELDTERMINATOR '{separator}' RETURN row\", "
        f"\"MERGE (a:{node_label} {{{id_prop_name}: {id_conversion}}}) ON CREATE SET {set_clause_str}\", "
        f"{{batchSize: {batch_size}, parallel: {parallel_str}}})"
    )

    return query
