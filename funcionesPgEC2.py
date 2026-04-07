import paramiko
import os
import psycopg2
import pandas as pd
import json
from contextlib import contextmanager
from typing import Optional, Dict, Any, List


def find_instance_by_name(ec2_client, instance_name):
    filters = [{'Name': 'tag:Name', 'Values': [instance_name]}]

    resp = ec2_client.describe_instances(Filters=filters)
    for reservation in resp.get('Reservations', []):
        for instance in reservation.get('Instances', []):
            instance_id = instance['InstanceId']
            public_ip = instance.get('PublicIpAddress')
            state = instance['State']['Name']
            print(f"Found instance {instance_id} @ {public_ip} (state: {state})")
            return instance_id, public_ip, state

    raise RuntimeError(f"No instance found with Name tag '{instance_name}'")


def find_running_instance(ec2_client, instance_name):

    resp = ec2_client.describe_instances(
        Filters=[
            {'Name': f'tag:{'Name'}',       'Values': [instance_name]},
            {'Name': 'instance-state-name',  'Values': ['running']}
        ]
    )
    for r in resp['Reservations']:
        for i in r['Instances']:
            instance_id = i['InstanceId']
            public_ip = i['PublicIpAddress']
            print(f"Found instance {instance_id} @ {public_ip}")
            return instance_id, public_ip
    
    raise RuntimeError(f"No running instance found with tag {'Name'}={instance_name}")
    return None, None

def start_instance(ec2_client, instance_id):
    """Start the given EC2 instance."""
    print(f"Starting instance {instance_id}...")
    resp = ec2_client.start_instances(InstanceIds=[instance_id])
    for inst in resp.get('StartingInstances', []):
        print(f"Instance {inst['InstanceId']} state: {inst['PreviousState']['Name']} → {inst['CurrentState']['Name']}")
    return resp

def stop_instance(ec2_client, instance_id):
    """Stop the given EC2 instance."""
    print(f"Stopping instance {instance_id}...")
    resp = ec2_client.stop_instances(InstanceIds=[instance_id])
    stopping = resp.get('StoppingInstances', [])
    for inst in stopping:
        print(f"Instance {inst['InstanceId']} state: {inst['PreviousState']['Name']} → {inst['CurrentState']['Name']}")
    return resp


# ─── POSTGRESQL CONNECTION AND QUERY FUNCTIONS ─────────────────────────────────

class PostgreSQLConnection:
    """PostgreSQL connection class similar to Neo4jConnection"""
    
    def __init__(
        self,
        host: str,
        port: int = 5432,
        database: str = "postgres",
        user: str = "postgres",
        password: str = "",
        **conn_kwargs
    ):
        """
        Initialize PostgreSQL connection
        
        :param host: PostgreSQL server host
        :param port: PostgreSQL server port (default 5432)
        :param database: Database name
        :param user: Username
        :param password: Password
        :param conn_kwargs: Additional connection parameters
        """
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.conn_kwargs = conn_kwargs
        self._connection = None
        
    def connect(self):
        """Establish connection to PostgreSQL"""
        try:
            self._connection = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password,
                **self.conn_kwargs
            )
            return self._connection
        except Exception as e:
            raise RuntimeError(f"Failed to connect to PostgreSQL: {e}") from e
    
    @contextmanager
    def get_connection(self):
        """Context manager for database connections"""
        conn = None
        try:
            conn = self.connect()
            yield conn
        finally:
            if conn:
                conn.close()
    
    @contextmanager
    def get_cursor(self):
        """Context manager for database cursors"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            try:
                yield cursor, conn
            finally:
                cursor.close()
    
    def query(self, sql: str, params: Optional[tuple] = None) -> List[tuple]:
        """
        Execute a SQL query and return results
        
        :param sql: SQL query string
        :param params: Query parameters
        :return: List of result tuples
        """
        with self.get_cursor() as (cursor, conn):
            cursor.execute(sql, params)
            if cursor.description:  # SELECT query
                return cursor.fetchall()
            else:  # INSERT/UPDATE/DELETE
                conn.commit()
                return []
    
    def query_dict(self, sql: str, params: Optional[tuple] = None) -> List[Dict[str, Any]]:
        """
        Execute a SQL query and return results as list of dictionaries
        
        :param sql: SQL query string
        :param params: Query parameters
        :return: List of result dictionaries
        """
        with self.get_cursor() as (cursor, conn):
            cursor.execute(sql, params)
            if cursor.description:
                columns = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                return [dict(zip(columns, row)) for row in rows]
            else:
                conn.commit()
                return []


def pgToDataframe(query: str, conn_pg: PostgreSQLConnection, params: Optional[tuple] = None) -> pd.DataFrame:
    """
    Run SQL query on PostgreSQL connection and return results as pandas DataFrame
    
    :param query: SQL query string
    :param conn_pg: PostgreSQL connection object
    :param params: Query parameters
    :return: pandas DataFrame
    """
    try:
        with conn_pg.get_cursor() as (cursor, conn):
            cursor.execute(query, params)
            if cursor.description:
                columns = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                return pd.DataFrame(rows, columns=columns)
            else:
                return pd.DataFrame()
    except Exception as e:
        print(f"Error executing query: {e}")
        return pd.DataFrame()


def pgToJson(query: str, conn_pg: PostgreSQLConnection, params: Optional[tuple] = None) -> str:
    """
    Run SQL query on PostgreSQL connection and return results as JSON string
    
    :param query: SQL query string
    :param conn_pg: PostgreSQL connection object
    :param params: Query parameters
    :return: JSON string
    """
    try:
        results = conn_pg.query_dict(query, params)
        return json.dumps(results, indent=2, default=str)
    except Exception as e:
        print(f"Error executing query: {e}")
        return json.dumps([])


def pgToDict(query: str, conn_pg: PostgreSQLConnection, group_var: str, params: Optional[tuple] = None) -> Dict[Any, List[Dict]]:
    """
    Run SQL query on PostgreSQL connection and return results grouped by specified variable
    
    :param query: SQL query string
    :param conn_pg: PostgreSQL connection object
    :param group_var: Column name to group by
    :param params: Query parameters
    :return: Dictionary grouped by group_var
    """
    try:
        results = conn_pg.query_dict(query, params)
        grouped = {}
        
        for row in results:
            group_value = row.get(group_var)
            if group_value not in grouped:
                grouped[group_value] = []
            grouped[group_value].append(row)
        
        return grouped
    except Exception as e:
        print(f"Error executing query: {e}")
        return {}


def pgToNestedDict(
    query: str,
    conn_pg: PostgreSQLConnection,
    grouping_keys: List[str],
    params: Optional[tuple] = None
) -> Dict[Any, Any]:
    """
    Run SQL query on PostgreSQL connection and return nested dictionary grouped by multiple keys
    
    :param query: SQL query string
    :param conn_pg: PostgreSQL connection object
    :param grouping_keys: List of column names to group by (in order of nesting)
    :param params: Query parameters
    :return: Nested dictionary
    """
    try:
        results = conn_pg.query_dict(query, params)
        if not results or not grouping_keys:
            return {}
        
        grouped = {}
        
        for row in results:
            # Navigate/create nested structure
            current_level = grouped
            for key in grouping_keys[:-1]:
                key_val = row.get(key)
                if key_val not in current_level:
                    current_level[key_val] = {}
                current_level = current_level[key_val]
            
            # Set final value
            final_key = row.get(grouping_keys[-1])
            current_level[final_key] = row
        
        return grouped
    except Exception as e:
        print(f"Error executing query: {e}")
        return {}


def pgQuery(query: str, conn_pg: PostgreSQLConnection, params: Optional[tuple] = None) -> List[tuple]:
    """
    Execute SQL query on PostgreSQL connection and return raw results
    
    :param query: SQL query string
    :param conn_pg: PostgreSQL connection object
    :param params: Query parameters
    :return: List of result tuples
    """
    return conn_pg.query(query, params)


def pgExecute(query: str, conn_pg: PostgreSQLConnection, params: Optional[tuple] = None) -> bool:
    """
    Execute SQL statement (INSERT/UPDATE/DELETE) on PostgreSQL connection
    
    :param query: SQL statement string
    :param conn_pg: PostgreSQL connection object
    :param params: Query parameters
    :return: True if successful, False otherwise
    """
    try:
        with conn_pg.get_cursor() as (cursor, conn):
            cursor.execute(query, params)
            conn.commit()
            return True
    except Exception as e:
        print(f"Error executing statement: {e}")
        return False


def pgBulkInsert(table: str, data: List[Dict[str, Any]], conn_pg: PostgreSQLConnection) -> bool:
    """
    Bulk insert data into PostgreSQL table
    
    :param table: Table name
    :param data: List of dictionaries with column:value pairs
    :param conn_pg: PostgreSQL connection object
    :return: True if successful, False otherwise
    """
    if not data:
        return True
    
    try:
        columns = list(data[0].keys())
        placeholders = ', '.join(['%s'] * len(columns))
        columns_str = ', '.join(columns)
        
        insert_query = f"INSERT INTO {table} ({columns_str}) VALUES ({placeholders})"
        
        with conn_pg.get_cursor() as (cursor, conn):
            for row in data:
                values = tuple(row[col] for col in columns)
                cursor.execute(insert_query, values)
            conn.commit()
            return True
    except Exception as e:
        print(f"Error bulk inserting data: {e}")
        return False
