import boto3
import pandas as pd
import json
import time
from typing import List, Dict, Optional


class AthenaMetadataConnection:
    def __init__(
        self,
        aws_access_key_id: str,
        aws_secret_access_key: str,
        region_name: str,
        catalog_name: str = 'AwsDataCatalog',
        **client_kwargs
    ):
        """
        Initialize Athena Metadata connection for reading table metadata.

        :param aws_access_key_id: AWS Access Key ID
        :param aws_secret_access_key: AWS Secret Access Key
        :param region_name: AWS Region (e.g., 'us-east-1')
        :param catalog_name: Athena catalog name (default: 'AwsDataCatalog')
        :param client_kwargs: Additional kwargs to pass to boto3.client()
        """
        self.__aws_access_key_id = aws_access_key_id
        self.__aws_secret_access_key = aws_secret_access_key
        self.__region_name = region_name
        self.__catalog_name = catalog_name

        try:
            self.__client = boto3.client(
                'athena',
                aws_access_key_id=self.__aws_access_key_id,
                aws_secret_access_key=self.__aws_secret_access_key,
                region_name=self.__region_name,
                **client_kwargs
            )
        except Exception as e:
            raise RuntimeError(f"Failed to create Athena client: {e}") from e

    def get_catalog_name(self) -> str:
        """Return the catalog name being used."""
        return self.__catalog_name

    def list_databases(self) -> List[str]:
        """
        List all databases in the Athena catalog.

        :return: List of database names
        """
        try:
            response = self.__client.list_databases(CatalogName=self.__catalog_name)
            return [db['Name'] for db in response.get('DatabaseList', [])]
        except Exception as e:
            print(f"Error listing databases: {e}")
            return []

    def list_tables(self, database_name: str, max_results: int = 50) -> List[str]:
        """
        List all tables in a specific database.

        :param database_name: Name of the database
        :param max_results: Maximum number of results to return
        :return: List of table names
        """
        try:
            response = self.__client.list_table_metadata(
                CatalogName=self.__catalog_name,
                DatabaseName=database_name,
                MaxResults=max_results
            )
            return [table['Name'] for table in response.get('TableMetadataList', [])]
        except Exception as e:
            print(f"Error listing tables in database {database_name}: {e}")
            return []

    def get_table_metadata(self, database_name: str, table_name: str) -> Optional[Dict]:
        """
        Get detailed metadata for a specific table.

        :param database_name: Name of the database
        :param table_name: Name of the table
        :return: Dictionary containing table metadata, or None if error
        """
        try:
            response = self.__client.get_table_metadata(
                CatalogName=self.__catalog_name,
                DatabaseName=database_name,
                TableName=table_name
            )

            table_metadata = response.get('TableMetadata', {})

            # Extract columns information
            columns = []
            for col in table_metadata.get('Columns', []):
                columns.append({
                    'Name': col.get('Name'),
                    'Type': col.get('Type'),
                    'Comment': col.get('Comment', '')
                })

            # Extract partition keys
            partition_keys = []
            for pk in table_metadata.get('PartitionKeys', []):
                partition_keys.append({
                    'Name': pk.get('Name'),
                    'Type': pk.get('Type'),
                    'Comment': pk.get('Comment', '')
                })

            # Build metadata structure
            metadata = {
                'DatabaseName': database_name,
                'TableName': table_metadata.get('Name'),
                'TableType': table_metadata.get('TableType'),
                'Columns': columns,
                'PartitionKeys': partition_keys,
                'Location': table_metadata.get('Parameters', {}).get('location', ''),
                'InputFormat': table_metadata.get('Parameters', {}).get('inputformat', ''),
                'OutputFormat': table_metadata.get('Parameters', {}).get('outputformat', ''),
                'SerdeInfo': table_metadata.get('Parameters', {}).get('serde.serialization.lib', ''),
                'Parameters': table_metadata.get('Parameters', {}),
                'CreateTime': str(table_metadata.get('CreateTime', '')),
                'LastAccessTime': str(table_metadata.get('LastAccessTime', '')),
                'ColumnCount': len(columns),
                'PartitionCount': len(partition_keys)
            }

            return metadata

        except Exception as e:
            print(f"Error getting metadata for table {database_name}.{table_name}: {e}")
            return None

    def get_all_tables_metadata(self, database_name: str) -> List[Dict]:
        """
        Get metadata for all tables in a database.

        :param database_name: Name of the database
        :return: List of dictionaries containing metadata for each table
        """
        tables = self.list_tables(database_name)
        metadata_list = []

        for table_name in tables:
            print(f"Reading metadata for {database_name}.{table_name}...")
            metadata = self.get_table_metadata(database_name, table_name)
            if metadata:
                metadata_list.append(metadata)
            time.sleep(0.5)  # Small delay to avoid API throttling

        return metadata_list

    def close(self):
        """Close the Athena client connection."""
        self.__client = None


def athenaMetadataToDataframe(database_name: str, conn_athena: AthenaMetadataConnection,
                               table_name: Optional[str] = None) -> pd.DataFrame:
    """
    Get table metadata and return as a pandas DataFrame.

    :param database_name: Name of the database
    :param conn_athena: AthenaMetadataConnection instance
    :param table_name: Optional specific table name. If None, gets all tables
    :return: DataFrame with metadata
    """
    if table_name:
        metadata = conn_athena.get_table_metadata(database_name, table_name)
        if metadata:
            # Flatten columns and partition keys for better DataFrame representation
            metadata_flat = metadata.copy()
            metadata_flat['Columns'] = json.dumps(metadata['Columns'])
            metadata_flat['PartitionKeys'] = json.dumps(metadata['PartitionKeys'])
            metadata_flat['Parameters'] = json.dumps(metadata['Parameters'])
            return pd.DataFrame([metadata_flat])
        else:
            return pd.DataFrame()
    else:
        metadata_list = conn_athena.get_all_tables_metadata(database_name)
        if metadata_list:
            # Flatten nested structures for DataFrame
            flattened = []
            for metadata in metadata_list:
                metadata_flat = metadata.copy()
                metadata_flat['Columns'] = json.dumps(metadata['Columns'])
                metadata_flat['PartitionKeys'] = json.dumps(metadata['PartitionKeys'])
                metadata_flat['Parameters'] = json.dumps(metadata['Parameters'])
                flattened.append(metadata_flat)
            return pd.DataFrame(flattened)
        else:
            return pd.DataFrame()


def athenaMetadataToJson(database_name: str, conn_athena: AthenaMetadataConnection,
                         table_name: Optional[str] = None) -> str:
    """
    Get table metadata and return as JSON string.

    :param database_name: Name of the database
    :param conn_athena: AthenaMetadataConnection instance
    :param table_name: Optional specific table name. If None, gets all tables
    :return: JSON string with metadata
    """
    if table_name:
        metadata = conn_athena.get_table_metadata(database_name, table_name)
        return json.dumps(metadata if metadata else {}, indent=2, default=str)
    else:
        metadata_list = conn_athena.get_all_tables_metadata(database_name)
        return json.dumps(metadata_list, indent=2, default=str)


def athenaMetadataToDict(database_name: str, conn_athena: AthenaMetadataConnection) -> Dict[str, Dict]:
    """
    Get all tables metadata and return as a dictionary grouped by table name.

    :param database_name: Name of the database
    :param conn_athena: AthenaMetadataConnection instance
    :return: Dictionary with table names as keys and metadata as values
    """
    metadata_list = conn_athena.get_all_tables_metadata(database_name)
    return {metadata['TableName']: metadata for metadata in metadata_list}


def athenaColumnsToDataframe(database_name: str, conn_athena: AthenaMetadataConnection,
                              table_name: Optional[str] = None) -> pd.DataFrame:
    """
    Get column information from tables and return as a DataFrame.
    Each row represents one column.

    :param database_name: Name of the database
    :param conn_athena: AthenaMetadataConnection instance
    :param table_name: Optional specific table name. If None, gets all tables
    :return: DataFrame with one row per column
    """
    if table_name:
        metadata = conn_athena.get_table_metadata(database_name, table_name)
        if metadata:
            columns_data = []
            # Regular columns
            for col in metadata.get('Columns', []):
                columns_data.append({
                    'DatabaseName': metadata['DatabaseName'],
                    'TableName': metadata['TableName'],
                    'ColumnName': col['Name'],
                    'ColumnType': col['Type'],
                    'Comment': col['Comment'],
                    'IsPartitionKey': False
                })
            # Partition columns
            for col in metadata.get('PartitionKeys', []):
                columns_data.append({
                    'DatabaseName': metadata['DatabaseName'],
                    'TableName': metadata['TableName'],
                    'ColumnName': col['Name'],
                    'ColumnType': col['Type'],
                    'Comment': col['Comment'],
                    'IsPartitionKey': True
                })
            return pd.DataFrame(columns_data)
        else:
            return pd.DataFrame()
    else:
        metadata_list = conn_athena.get_all_tables_metadata(database_name)
        all_columns = []
        for metadata in metadata_list:
            # Regular columns
            for col in metadata.get('Columns', []):
                all_columns.append({
                    'DatabaseName': metadata['DatabaseName'],
                    'TableName': metadata['TableName'],
                    'ColumnName': col['Name'],
                    'ColumnType': col['Type'],
                    'Comment': col['Comment'],
                    'IsPartitionKey': False
                })
            # Partition columns
            for col in metadata.get('PartitionKeys', []):
                all_columns.append({
                    'DatabaseName': metadata['DatabaseName'],
                    'TableName': metadata['TableName'],
                    'ColumnName': col['Name'],
                    'ColumnType': col['Type'],
                    'Comment': col['Comment'],
                    'IsPartitionKey': True
                })
        return pd.DataFrame(all_columns)


def athenaDatabasesToDataframe(conn_athena: AthenaMetadataConnection) -> pd.DataFrame:
    """
    Get list of all databases and return as a DataFrame.

    :param conn_athena: AthenaMetadataConnection instance
    :return: DataFrame with database names
    """
    databases = conn_athena.list_databases()
    return pd.DataFrame({'DatabaseName': databases})
