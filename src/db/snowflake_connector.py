import snowflake.connector
from snowflake.connector import SnowflakeConnection, ProgrammingError, DatabaseError
from contextlib import contextmanager
from typing import Dict, List, Any, Optional, Generator
import pandas as pd
from loguru import logger
from src.utils.config import SnowflakeConfig


class SnowflakeConnector:
    def __init__(self, config: SnowflakeConfig):
        self.config = config
        self._connection: Optional[SnowflakeConnection] = None
        
    def _get_connection_params(self) -> Dict[str, Any]:
        return {
            "account": self.config.account,
            "user": self.config.user,
            "password": self.config.password,
            "warehouse": self.config.warehouse,
            "database": self.config.database,
            "schema": self.config.schema,
            "role": self.config.role,
            "client_session_keep_alive": True,
            "autocommit": True
        }
    
    def connect(self) -> None:
        """Establish connection to Snowflake"""
        try:
            if self._connection is None or self._connection.is_closed():
                logger.info("Connecting to Snowflake...")
                self._connection = snowflake.connector.connect(**self._get_connection_params())
                logger.info("Successfully connected to Snowflake")
        except Exception as e:
            logger.error(f"Failed to connect to Snowflake: {e}")
            raise
    
    def disconnect(self) -> None:
        """Close Snowflake connection"""
        if self._connection and not self._connection.is_closed():
            self._connection.close()
            logger.info("Disconnected from Snowflake")
            self._connection = None
    
    @property
    def connection(self) -> SnowflakeConnection:
        """Get active connection, creating one if necessary"""
        if self._connection is None or self._connection.is_closed():
            self.connect()
        return self._connection
    
    @contextmanager
    def cursor(self) -> Generator:
        """Context manager for cursor operations"""
        cursor = None
        try:
            cursor = self.connection.cursor()
            yield cursor
        except ProgrammingError as e:
            logger.error(f"SQL execution error: {e}")
            raise
        except DatabaseError as e:
            logger.error(f"Database error: {e}")
            raise
        finally:
            if cursor:
                cursor.close()
    
    def execute(self, query: str, params: Optional[Dict[str, Any]] = None) -> None:
        """Execute a query without returning results"""
        with self.cursor() as cursor:
            logger.debug(f"Executing query: {query}")
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            logger.debug("Query executed successfully")
    
    def fetch_all(self, query: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """Execute query and fetch all results as list of dicts"""
        with self.cursor() as cursor:
            logger.debug(f"Fetching data with query: {query}")
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            
            columns = [desc[0] for desc in cursor.description]
            results = [dict(zip(columns, row)) for row in cursor.fetchall()]
            logger.debug(f"Fetched {len(results)} rows")
            return results
    
    def fetch_pandas(self, query: str, params: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
        """Execute query and return results as pandas DataFrame"""
        with self.cursor() as cursor:
            logger.debug(f"Fetching DataFrame with query: {query}")
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            
            df = cursor.fetch_pandas_all()
            logger.debug(f"Fetched DataFrame with {len(df)} rows")
            return df
    
    def bulk_insert(self, table: str, data: List[Dict[str, Any]], chunk_size: int = 1000) -> None:
        """Bulk insert data into table"""
        if not data:
            logger.warning("No data to insert")
            return
        
        columns = list(data[0].keys())
        placeholders = ", ".join([f"%({col})s" for col in columns])
        column_names = ", ".join(columns)
        
        query = f"INSERT INTO {table} ({column_names}) VALUES ({placeholders})"
        
        with self.cursor() as cursor:
            logger.info(f"Bulk inserting {len(data)} rows into {table}")
            
            for i in range(0, len(data), chunk_size):
                chunk = data[i:i + chunk_size]
                cursor.executemany(query, chunk)
                logger.debug(f"Inserted chunk {i//chunk_size + 1} ({len(chunk)} rows)")
            
            logger.info(f"Bulk insert completed for {table}")
    
    def truncate_table(self, table: str) -> None:
        """Truncate a table"""
        query = f"TRUNCATE TABLE IF EXISTS {table}"
        self.execute(query)
        logger.info(f"Truncated table {table}")
    
    def table_exists(self, table: str, schema: Optional[str] = None) -> bool:
        """Check if table exists"""
        schema = schema or self.config.schema
        query = """
            SELECT COUNT(*) as count
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = %(schema)s
            AND TABLE_NAME = %(table)s
        """
        result = self.fetch_all(query, {"schema": schema.upper(), "table": table.upper()})
        return result[0]["COUNT"] > 0 if result else False
    
    def get_table_row_count(self, table: str) -> int:
        """Get row count for a table"""
        query = f"SELECT COUNT(*) as count FROM {table}"
        result = self.fetch_all(query)
        return result[0]["COUNT"] if result else 0
    
    def __enter__(self):
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()