import pytest
from unittest.mock import Mock, patch, MagicMock
from src.db.snowflake_connector import SnowflakeConnector
from src.utils.config import SnowflakeConfig


@pytest.fixture
def mock_config():
    return SnowflakeConfig(
        account="test_account",
        user="test_user",
        password="test_password",
        warehouse="test_warehouse",
        database="test_database",
        schema="test_schema",
        role="test_role"
    )


@pytest.fixture
def mock_connection():
    connection = MagicMock()
    connection.is_closed.return_value = False
    connection.cursor.return_value.__enter__.return_value = MagicMock()
    return connection


class TestSnowflakeConnector:
    
    @patch("snowflake.connector.connect")
    def test_connect(self, mock_connect, mock_config):
        mock_connect.return_value = MagicMock()
        
        connector = SnowflakeConnector(mock_config)
        connector.connect()
        
        mock_connect.assert_called_once_with(
            account="test_account",
            user="test_user",
            password="test_password",
            warehouse="test_warehouse",
            database="test_database",
            schema="test_schema",
            role="test_role",
            client_session_keep_alive=True,
            autocommit=True
        )
    
    @patch("snowflake.connector.connect")
    def test_disconnect(self, mock_connect, mock_config, mock_connection):
        mock_connect.return_value = mock_connection
        
        connector = SnowflakeConnector(mock_config)
        connector.connect()
        connector.disconnect()
        
        mock_connection.close.assert_called_once()
    
    @patch("snowflake.connector.connect")
    def test_execute(self, mock_connect, mock_config, mock_connection):
        mock_cursor = MagicMock()
        # Set up cursor as a context manager
        mock_cursor.__enter__.return_value = mock_cursor
        mock_cursor.__exit__.return_value = None
        mock_connection.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_connection
        
        connector = SnowflakeConnector(mock_config)
        connector.execute("SELECT 1")
        
        mock_cursor.execute.assert_called_once_with("SELECT 1")
    
    @patch("snowflake.connector.connect")
    def test_fetch_all(self, mock_connect, mock_config, mock_connection):
        mock_cursor = MagicMock()
        # Set up cursor as a context manager
        mock_cursor.__enter__.return_value = mock_cursor
        mock_cursor.__exit__.return_value = None
        mock_cursor.description = [("col1",), ("col2",)]
        mock_cursor.fetchall.return_value = [(1, "a"), (2, "b")]
        mock_connection.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_connection
        
        connector = SnowflakeConnector(mock_config)
        result = connector.fetch_all("SELECT col1, col2 FROM table")
        
        assert result == [
            {"col1": 1, "col2": "a"},
            {"col1": 2, "col2": "b"}
        ]
    
    @patch("snowflake.connector.connect")
    def test_bulk_insert(self, mock_connect, mock_config, mock_connection):
        mock_cursor = MagicMock()
        # Set up cursor as a context manager
        mock_cursor.__enter__.return_value = mock_cursor
        mock_cursor.__exit__.return_value = None
        mock_connection.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_connection
        
        connector = SnowflakeConnector(mock_config)
        data = [
            {"col1": 1, "col2": "a"},
            {"col1": 2, "col2": "b"}
        ]
        
        connector.bulk_insert("test_table", data)
        
        # Now using single-row inserts instead of executemany
        expected_query = "INSERT INTO test_table (col1, col2) SELECT %s, %s"
        assert mock_cursor.execute.call_count == 2
        # Check first call
        mock_cursor.execute.assert_any_call(expected_query, (1, "a"))
        # Check second call
        mock_cursor.execute.assert_any_call(expected_query, (2, "b"))
    
    @patch("snowflake.connector.connect")
    def test_table_exists(self, mock_connect, mock_config, mock_connection):
        mock_cursor = MagicMock()
        # Set up cursor as a context manager
        mock_cursor.__enter__.return_value = mock_cursor
        mock_cursor.__exit__.return_value = None
        mock_cursor.description = [("COUNT",)]
        mock_cursor.fetchall.return_value = [(1,)]
        mock_connection.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_connection
        
        connector = SnowflakeConnector(mock_config)
        result = connector.table_exists("test_table")
        
        assert result is True