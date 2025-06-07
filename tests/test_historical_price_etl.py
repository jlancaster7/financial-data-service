"""
Tests for Historical Price ETL Pipeline
"""
import pytest
from datetime import date, datetime, timedelta, timezone
from unittest.mock import Mock, patch, MagicMock

from src.etl.historical_price_etl import HistoricalPriceETL
from src.etl.base_etl import ETLStatus
from src.utils.config import Config


@pytest.fixture
def mock_config():
    """Create mock configuration"""
    config = Mock(spec=Config)
    # FMP config
    config.fmp = Mock()
    config.fmp.api_key = "test_key"
    config.fmp.base_url = "https://test.api.com"
    # Snowflake config
    config.snowflake = Mock()
    config.snowflake.account = "test_account"
    config.snowflake.user = "test_user"
    config.snowflake.password = "test_pass"
    config.snowflake.warehouse = "test_wh"
    config.snowflake.database = "test_db"
    config.snowflake.role = "test_role"
    config.snowflake.schema = "test_schema"
    # App config
    config.app = Mock()
    config.app.batch_size = 100
    config.app.enable_monitoring = True
    return config


@pytest.fixture
def mock_fmp_price_data():
    """Create mock FMP price data"""
    return [
        {
            'date': '2024-01-15',
            'open': 150.0,
            'high': 155.0,
            'low': 149.0,
            'close': 154.0,
            'adjClose': 154.0,
            'volume': 1000000,
            'changePercent': 2.67
        },
        {
            'date': '2024-01-14',
            'open': 148.0,
            'high': 151.0,
            'low': 147.0,
            'close': 150.0,
            'adjClose': 150.0,
            'volume': 900000,
            'changePercent': 1.35
        }
    ]


class TestHistoricalPriceETL:
    """Test cases for Historical Price ETL"""
    
    @patch('src.etl.historical_price_etl.SnowflakeConnector')
    @patch('src.etl.historical_price_etl.FMPClient')
    def test_extract_single_symbol(self, mock_fmp_class, mock_sf_class, mock_config, mock_fmp_price_data):
        """Test extracting price data for a single symbol"""
        # Setup mocks
        mock_fmp_instance = Mock()
        mock_fmp_instance.get_historical_prices.return_value = mock_fmp_price_data
        mock_fmp_class.return_value = mock_fmp_instance
        
        mock_sf_instance = Mock()
        mock_sf_class.return_value = mock_sf_instance
        
        # Create ETL instance
        etl = HistoricalPriceETL(mock_config)
        
        # Extract data
        from_date = date(2024, 1, 1)
        to_date = date(2024, 1, 31)
        result = etl.extract(['AAPL'], from_date, to_date)
        
        # Verify
        assert len(result) == 2
        assert all(record['symbol'] == 'AAPL' for record in result)
        mock_fmp_instance.get_historical_prices.assert_called_once_with(
            symbol='AAPL',
            from_date=from_date,
            to_date=to_date
        )
    
    @patch('src.etl.historical_price_etl.SnowflakeConnector')
    @patch('src.etl.historical_price_etl.FMPClient')
    def test_extract_multiple_symbols(self, mock_fmp_class, mock_sf_class, mock_config, mock_fmp_price_data):
        """Test extracting price data for multiple symbols"""
        # Setup mocks
        mock_fmp_instance = Mock()
        mock_fmp_instance.get_historical_prices.return_value = mock_fmp_price_data
        mock_fmp_class.return_value = mock_fmp_instance
        
        mock_sf_instance = Mock()
        mock_sf_class.return_value = mock_sf_instance
        
        # Create ETL instance
        etl = HistoricalPriceETL(mock_config)
        
        # Extract data
        symbols = ['AAPL', 'MSFT', 'GOOGL']
        result = etl.extract(symbols)
        
        # Verify
        assert len(result) == 6  # 2 records per symbol
        assert mock_fmp_instance.get_historical_prices.call_count == 3
    
    @patch('src.etl.historical_price_etl.SnowflakeConnector')
    @patch('src.etl.historical_price_etl.FMPClient')
    def test_extract_with_default_dates(self, mock_fmp_class, mock_sf_class, mock_config):
        """Test extraction with default date range (last 30 days)"""
        # Setup mocks
        mock_fmp_instance = Mock()
        mock_fmp_instance.get_historical_prices.return_value = []
        mock_fmp_class.return_value = mock_fmp_instance
        
        mock_sf_instance = Mock()
        mock_sf_class.return_value = mock_sf_instance
        
        # Create ETL instance
        etl = HistoricalPriceETL(mock_config)
        
        # Extract without dates
        etl.extract(['AAPL'])
        
        # Verify dates are set to defaults
        call_args = mock_fmp_instance.get_historical_prices.call_args
        assert call_args[1]['from_date'] == date.today() - timedelta(days=30)
        assert call_args[1]['to_date'] == date.today()
    
    @patch('src.etl.historical_price_etl.SnowflakeConnector')
    @patch('src.etl.historical_price_etl.FMPClient')
    def test_transform(self, mock_fmp_class, mock_sf_class, mock_config):
        """Test transforming price data"""
        # Setup mocks
        mock_fmp_instance = Mock()
        mock_fmp_class.return_value = mock_fmp_instance
        
        mock_sf_instance = Mock()
        mock_sf_class.return_value = mock_sf_instance
        
        # Create ETL instance
        etl = HistoricalPriceETL(mock_config)
        
        # Mock the transformer in the ETL instance
        mock_transformer = Mock()
        mock_transformer.transform_historical_prices.return_value = {
            'raw': [{'symbol': 'AAPL', 'raw_data': '{}'}],
            'staging': [{'symbol': 'AAPL', 'price_date': date(2024, 1, 15), 'close_price': 154.0}]
        }
        etl.transformer = mock_transformer
        
        # Transform data
        raw_data = [
            {'symbol': 'AAPL', 'date': '2024-01-15', 'close': 154.0}
        ]
        result = etl.transform(raw_data)
        
        # Verify
        assert len(result['raw']) == 1
        assert len(result['staging']) == 1
        mock_transformer.transform_historical_prices.assert_called_once()
    
    @patch('src.etl.historical_price_etl.SnowflakeConnector')
    @patch('src.etl.historical_price_etl.FMPClient')
    def test_load(self, mock_fmp_class, mock_sf_class, mock_config):
        """Test loading data to Snowflake"""
        # Setup mocks
        mock_fmp_instance = Mock()
        mock_fmp_class.return_value = mock_fmp_instance
        
        mock_sf_instance = Mock()
        mock_sf_instance.__enter__ = Mock(return_value=mock_sf_instance)
        mock_sf_instance.__exit__ = Mock(return_value=None)
        mock_sf_instance.bulk_insert.return_value = 1
        mock_sf_instance.merge.return_value = 1
        mock_sf_class.return_value = mock_sf_instance
        
        # Create ETL instance
        etl = HistoricalPriceETL(mock_config)
        
        # Load data
        transformed_data = {
            'raw': [{'symbol': 'AAPL', 'raw_data': '{}'}],
            'staging': [{'symbol': 'AAPL', 'price_date': date(2024, 1, 15)}]
        }
        result = etl.load(transformed_data)
        
        # Verify
        assert result == 2  # 1 raw + 1 staging
        assert mock_sf_instance.bulk_insert.call_count == 1  # Only raw uses bulk_insert
        assert mock_sf_instance.merge.call_count == 1  # Staging uses merge
    
    @patch('src.etl.historical_price_etl.SnowflakeConnector')
    @patch('src.etl.historical_price_etl.FMPClient')
    def test_update_fact_table(self, mock_fmp_class, mock_sf_class, mock_config):
        """Test updating FACT_DAILY_PRICES"""
        # Setup mocks
        mock_fmp_instance = Mock()
        mock_fmp_class.return_value = mock_fmp_instance
        
        mock_sf_instance = Mock()
        mock_sf_instance.__enter__ = Mock(return_value=mock_sf_instance)
        mock_sf_instance.__exit__ = Mock(return_value=None)
        mock_sf_instance.fetch_one.return_value = {'min_date': date(2024, 1, 1)}
        mock_sf_instance.execute.return_value = 10
        mock_sf_class.return_value = mock_sf_instance
        
        # Create ETL instance
        etl = HistoricalPriceETL(mock_config)
        
        # Update fact table
        etl.update_fact_table(['AAPL', 'MSFT'])
        
        # Verify
        mock_sf_instance.fetch_one.assert_called_once()  # To get min date
        mock_sf_instance.execute.assert_called_once()  # MERGE statement
    
    @patch('src.etl.historical_price_etl.SnowflakeConnector')
    @patch('src.etl.historical_price_etl.FMPClient')
    def test_run_complete_pipeline(self, mock_fmp_class, mock_sf_class, mock_config, mock_fmp_price_data):
        """Test running the complete ETL pipeline"""
        # Setup mocks
        mock_fmp_instance = Mock()
        mock_fmp_instance.get_historical_prices.return_value = mock_fmp_price_data
        mock_fmp_class.return_value = mock_fmp_instance
        
        mock_sf_instance = Mock()
        mock_sf_instance.__enter__ = Mock(return_value=mock_sf_instance)
        mock_sf_instance.__exit__ = Mock(return_value=None)
        mock_sf_instance.bulk_insert.return_value = 2
        mock_sf_instance.merge.return_value = 2
        mock_sf_instance.execute.return_value = 2
        mock_sf_instance.fetch_one.return_value = {'min_date': date(2024, 1, 1)}
        mock_sf_class.return_value = mock_sf_instance
        
        # Create ETL instance
        etl = HistoricalPriceETL(mock_config)
        
        # Run pipeline
        result = etl.run(
            symbols=['AAPL'],
            from_date=date(2024, 1, 1),
            to_date=date(2024, 1, 31),
            update_analytics=True
        )
        
        # Verify
        assert result.status == ETLStatus.SUCCESS
        assert result.records_loaded == 4  # 2 raw + 2 staging
        assert "Processed 2 price records for 1 symbols" in result.metadata['message']
    
    @patch('src.etl.historical_price_etl.SnowflakeConnector')
    @patch('src.etl.historical_price_etl.FMPClient')
    def test_run_with_errors(self, mock_fmp_class, mock_sf_class, mock_config):
        """Test pipeline behavior with errors"""
        # Setup mocks to raise error
        mock_fmp_instance = Mock()
        mock_fmp_instance.get_historical_prices.side_effect = Exception("API Error")
        mock_fmp_class.return_value = mock_fmp_instance
        
        mock_sf_instance = Mock()
        mock_sf_class.return_value = mock_sf_instance
        
        # Create ETL instance
        etl = HistoricalPriceETL(mock_config)
        
        # Run pipeline
        result = etl.run(symbols=['AAPL'])
        
        # Verify
        assert result.status == ETLStatus.SUCCESS  # No data but no fatal error
        assert result.records_loaded == 0
        assert len(etl.job_errors) > 0
    
    @patch('src.etl.historical_price_etl.SnowflakeConnector')
    @patch('src.etl.historical_price_etl.FMPClient')
    def test_skip_analytics_update(self, mock_fmp_class, mock_sf_class, mock_config, mock_fmp_price_data):
        """Test skipping analytics layer update"""
        # Setup mocks
        mock_fmp_instance = Mock()
        mock_fmp_instance.get_historical_prices.return_value = mock_fmp_price_data
        mock_fmp_class.return_value = mock_fmp_instance
        
        mock_sf_instance = Mock()
        mock_sf_instance.__enter__ = Mock(return_value=mock_sf_instance)
        mock_sf_instance.__exit__ = Mock(return_value=None)
        mock_sf_instance.bulk_insert.return_value = 2
        mock_sf_instance.execute.return_value = 0
        mock_sf_class.return_value = mock_sf_instance
        
        # Create ETL instance
        etl = HistoricalPriceETL(mock_config)
        
        # Run pipeline without analytics update
        result = etl.run(
            symbols=['AAPL'],
            update_analytics=False
        )
        
        # Verify fact table update was not called
        mock_sf_instance.execute.assert_not_called()