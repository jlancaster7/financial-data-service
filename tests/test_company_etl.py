"""
Unit tests for Company ETL pipeline
"""
import pytest
from unittest.mock import Mock, MagicMock, patch
from datetime import datetime, timezone

from src.etl.company_etl import CompanyETL
from src.etl.base_etl import ETLStatus
from src.db.snowflake_connector import SnowflakeConnector
from src.api.fmp_client import FMPClient
from src.utils.config import Config, SnowflakeConfig, FMPConfig, AppConfig


class TestCompanyETL:
    """Test Company ETL pipeline"""
    
    @pytest.fixture
    def mock_snowflake(self):
        """Mock Snowflake connector"""
        mock = Mock(spec=SnowflakeConnector)
        mock.fetch_all.return_value = []  # No existing companies by default
        return mock
    
    @pytest.fixture
    def mock_fmp(self):
        """Mock FMP client"""
        mock = Mock(spec=FMPClient)
        # Default successful response
        mock.get_company_profile.side_effect = lambda symbol: {
            'symbol': symbol,
            'companyName': f'{symbol} Inc.',
            'sector': 'Technology',
            'industry': 'Software',
            'exchange': 'NASDAQ',
            'marketCap': 1000000000,
            'description': f'Description for {symbol}',
            'website': f'https://{symbol.lower()}.com',
            'ceo': 'John Doe',
            'fullTimeEmployees': 1000,
            'city': 'San Francisco',
            'state': 'CA',
            'country': 'US'
        }
        return mock
    
    @pytest.fixture
    def mock_config(self):
        """Mock configuration"""
        config = Mock(spec=Config)
        config.snowflake = SnowflakeConfig(
            account="test_account",
            user="test_user",
            password="test_pass",
            database="TEST_DB",
            warehouse="TEST_WH",
            role="TEST_ROLE",
            schema="PUBLIC"
        )
        config.fmp = FMPConfig(
            api_key="test_key",
            base_url="https://financialmodelingprep.com/api/v3",
            rate_limit_calls=300,
            rate_limit_period=60
        )
        config.app = AppConfig(
            batch_size=100,
            enable_monitoring=False,
            log_level="INFO"
        )
        return config
    
    @pytest.fixture
    def company_etl(self, mock_snowflake, mock_fmp, mock_config):
        """Create Company ETL instance"""
        with patch('src.etl.company_etl.SnowflakeConnector', return_value=mock_snowflake):
            with patch('src.etl.company_etl.FMPClient', return_value=mock_fmp):
                return CompanyETL(mock_config)
    
    def test_extract_success(self, company_etl, mock_fmp):
        """Test successful extraction of company profiles"""
        symbols = ['AAPL', 'MSFT', 'GOOGL']
        profiles = company_etl.extract(symbols)
        
        assert len(profiles) == 3
        assert profiles[0]['symbol'] == 'AAPL'
        assert profiles[1]['symbol'] == 'MSFT'
        assert profiles[2]['symbol'] == 'GOOGL'
        assert mock_fmp.get_company_profile.call_count == 3
    
    def test_extract_with_failures(self, company_etl, mock_fmp):
        """Test extraction with some failures"""
        # Make MSFT fail
        def side_effect(symbol):
            if symbol == 'MSFT':
                raise Exception("API Error")
            return {
                'symbol': symbol,
                'companyName': f'{symbol} Inc.',
                'sector': 'Technology'
            }
        
        mock_fmp.get_company_profile.side_effect = side_effect
        
        symbols = ['AAPL', 'MSFT', 'GOOGL']
        profiles = company_etl.extract(symbols)
        
        assert len(profiles) == 2  # Only AAPL and GOOGL
        assert len(company_etl.result.errors) == 2  # Individual error + summary error
        assert 'MSFT' in company_etl.result.errors[0]
    
    def test_extract_with_batch_endpoint(self, company_etl, mock_fmp):
        """Test extraction using batch endpoint"""
        # Add batch method
        mock_fmp.batch_get_company_profiles = Mock(return_value={
            'AAPL': {'symbol': 'AAPL', 'companyName': 'Apple Inc.'},
            'MSFT': {'symbol': 'MSFT', 'companyName': 'Microsoft Corp.'},
            'GOOGL': {'symbol': 'GOOGL', 'companyName': 'Alphabet Inc.'}
        })
        
        symbols = ['AAPL', 'MSFT', 'GOOGL']
        profiles = company_etl.extract(symbols)
        
        assert len(profiles) == 3
        assert mock_fmp.batch_get_company_profiles.called
        assert mock_fmp.get_company_profile.call_count == 0  # Individual calls not made
    
    def test_transform_new_companies(self, company_etl):
        """Test transformation of new companies"""
        raw_profiles = [
            {
                'symbol': 'AAPL',
                'companyName': 'Apple Inc.',
                'sector': 'Technology',
                'marketCap': 3000000000000
            },
            {
                'symbol': 'MSFT',
                'companyName': 'Microsoft Corp.',
                'sector': 'Technology',
                'marketCap': 2500000000000
            }
        ]
        
        transformed = company_etl.transform(raw_profiles)
        
        assert 'raw' in transformed
        assert 'staging' in transformed
        assert len(transformed['raw']) == 2
        assert len(transformed['staging']) == 2
        
        # Check new company flags
        for record in transformed['staging']:
            assert record.get('is_new_company') is True
            assert record.get('has_changes') is True
            assert record.get('changed_fields') == ['all']
    
    def test_transform_with_existing_companies(self, company_etl, mock_snowflake):
        """Test transformation with existing companies"""
        # Set up existing companies
        mock_snowflake.fetch_all.return_value = [
            {
                'SYMBOL': 'AAPL',
                'COMPANY_NAME': 'Apple Inc.',
                'SECTOR': 'Technology',
                'INDUSTRY': 'Consumer Electronics',
                'MARKET_CAP': 3000000000000,
                'IS_CURRENT': True
            }
        ]
        
        # Load existing companies
        company_etl._load_existing_companies()
        
        # Transform with one existing (no changes) and one new
        raw_profiles = [
            {
                'symbol': 'AAPL',
                'companyName': 'Apple Inc.',
                'sector': 'Technology',
                'industry': 'Consumer Electronics',
                'marketCap': 3000000000000
            },
            {
                'symbol': 'MSFT',
                'companyName': 'Microsoft Corp.',
                'sector': 'Technology',
                'marketCap': 2500000000000
            }
        ]
        
        transformed = company_etl.transform(raw_profiles)
        
        staging = transformed['staging']
        
        # Check AAPL (existing, no changes)
        aapl = next(r for r in staging if r['symbol'] == 'AAPL')
        assert aapl['is_new_company'] is False
        assert aapl['has_changes'] is False
        
        # Check MSFT (new)
        msft = next(r for r in staging if r['symbol'] == 'MSFT')
        assert msft['is_new_company'] is True
        assert msft['has_changes'] is True
    
    def test_transform_detects_changes(self, company_etl, mock_snowflake):
        """Test that transformation detects changes in existing companies"""
        # Set up existing company
        mock_snowflake.fetch_all.return_value = [
            {
                'SYMBOL': 'AAPL',
                'COMPANY_NAME': 'Apple Inc.',
                'SECTOR': 'Technology',
                'INDUSTRY': 'Consumer Electronics',
                'MARKET_CAP': 2500000000000,  # Different market cap
                'IS_CURRENT': True
            }
        ]
        
        company_etl._load_existing_companies()
        
        # Transform with significant market cap change (>10%)
        raw_profiles = [{
            'symbol': 'AAPL',
            'companyName': 'Apple Inc.',
            'sector': 'Technology',
            'industry': 'Consumer Electronics',
            'marketCap': 3000000000000  # 20% increase
        }]
        
        transformed = company_etl.transform(raw_profiles)
        
        aapl = transformed['staging'][0]
        assert aapl['is_new_company'] is False
        assert aapl['has_changes'] is True
        assert 'market_cap' in aapl['changed_fields']
    
    def test_load_to_snowflake(self, company_etl, mock_snowflake):
        """Test loading data to Snowflake"""
        transformed_data = {
            'raw': [
                {
                    'symbol': 'AAPL',
                    'raw_data': {'symbol': 'AAPL', 'companyName': 'Apple Inc.'},
                    'api_source': 'FMP',
                    'loaded_timestamp': datetime.now(timezone.utc)
                }
            ],
            'staging': [
                {
                    'symbol': 'AAPL',
                    'company_name': 'Apple Inc.',
                    'sector': 'Technology',
                    'is_new_company': True,
                    'has_changes': True,
                    'changed_fields': ['all']
                }
            ]
        }
        
        loaded = company_etl.load(transformed_data)
        
        # Should load 1 raw + 1 staging
        assert loaded == 2
        assert mock_snowflake.bulk_insert.call_count == 2
        
        # Check that metadata fields were removed from staging
        staging_call = mock_snowflake.bulk_insert.call_args_list[1]
        staging_records = staging_call[0][1]
        assert 'is_new_company' not in staging_records[0]
        assert 'has_changes' not in staging_records[0]
        assert 'changed_fields' not in staging_records[0]
    
    def test_update_analytics_layer(self, company_etl, mock_snowflake):
        """Test updating DIM_COMPANY in analytics layer"""
        # Enable analytics updates
        company_etl.load_to_analytics = True
        
        staging_records = [
            {
                'symbol': 'AAPL',
                'company_name': 'Apple Inc.',
                'sector': 'Technology',
                'industry': 'Consumer Electronics',
                'exchange': 'NASDAQ',
                'market_cap': 3000000000000,
                'description': 'Apple designs consumer electronics',
                'website': 'https://apple.com',
                'ceo': 'Tim Cook',
                'employees': 164000,
                'headquarters_city': 'Cupertino',
                'headquarters_state': 'CA',
                'headquarters_country': 'US',
                'is_new_company': True,
                'has_changes': True,
                'changed_fields': ['all']
            },
            {
                'symbol': 'MSFT',
                'company_name': 'Microsoft Corp.',
                'sector': 'Technology',
                'industry': 'Software',
                'exchange': 'NASDAQ',
                'market_cap': 2500000000000,
                'description': 'Microsoft develops software',
                'website': 'https://microsoft.com',
                'ceo': 'Satya Nadella',
                'employees': 221000,
                'headquarters_city': 'Redmond',
                'headquarters_state': 'WA',
                'headquarters_country': 'US',
                'is_new_company': False,
                'has_changes': True,
                'changed_fields': ['market_cap']
            }
        ]
        
        updates = company_etl._update_analytics_layer(staging_records)
        
        assert updates == 2
        
        # Should execute UPDATE for MSFT and INSERT for both
        assert mock_snowflake.execute.call_count == 1  # UPDATE for MSFT
        assert mock_snowflake.bulk_insert.call_count == 2  # INSERT for both
    
    def test_full_pipeline_run(self, company_etl):
        """Test full ETL pipeline execution"""
        symbols = ['AAPL', 'MSFT', 'GOOGL']
        
        # Extract
        raw_data = company_etl.extract(symbols)
        company_etl.result.records_extracted = len(raw_data)
        
        # Transform
        transformed_data = company_etl.transform(raw_data)
        company_etl.result.records_transformed = len(transformed_data.get('staging', []))
        
        # Load
        records_loaded = company_etl.load(transformed_data)
        company_etl.result.records_loaded = records_loaded
        
        # Set status
        company_etl.result.status = ETLStatus.SUCCESS
        company_etl.result.end_time = datetime.now(timezone.utc)
        
        result = company_etl.result
        
        assert result.status == ETLStatus.SUCCESS
        assert result.records_extracted == 3
        assert result.records_transformed == 3
        assert result.records_loaded > 0
        assert result.duration_seconds is not None
        
    @patch('src.etl.company_etl.logger')
    def test_load_error_handling(self, mock_logger, company_etl, mock_snowflake):
        """Test error handling during load phase"""
        # Make bulk_insert fail for raw layer
        mock_snowflake.bulk_insert.side_effect = [
            Exception("Database error"),
            None  # Staging would succeed
        ]
        
        transformed_data = {
            'raw': [{'symbol': 'AAPL', 'raw_data': {}}],
            'staging': [{'symbol': 'AAPL', 'company_name': 'Apple Inc.'}]
        }
        
        with pytest.raises(Exception, match="Database error"):
            company_etl.load(transformed_data)
        
        assert len(company_etl.result.errors) == 1
        assert "Raw layer load failed" in company_etl.result.errors[0]