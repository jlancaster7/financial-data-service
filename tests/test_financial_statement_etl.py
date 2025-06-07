"""
Tests for Financial Statement ETL Pipeline
"""
import pytest
from datetime import date, datetime, timedelta, timezone
from unittest.mock import Mock, patch, MagicMock

from src.etl.financial_statement_etl import FinancialStatementETL
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
def mock_fmp_financial_data():
    """Create mock FMP financial statement data"""
    return {
        'income': [
            {
                'symbol': 'AAPL',
                'date': '2023-09-30',
                'period': 'FY',
                'reportedCurrency': 'USD',
                'revenue': 383285000000,
                'costOfRevenue': 214137000000,
                'grossProfit': 169148000000,
                'operatingIncome': 114301000000,
                'netIncome': 96995000000,
                'eps': 6.16,
                'epsDiluted': 6.13
            },
            {
                'symbol': 'AAPL',
                'date': '2022-09-24',
                'period': 'FY',
                'reportedCurrency': 'USD',
                'revenue': 394328000000,
                'costOfRevenue': 223546000000,
                'grossProfit': 170782000000,
                'operatingIncome': 119437000000,
                'netIncome': 99803000000,
                'eps': 6.15,
                'epsDiluted': 6.11
            }
        ],
        'balance': [
            {
                'symbol': 'AAPL',
                'date': '2023-09-30',
                'period': 'FY',
                'reportedCurrency': 'USD',
                'totalAssets': 352755000000,
                'totalLiabilities': 290437000000,
                'totalStockholdersEquity': 62318000000,  # Fixed to make equation balance
                'cashAndCashEquivalents': 29965000000,
                'totalDebt': 111110000000,
                'netDebt': 81145000000
            }
        ],
        'cashflow': [
            {
                'symbol': 'AAPL',
                'date': '2023-09-30',
                'period': 'FY',
                'reportedCurrency': 'USD',
                'operatingCashFlow': 110543000000,
                'netCashProvidedByInvestingActivities': -7077000000,
                'netCashProvidedByFinancingActivities': -108488000000,
                'freeCashFlow': 99584000000,
                'capitalExpenditure': -10959000000,
                'dividendsPaid': -15025000000
            }
        ]
    }


class TestFinancialStatementETL:
    """Test cases for Financial Statement ETL"""
    
    @patch('src.etl.financial_statement_etl.SnowflakeConnector')
    @patch('src.etl.financial_statement_etl.FMPClient')
    def test_extract_single_symbol(self, mock_fmp_class, mock_sf_class, mock_config, mock_fmp_financial_data):
        """Test extracting financial statements for a single symbol"""
        # Setup mocks
        mock_fmp_instance = Mock()
        mock_fmp_instance.get_income_statement.return_value = mock_fmp_financial_data['income']
        mock_fmp_instance.get_balance_sheet.return_value = mock_fmp_financial_data['balance']
        mock_fmp_instance.get_cash_flow.return_value = mock_fmp_financial_data['cashflow']
        mock_fmp_class.return_value = mock_fmp_instance
        
        mock_sf_instance = Mock()
        mock_sf_class.return_value = mock_sf_instance
        
        # Create ETL instance
        etl = FinancialStatementETL(mock_config)
        
        # Extract data
        result = etl.extract(['AAPL'], period='annual', limit=5)
        
        # Verify
        assert len(result['income']) == 2
        assert len(result['balance']) == 1
        assert len(result['cashflow']) == 1
        mock_fmp_instance.get_income_statement.assert_called_once_with(
            symbol='AAPL',
            period='annual',
            limit=5
        )
        mock_fmp_instance.get_balance_sheet.assert_called_once()
        mock_fmp_instance.get_cash_flow.assert_called_once()
    
    @patch('src.etl.financial_statement_etl.SnowflakeConnector')
    @patch('src.etl.financial_statement_etl.FMPClient')
    def test_extract_multiple_symbols(self, mock_fmp_class, mock_sf_class, mock_config, mock_fmp_financial_data):
        """Test extracting financial statements for multiple symbols"""
        # Setup mocks
        mock_fmp_instance = Mock()
        mock_fmp_instance.get_income_statement.return_value = mock_fmp_financial_data['income']
        mock_fmp_instance.get_balance_sheet.return_value = mock_fmp_financial_data['balance']
        mock_fmp_instance.get_cash_flow.return_value = mock_fmp_financial_data['cashflow']
        mock_fmp_class.return_value = mock_fmp_instance
        
        mock_sf_instance = Mock()
        mock_sf_class.return_value = mock_sf_instance
        
        # Create ETL instance
        etl = FinancialStatementETL(mock_config)
        
        # Extract data
        symbols = ['AAPL', 'MSFT', 'GOOGL']
        result = etl.extract(symbols, period='quarterly', limit=4)
        
        # Verify
        assert mock_fmp_instance.get_income_statement.call_count == 3
        assert mock_fmp_instance.get_balance_sheet.call_count == 3
        assert mock_fmp_instance.get_cash_flow.call_count == 3
    
    @patch('src.etl.financial_statement_etl.SnowflakeConnector')
    @patch('src.etl.financial_statement_etl.FMPClient')
    def test_extract_with_api_error(self, mock_fmp_class, mock_sf_class, mock_config):
        """Test extraction with API errors"""
        # Setup mocks
        mock_fmp_instance = Mock()
        mock_fmp_instance.get_income_statement.side_effect = Exception("API Error")
        mock_fmp_instance.get_balance_sheet.return_value = []
        mock_fmp_instance.get_cash_flow.return_value = []
        mock_fmp_class.return_value = mock_fmp_instance
        
        mock_sf_instance = Mock()
        mock_sf_class.return_value = mock_sf_instance
        
        # Create ETL instance
        etl = FinancialStatementETL(mock_config)
        
        # Extract data
        result = etl.extract(['AAPL'])
        
        # Verify
        assert len(result['income']) == 0
        assert len(result['balance']) == 0
        assert len(result['cashflow']) == 0
        assert len(etl.job_errors) > 0
    
    @patch('src.etl.financial_statement_etl.SnowflakeConnector')
    @patch('src.etl.financial_statement_etl.FMPClient')
    def test_transform(self, mock_fmp_class, mock_sf_class, mock_config, mock_fmp_financial_data):
        """Test transforming financial statement data"""
        # Setup mocks
        mock_fmp_instance = Mock()
        mock_fmp_class.return_value = mock_fmp_instance
        
        mock_sf_instance = Mock()
        mock_sf_class.return_value = mock_sf_instance
        
        # Create ETL instance
        etl = FinancialStatementETL(mock_config)
        
        # Mock the transformer
        mock_transformer = Mock()
        mock_transformer.transform_income_statements.return_value = {
            'raw': [{'symbol': 'AAPL', 'raw_data': '{}'}],
            'staging': [{'symbol': 'AAPL', 'fiscal_date': date(2023, 9, 30), 'period': 'FY', 'revenue': 383285000000}]
        }
        mock_transformer.transform_balance_sheets.return_value = {
            'raw': [{'symbol': 'AAPL', 'raw_data': '{}'}],
            'staging': [{'symbol': 'AAPL', 'fiscal_date': date(2023, 9, 30), 'period': 'FY', 'total_assets': 352755000000}]
        }
        mock_transformer.transform_cash_flows.return_value = {
            'raw': [{'symbol': 'AAPL', 'raw_data': '{}'}],
            'staging': [{'symbol': 'AAPL', 'fiscal_date': date(2023, 9, 30), 'period': 'FY', 'operating_cash_flow': 110543000000}]
        }
        etl.transformer = mock_transformer
        
        # Transform data
        result = etl.transform(mock_fmp_financial_data)
        
        # Verify
        assert len(result['income']['raw']) == 1
        assert len(result['income']['staging']) == 1
        assert len(result['balance']['raw']) == 1
        assert len(result['balance']['staging']) == 1
        assert len(result['cashflow']['raw']) == 1
        assert len(result['cashflow']['staging']) == 1
    
    @patch('src.etl.financial_statement_etl.SnowflakeConnector')
    @patch('src.etl.financial_statement_etl.FMPClient')
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
        etl = FinancialStatementETL(mock_config)
        
        # Load data
        transformed_data = {
            'income': {
                'raw': [{'symbol': 'AAPL', 'raw_data': '{}'}],
                'staging': [{'symbol': 'AAPL', 'fiscal_date': date(2023, 9, 30)}]
            },
            'balance': {
                'raw': [{'symbol': 'AAPL', 'raw_data': '{}'}],
                'staging': [{'symbol': 'AAPL', 'fiscal_date': date(2023, 9, 30)}]
            },
            'cashflow': {
                'raw': [{'symbol': 'AAPL', 'raw_data': '{}'}],
                'staging': [{'symbol': 'AAPL', 'fiscal_date': date(2023, 9, 30)}]
            }
        }
        result = etl.load(transformed_data)
        
        # Verify
        assert result == 6  # 3 raw + 3 staging
        assert mock_sf_instance.bulk_insert.call_count == 3  # 3 raw tables
        assert mock_sf_instance.merge.call_count == 3  # 3 staging tables
    
    @patch('src.etl.financial_statement_etl.SnowflakeConnector')
    @patch('src.etl.financial_statement_etl.FMPClient')
    def test_update_fact_table(self, mock_fmp_class, mock_sf_class, mock_config):
        """Test updating FACT_FINANCIALS"""
        # Setup mocks
        mock_fmp_instance = Mock()
        mock_fmp_class.return_value = mock_fmp_instance
        
        mock_sf_instance = Mock()
        mock_sf_instance.__enter__ = Mock(return_value=mock_sf_instance)
        mock_sf_instance.__exit__ = Mock(return_value=None)
        mock_sf_instance.fetch_all.return_value = [{'min_date': date(2020, 1, 1)}]
        mock_sf_instance.execute.return_value = 10
        mock_sf_class.return_value = mock_sf_instance
        
        # Create ETL instance
        etl = FinancialStatementETL(mock_config)
        
        # Update fact table
        etl.update_fact_table(['AAPL', 'MSFT'])
        
        # Verify
        mock_sf_instance.fetch_all.assert_called_once()  # To get min date
        mock_sf_instance.execute.assert_called_once()  # MERGE statement
    
    @patch('src.etl.financial_statement_etl.SnowflakeConnector')
    @patch('src.etl.financial_statement_etl.FMPClient')
    def test_run_complete_pipeline(self, mock_fmp_class, mock_sf_class, mock_config, mock_fmp_financial_data):
        """Test running the complete ETL pipeline"""
        # Setup mocks
        mock_fmp_instance = Mock()
        mock_fmp_instance.get_income_statement.return_value = mock_fmp_financial_data['income']
        mock_fmp_instance.get_balance_sheet.return_value = mock_fmp_financial_data['balance']
        mock_fmp_instance.get_cash_flow.return_value = mock_fmp_financial_data['cashflow']
        mock_fmp_class.return_value = mock_fmp_instance
        
        mock_sf_instance = Mock()
        mock_sf_instance.__enter__ = Mock(return_value=mock_sf_instance)
        mock_sf_instance.__exit__ = Mock(return_value=None)
        mock_sf_instance.bulk_insert.return_value = 1
        mock_sf_instance.merge.return_value = 1
        mock_sf_instance.execute.return_value = 1
        mock_sf_instance.fetch_all.return_value = [{'min_date': date(2020, 1, 1)}]
        mock_sf_class.return_value = mock_sf_instance
        
        # Create ETL instance
        etl = FinancialStatementETL(mock_config)
        
        # Run pipeline
        result = etl.run(
            symbols=['AAPL'],
            period='annual',
            limit=5,
            update_analytics=True
        )
        
        # Verify
        assert result.status == ETLStatus.SUCCESS
        assert result.records_extracted == 4  # 2 income + 1 balance + 1 cashflow
        assert result.records_loaded == 6  # 3 raw + 3 staging
        assert "Processed 4 financial statements for 1 symbols" in result.metadata['message']
    
    @patch('src.etl.financial_statement_etl.SnowflakeConnector')
    @patch('src.etl.financial_statement_etl.FMPClient')
    def test_run_with_no_data(self, mock_fmp_class, mock_sf_class, mock_config):
        """Test pipeline behavior with no data"""
        # Setup mocks
        mock_fmp_instance = Mock()
        mock_fmp_instance.get_income_statement.return_value = []
        mock_fmp_instance.get_balance_sheet.return_value = []
        mock_fmp_instance.get_cash_flow.return_value = []
        mock_fmp_class.return_value = mock_fmp_instance
        
        mock_sf_instance = Mock()
        mock_sf_class.return_value = mock_sf_instance
        
        # Create ETL instance
        etl = FinancialStatementETL(mock_config)
        
        # Run pipeline
        result = etl.run(symbols=['AAPL'])
        
        # Verify
        assert result.status == ETLStatus.SUCCESS
        assert result.records_extracted == 0
        assert result.records_loaded == 0
        assert "No data to process" in result.metadata['message']
    
    @patch('src.etl.financial_statement_etl.SnowflakeConnector')
    @patch('src.etl.financial_statement_etl.FMPClient')
    def test_skip_analytics_update(self, mock_fmp_class, mock_sf_class, mock_config, mock_fmp_financial_data):
        """Test skipping analytics layer update"""
        # Setup mocks
        mock_fmp_instance = Mock()
        mock_fmp_instance.get_income_statement.return_value = mock_fmp_financial_data['income']
        mock_fmp_instance.get_balance_sheet.return_value = []
        mock_fmp_instance.get_cash_flow.return_value = []
        mock_fmp_class.return_value = mock_fmp_instance
        
        mock_sf_instance = Mock()
        mock_sf_instance.__enter__ = Mock(return_value=mock_sf_instance)
        mock_sf_instance.__exit__ = Mock(return_value=None)
        mock_sf_instance.bulk_insert.return_value = 2
        mock_sf_instance.merge.return_value = 2
        mock_sf_instance.execute.return_value = 0
        mock_sf_class.return_value = mock_sf_instance
        
        # Create ETL instance
        etl = FinancialStatementETL(mock_config)
        
        # Run pipeline without analytics update
        result = etl.run(
            symbols=['AAPL'],
            period='quarterly',
            update_analytics=False
        )
        
        # Verify fact table update was not called
        mock_sf_instance.execute.assert_not_called()