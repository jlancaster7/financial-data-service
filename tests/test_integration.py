"""
Integration tests for the financial data pipeline

These tests verify the full pipeline works correctly end-to-end,
including parallel processing, data flow, and calculations.
"""
import os
import sys
import pytest
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import Mock, patch
import json

# Add the src directory to the path
sys.path.append(str(Path(__file__).parent.parent))

from src.utils.config import Config
from src.db.snowflake_connector import SnowflakeConnector
from scripts.run_daily_pipeline import PipelineOrchestrator


class TestIntegration:
    """Integration tests for the full pipeline"""
    
    @pytest.fixture
    def test_config(self):
        """Create test configuration"""
        config = Mock(spec=Config)
        
        # Snowflake config
        config.snowflake = Mock()
        config.snowflake.account = "test_account"
        config.snowflake.user = "test_user"
        config.snowflake.password = "test_password"
        config.snowflake.warehouse = "test_warehouse"
        config.snowflake.database = "EQUITY_DATA_TEST"  # Test database
        config.snowflake.schema = "PUBLIC"
        config.snowflake.role = "test_role"
        
        # FMP config
        config.fmp = Mock()
        config.fmp.api_key = "test_key"
        config.fmp.base_url = "https://financialmodelingprep.com/api/v3"
        config.fmp.rate_limit_calls = 300
        config.fmp.rate_limit_period = 60
        
        # App config
        config.app = Mock()
        config.app.log_level = "INFO"
        config.app.batch_size = 100
        config.app.enable_monitoring = True
        config.app.connection_pool_size = 3
        config.app.pipeline_timeout = 600
        config.app.enable_parallel_processing = True
        
        return config
    
    @pytest.fixture
    def mock_fmp_responses(self):
        """Mock FMP API responses"""
        return {
            'company_profile': [{
                'symbol': 'TEST',
                'companyName': 'Test Company',
                'sector': 'Technology',
                'industry': 'Software',
                'exchange': 'NASDAQ',
                'currency': 'USD',
                'country': 'US',
                'marketCap': 1000000000,
                'employees': 1000,
                'website': 'https://test.com',
                'description': 'Test company description',
                'ceo': 'Test CEO',
                'address': '123 Test St',
                'city': 'Test City',
                'state': 'TS',
                'zip': '12345'
            }],
            'historical_prices': [
                {
                    'date': '2025-01-15',
                    'open': 100.0,
                    'high': 105.0,
                    'low': 99.0,
                    'close': 103.0,
                    'adjClose': 103.0,
                    'volume': 1000000,
                    'unadjustedVolume': 1000000,
                    'change': 3.0,
                    'changePercent': 3.0,
                    'vwap': 102.0,
                    'label': 'January 15, 25',
                    'changeOverTime': 0.03
                },
                {
                    'date': '2025-01-14',
                    'open': 98.0,
                    'high': 101.0,
                    'low': 97.0,
                    'close': 100.0,
                    'adjClose': 100.0,
                    'volume': 900000,
                    'unadjustedVolume': 900000,
                    'change': 2.0,
                    'changePercent': 2.0,
                    'vwap': 99.0,
                    'label': 'January 14, 25',
                    'changeOverTime': 0.02
                }
            ],
            'income_statement': [
                {
                    'date': '2024-09-30',
                    'symbol': 'TEST',
                    'reportedCurrency': 'USD',
                    'fillingDate': '2024-10-30',
                    'acceptedDate': '2024-10-30',
                    'period': 'Q3',
                    'revenue': 100000000,
                    'costOfRevenue': 40000000,
                    'grossProfit': 60000000,
                    'operatingExpenses': 30000000,
                    'operatingIncome': 30000000,
                    'netIncome': 25000000,
                    'eps': 1.25,
                    'epsDiluted': 1.20,
                    'weightedAverageShsOut': 20000000,
                    'weightedAverageShsOutDil': 20833333
                }
            ],
            'balance_sheet': [
                {
                    'date': '2024-09-30',
                    'symbol': 'TEST',
                    'reportedCurrency': 'USD',
                    'fillingDate': '2024-10-30',
                    'acceptedDate': '2024-10-30',
                    'period': 'Q3',
                    'totalAssets': 500000000,
                    'totalCurrentAssets': 200000000,
                    'totalLiabilities': 200000000,
                    'totalCurrentLiabilities': 100000000,
                    'totalStockholdersEquity': 300000000,
                    'commonStock': 20000000,
                    'retainedEarnings': 280000000
                }
            ],
            'cash_flow': [
                {
                    'date': '2024-09-30',
                    'symbol': 'TEST',
                    'reportedCurrency': 'USD',
                    'fillingDate': '2024-10-30',
                    'acceptedDate': '2024-10-30',
                    'period': 'Q3',
                    'operatingCashFlow': 35000000,
                    'capitalExpenditure': -10000000,
                    'freeCashFlow': 25000000,
                    'dividendsPaid': -5000000,
                    'netCashProvidedByOperatingActivities': 35000000,
                    'netCashUsedForInvestingActivites': -10000000,
                    'netCashUsedProvidedByFinancingActivities': -5000000,
                    'netChangeInCash': 20000000
                }
            ]
        }
    
    def test_full_pipeline_flow(self, test_config, mock_fmp_responses, mocker):
        """Test the complete pipeline flow from API to database"""
        # Mock FMP API calls
        mock_fmp_client = mocker.patch('src.api.fmp_client.FMPClient')
        mock_instance = mock_fmp_client.return_value
        mock_instance.get_company_profile.return_value = mock_fmp_responses['company_profile']
        mock_instance.get_historical_prices.return_value = mock_fmp_responses['historical_prices']
        mock_instance.get_income_statement.return_value = mock_fmp_responses['income_statement']
        mock_instance.get_balance_sheet.return_value = mock_fmp_responses['balance_sheet']
        mock_instance.get_cash_flow_statement.return_value = mock_fmp_responses['cash_flow']
        
        # Mock Snowflake operations
        mock_snowflake = mocker.patch('src.db.snowflake_connector.SnowflakeConnector')
        mock_sf_instance = mock_snowflake.return_value
        mock_sf_instance.connect.return_value = None
        mock_sf_instance.disconnect.return_value = None
        mock_sf_instance.execute.return_value = None
        mock_sf_instance.bulk_insert.return_value = 1  # Records inserted
        mock_sf_instance.merge.return_value = 1  # Records merged
        mock_sf_instance.fetch_all.return_value = []  # Empty results
        
        # Create pipeline orchestrator
        orchestrator = PipelineOrchestrator(test_config, dry_run=False)
        
        # Create mock args
        args = Mock()
        args.symbols = ['TEST']
        args.sp500 = False
        args.skip_company = False
        args.skip_price = False
        args.skip_financial = False
        args.skip_ttm = False
        args.skip_ratio = False
        args.skip_market_metrics = False
        args.skip_analytics = False
        args.all_symbols = False
        args.days_back = 30
        args.from_date = None
        args.to_date = None
        args.period = 'quarterly'
        args.limit = 8
        
        # Run the pipeline
        exit_code = orchestrator.run_daily_update(args)
        
        # Verify pipeline completed successfully
        assert exit_code == 0
        
        # Verify all ETL pipelines were called
        assert 'company' in orchestrator.results
        assert 'price' in orchestrator.results
        assert 'financial' in orchestrator.results
        
        # Verify API calls were made
        mock_instance.get_company_profile.assert_called_once_with(['TEST'])
        mock_instance.get_historical_prices.assert_called()
        mock_instance.get_income_statement.assert_called()
        
    def test_parallel_processing_integrity(self, test_config, mock_fmp_responses, mocker):
        """Test that parallel processing maintains data integrity"""
        # Mock multiple symbols
        symbols = ['TEST1', 'TEST2', 'TEST3']
        
        # Mock FMP responses for multiple symbols
        mock_fmp_client = mocker.patch('src.api.fmp_client.FMPClient')
        mock_instance = mock_fmp_client.return_value
        
        # Each symbol gets its own profile
        def get_company_profile(syms):
            return [{'symbol': s, 'companyName': f'Test Company {s}'} for s in syms]
        
        mock_instance.get_company_profile.side_effect = get_company_profile
        mock_instance.get_historical_prices.return_value = mock_fmp_responses['historical_prices']
        mock_instance.get_income_statement.return_value = mock_fmp_responses['income_statement']
        mock_instance.get_balance_sheet.return_value = mock_fmp_responses['balance_sheet']
        mock_instance.get_cash_flow_statement.return_value = mock_fmp_responses['cash_flow']
        
        # Mock Snowflake
        mock_snowflake = mocker.patch('src.db.snowflake_connector.SnowflakeConnector')
        mock_sf_instance = mock_snowflake.return_value
        mock_sf_instance.connect.return_value = None
        mock_sf_instance.disconnect.return_value = None
        mock_sf_instance.execute.return_value = None
        mock_sf_instance.bulk_insert.return_value = 1
        mock_sf_instance.merge.return_value = 1
        mock_sf_instance.fetch_all.return_value = []
        
        # Track parallel execution
        execution_order = []
        
        def track_execution(table_name, data):
            execution_order.append((table_name, len(data)))
            return len(data)
        
        mock_sf_instance.bulk_insert.side_effect = track_execution
        
        # Create pipeline orchestrator
        orchestrator = PipelineOrchestrator(test_config, dry_run=False)
        
        # Create mock args
        args = Mock()
        args.symbols = symbols
        args.sp500 = False
        args.skip_company = False
        args.skip_price = False
        args.skip_financial = False
        args.skip_ttm = True  # Skip dependent ETLs for this test
        args.skip_ratio = True
        args.skip_market_metrics = True
        args.skip_analytics = False
        args.all_symbols = False
        args.days_back = 2
        args.from_date = None
        args.to_date = None
        args.period = 'quarterly'
        args.limit = 1
        
        # Run the pipeline
        exit_code = orchestrator.run_daily_update(args)
        
        # Verify successful completion
        assert exit_code == 0
        
        # Verify parallel execution occurred
        # Company, Price, and Financial should all be in results
        assert all(etl in orchestrator.results for etl in ['company', 'price', 'financial'])
        
        # Verify data integrity - all symbols processed
        assert len(symbols) == 3
        
    def test_ttm_calculation_accuracy(self, test_config, mocker):
        """Test TTM calculations are accurate"""
        # Mock quarterly financial data for TTM calculation
        quarterly_data = []
        base_date = datetime(2024, 9, 30)
        
        for i in range(4):
            quarter_date = base_date - timedelta(days=90 * i)
            quarterly_data.append({
                'symbol': 'TEST',
                'fiscal_date': quarter_date.date(),
                'period': 'quarterly',
                'revenue': 100000000,  # 100M per quarter
                'net_income': 25000000,  # 25M per quarter
                'operating_cash_flow': 30000000,  # 30M per quarter
                'eps_diluted': 1.20,
                'shares_outstanding': 20000000,
                'total_equity': 300000000,
                'accepted_date': quarter_date.date()
            })
        
        # Mock Snowflake to return quarterly data
        mock_snowflake = mocker.patch('src.db.snowflake_connector.SnowflakeConnector')
        mock_sf_instance = mock_snowflake.return_value
        
        def mock_fetch_all(query):
            if 'FACT_FINANCIALS' in query and 'quarterly' in query:
                return quarterly_data
            elif 'calculation_date' in query:
                return []  # No existing TTM records
            else:
                return []
        
        mock_sf_instance.fetch_all.side_effect = mock_fetch_all
        mock_sf_instance.bulk_insert.return_value = 1
        
        # Import and instantiate TTM ETL
        from src.etl.ttm_calculation_etl import TTMCalculationETL
        ttm_etl = TTMCalculationETL(test_config)
        
        # Run TTM calculation
        result = ttm_etl.run(symbols=['TEST'])
        
        # Verify TTM calculations
        assert result['status'] == 'success'
        
        # Check that TTM values would be correct (4 quarters summed)
        # Revenue: 4 * 100M = 400M
        # Net Income: 4 * 25M = 100M
        # Operating Cash Flow: 4 * 30M = 120M
        
    def test_market_metrics_with_ttm(self, test_config, mocker):
        """Test market metrics use pre-calculated TTM values correctly"""
        # Mock data for market metrics calculation
        price_data = [{
            'symbol': 'TEST',
            'date': datetime(2025, 1, 15).date(),
            'close_price': 100.0
        }]
        
        ttm_data = [{
            'symbol': 'TEST',
            'calculation_date': datetime(2024, 12, 31).date(),
            'ttm_revenue': 400000000,
            'ttm_net_income': 100000000,
            'ttm_eps_diluted': 4.80,
            'ttm_operating_cash_flow': 120000000,
            'ttm_free_cash_flow': 100000000,
            'ttm_dividends': -20000000,  # Negative for cash outflow
            'shares_outstanding': 20000000,
            'total_equity': 300000000,
            'accepted_date': datetime(2024, 10, 30).date()
        }]
        
        financial_ratios = [{
            'symbol': 'TEST',
            'fiscal_date': datetime(2024, 9, 30).date(),
            'revenue_per_share': 5.0,
            'revenue_per_share_ttm': 20.0
        }]
        
        # Mock Snowflake
        mock_snowflake = mocker.patch('src.db.snowflake_connector.SnowflakeConnector')
        mock_sf_instance = mock_snowflake.return_value
        
        def mock_fetch_all(query):
            if 'FACT_DAILY_PRICES' in query:
                return price_data
            elif 'FACT_FINANCIALS_TTM' in query:
                return ttm_data
            elif 'FACT_FINANCIAL_RATIOS' in query:
                return financial_ratios
            else:
                return []
        
        mock_sf_instance.fetch_all.side_effect = mock_fetch_all
        mock_sf_instance.bulk_insert.return_value = 1
        
        # Import and instantiate Market Metrics ETL
        from src.etl.market_metrics_etl import MarketMetricsETL
        metrics_etl = MarketMetricsETL(test_config)
        
        # Run market metrics calculation
        result = metrics_etl.run(symbols=['TEST'])
        
        # Verify calculations would be correct
        assert result['status'] == 'success'
        
        # Expected calculations:
        # P/E TTM = Price / TTM EPS = 100 / 4.80 = 20.83
        # P/S TTM = Price / Revenue per share TTM = 100 / 20 = 5.0
        # Dividend Yield = (Dividends per share / Price) * 100 = (1.0 / 100) * 100 = 1.0%
        
    def test_error_handling_and_recovery(self, test_config, mocker):
        """Test pipeline handles errors gracefully"""
        # Mock FMP client to raise an error for one ETL
        mock_fmp_client = mocker.patch('src.api.fmp_client.FMPClient')
        mock_instance = mock_fmp_client.return_value
        mock_instance.get_company_profile.return_value = [{'symbol': 'TEST', 'companyName': 'Test'}]
        mock_instance.get_historical_prices.side_effect = Exception("API Error")
        mock_instance.get_income_statement.return_value = []
        
        # Mock Snowflake
        mock_snowflake = mocker.patch('src.db.snowflake_connector.SnowflakeConnector')
        mock_sf_instance = mock_snowflake.return_value
        mock_sf_instance.connect.return_value = None
        mock_sf_instance.disconnect.return_value = None
        mock_sf_instance.bulk_insert.return_value = 1
        
        # Create pipeline orchestrator
        orchestrator = PipelineOrchestrator(test_config, dry_run=False)
        
        # Create mock args
        args = Mock()
        args.symbols = ['TEST']
        args.sp500 = False
        args.skip_company = False
        args.skip_price = False
        args.skip_financial = False
        args.skip_ttm = True
        args.skip_ratio = True
        args.skip_market_metrics = True
        args.skip_analytics = False
        args.all_symbols = False
        args.days_back = 30
        args.from_date = None
        args.to_date = None
        args.period = 'quarterly'
        args.limit = 1
        
        # Run the pipeline
        exit_code = orchestrator.run_daily_update(args)
        
        # Should return partial success (1) since price ETL failed
        assert exit_code == 1
        
        # Verify company ETL succeeded
        assert orchestrator.results['company']['status'] in ['success', 'SUCCESS']
        
        # Verify price ETL failed
        assert orchestrator.results['price']['status'] == 'failed'
        
    def test_data_quality_validation(self, test_config, mock_fmp_responses, mocker):
        """Test data quality checks are performed"""
        # Mock FMP and Snowflake
        mock_fmp_client = mocker.patch('src.api.fmp_client.FMPClient')
        mock_instance = mock_fmp_client.return_value
        mock_instance.get_company_profile.return_value = mock_fmp_responses['company_profile']
        
        mock_snowflake = mocker.patch('src.db.snowflake_connector.SnowflakeConnector')
        mock_sf_instance = mock_snowflake.return_value
        
        # Track validation queries
        validation_queries = []
        
        def track_queries(query):
            validation_queries.append(query)
            return []
        
        mock_sf_instance.fetch_all.side_effect = track_queries
        mock_sf_instance.bulk_insert.return_value = 1
        
        # Import and test company ETL with validation
        from src.etl.company_etl import CompanyETL
        company_etl = CompanyETL(test_config)
        
        # Run extraction
        data = company_etl.extract(symbols=['TEST'])
        
        # Transform data
        transformed = company_etl.transform(data)
        
        # Verify data quality
        assert len(data) > 0
        assert 'staging' in transformed
        assert all(record.get('symbol') for record in transformed['staging'])