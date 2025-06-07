"""
Unit tests for data transformation modules
"""
import pytest
from datetime import date, datetime, timezone
from src.transformations.fmp_transformer import FMPTransformer
from src.transformations.data_quality import DataQualityValidator


class TestFMPTransformer:
    """Test FMP data transformer"""
    
    @pytest.fixture
    def transformer(self):
        """Create transformer instance"""
        return FMPTransformer()
    
    @pytest.fixture
    def sample_profile(self):
        """Sample company profile data"""
        return {
            'symbol': 'AAPL',
            'companyName': 'Apple Inc.',
            'sector': 'Technology',
            'industry': 'Consumer Electronics',
            'exchange': 'NASDAQ',
            'marketCap': 3000000000000,
            'description': 'Apple designs and manufactures consumer electronics',
            'website': 'https://apple.com',
            'ceo': 'Tim Cook',
            'fullTimeEmployees': 164000,
            'city': 'Cupertino',
            'state': 'California',
            'country': 'US'
        }
    
    @pytest.fixture
    def sample_price(self):
        """Sample historical price data"""
        return {
            'date': '2024-01-02',
            'open': 150.25,
            'high': 152.50,
            'low': 149.80,
            'close': 151.75,
            'adjClose': 151.75,
            'volume': 50000000,
            'changePercent': 1.25
        }
    
    @pytest.fixture
    def sample_income_statement(self):
        """Sample income statement data"""
        return {
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
        }
    
    def test_transform_company_profile(self, transformer, sample_profile):
        """Test company profile transformation"""
        result = transformer.transform_company_profile(sample_profile)
        
        assert 'raw' in result
        assert 'staging' in result
        assert len(result['raw']) == 1
        assert len(result['staging']) == 1
        
        # Check raw record
        raw = result['raw'][0]
        assert raw['symbol'] == 'AAPL'
        assert 'raw_data' in raw
        assert raw['api_source'] == 'FMP'
        assert 'loaded_timestamp' in raw
        
        # Check staging record
        staging = result['staging'][0]
        assert staging['symbol'] == 'AAPL'
        assert staging['company_name'] == 'Apple Inc.'
        assert staging['sector'] == 'Technology'
        assert staging['employees'] == 164000
    
    def test_transform_historical_prices(self, transformer, sample_price):
        """Test historical price transformation"""
        result = transformer.transform_historical_prices('AAPL', [sample_price])
        
        assert 'raw' in result
        assert 'staging' in result
        assert len(result['raw']) == 1
        assert len(result['staging']) == 1
        
        # Check raw record
        raw = result['raw'][0]
        assert raw['symbol'] == 'AAPL'
        assert raw['price_date'] == date(2024, 1, 2)
        assert 'raw_data' in raw
        
        # Check staging record
        staging = result['staging'][0]
        assert staging['symbol'] == 'AAPL'
        assert staging['open_price'] == 150.25
        assert staging['close_price'] == 151.75
        assert staging['volume'] == 50000000
    
    def test_transform_income_statements(self, transformer, sample_income_statement):
        """Test income statement transformation"""
        result = transformer.transform_income_statements([sample_income_statement])
        
        assert 'raw' in result
        assert 'staging' in result
        assert len(result['raw']) == 1
        assert len(result['staging']) == 1
        
        # Check staging record
        staging = result['staging'][0]
        assert staging['symbol'] == 'AAPL'
        assert staging['fiscal_date'] == date(2023, 9, 30)
        assert staging['revenue'] == 383285000000
        assert staging['net_income'] == 96995000000
    
    def test_transform_batch(self, transformer, sample_profile):
        """Test batch transformation"""
        result = transformer.transform_batch('profile', sample_profile)
        assert 'raw' in result
        assert 'staging' in result
    
    def test_invalid_data_handling(self, transformer):
        """Test handling of invalid data"""
        # Profile without symbol
        invalid_profile = {'companyName': 'Test Corp'}
        result = transformer.transform_company_profile(invalid_profile)
        assert len(result['raw']) == 0
        assert len(result['staging']) == 0
        
        # Price without date
        invalid_price = {'open': 100, 'close': 101}
        result = transformer.transform_historical_prices('TEST', [invalid_price])
        assert len(result['raw']) == 0
        assert len(result['staging']) == 0
    
    def test_transformation_stats(self, transformer, sample_profile):
        """Test transformation statistics"""
        transformer.reset_stats()
        
        # Process valid data
        transformer.transform_company_profile(sample_profile)
        stats = transformer.get_stats()
        assert stats['records_processed'] == 1
        assert stats['records_failed'] == 0
        
        # Process invalid data
        transformer.transform_company_profile({'invalid': 'data'})
        stats = transformer.get_stats()
        assert stats['records_processed'] == 1
        assert stats['records_failed'] == 1


class TestDataQualityValidator:
    """Test data quality validator"""
    
    @pytest.fixture
    def validator(self):
        """Create validator instance"""
        return DataQualityValidator()
    
    def test_validate_company_profile(self, validator):
        """Test company profile validation"""
        # Valid profile
        valid_profile = {
            'symbol': 'AAPL',
            'company_name': 'Apple Inc.',
            'market_cap': 3000000000000,
            'employees': 164000
        }
        is_valid, issues = validator.validate_company_profile(valid_profile)
        assert is_valid
        assert len(issues) == 0
        
        # Invalid profile - missing required fields
        invalid_profile = {
            'market_cap': 3000000000000
        }
        is_valid, issues = validator.validate_company_profile(invalid_profile)
        assert not is_valid
        assert 'Missing required field: symbol' in issues
        
        # Invalid profile - bad data
        invalid_profile = {
            'symbol': 'TOOLONGSYMBOL',
            'company_name': 'Test',
            'market_cap': -1000,
            'employees': -10
        }
        is_valid, issues = validator.validate_company_profile(invalid_profile)
        assert not is_valid
        assert any('Invalid symbol format' in issue for issue in issues)
        assert any('Invalid market cap' in issue for issue in issues)
        assert any('Invalid employee count' in issue for issue in issues)
    
    def test_validate_historical_price(self, validator):
        """Test historical price validation"""
        # Valid price
        valid_price = {
            'symbol': 'AAPL',
            'price_date': date(2024, 1, 2),
            'open_price': 150.25,
            'high_price': 152.50,
            'low_price': 149.80,
            'close_price': 151.75,
            'volume': 50000000
        }
        is_valid, issues = validator.validate_historical_price(valid_price)
        assert is_valid
        assert len(issues) == 0
        
        # Invalid price - negative values
        invalid_price = {
            'symbol': 'AAPL',
            'price_date': date(2024, 1, 2),
            'close_price': -100,
            'volume': -1000
        }
        is_valid, issues = validator.validate_historical_price(invalid_price)
        assert not is_valid
        assert any('Negative price' in issue for issue in issues)
        assert any('Negative volume' in issue for issue in issues)
        
        # Invalid price - illogical values
        invalid_price = {
            'symbol': 'AAPL',
            'price_date': date(2024, 1, 2),
            'high_price': 100,
            'low_price': 150,  # Low > High
            'close_price': 125
        }
        is_valid, issues = validator.validate_historical_price(invalid_price)
        assert not is_valid
        assert any('Low price' in issue and 'greater than high price' in issue for issue in issues)
    
    def test_validate_financial_statement(self, validator):
        """Test financial statement validation"""
        # Valid income statement
        valid_income = {
            'symbol': 'AAPL',
            'fiscal_date': date(2023, 9, 30),
            'period': 'FY',
            'revenue': 383285000000,
            'cost_of_revenue': 214137000000,
            'gross_profit': 169148000000
        }
        is_valid, issues = validator.validate_financial_statement(valid_income, 'income')
        assert is_valid
        assert len(issues) == 0
        
        # Invalid balance sheet - equation violation
        invalid_balance = {
            'symbol': 'AAPL',
            'fiscal_date': date(2023, 9, 30),
            'period': 'FY',
            'total_assets': 1000000,
            'total_liabilities': 600000,
            'total_equity': 500000  # Should be 400000
        }
        is_valid, issues = validator.validate_financial_statement(invalid_balance, 'balance')
        assert not is_valid
        assert any('Balance sheet equation violation' in issue for issue in issues)
    
    def test_validate_batch(self, validator):
        """Test batch validation"""
        records = [
            {'symbol': 'AAPL', 'company_name': 'Apple Inc.'},
            {'symbol': 'MSFT', 'company_name': 'Microsoft Corp.'},
            {'company_name': 'Missing Symbol Inc.'}  # Invalid
        ]
        
        result = validator.validate_batch(records, 'profile')
        assert result['total_records'] == 3
        assert result['valid_records'] == 2
        assert result['invalid_records'] == 1
        assert len(result['issues_by_record']) == 1