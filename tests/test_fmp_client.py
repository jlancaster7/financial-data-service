import pytest
import requests
from unittest.mock import Mock, patch, MagicMock
from datetime import date
from src.api.fmp_client import FMPClient, FMPAPIError
from src.utils.config import FMPConfig


@pytest.fixture
def mock_config():
    return FMPConfig(
        api_key="test_api_key",
        base_url="https://api.test.com/v3",
        rate_limit_calls=300,
        rate_limit_period=60
    )


@pytest.fixture
def mock_response():
    response = MagicMock()
    response.status_code = 200
    response.raise_for_status = MagicMock()
    return response


class TestFMPClient:
    
    def test_build_url(self, mock_config):
        client = FMPClient(mock_config)
        url = client._build_url("profile/AAPL")
        assert url == "https://api.test.com/v3/profile/AAPL"
    
    def test_add_api_key(self, mock_config):
        client = FMPClient(mock_config)
        params = client._add_api_key({"param1": "value1"})
        assert params == {"param1": "value1", "apikey": "test_api_key"}
    
    @patch('requests.Session.get')
    def test_get_company_profile_success(self, mock_get, mock_config, mock_response):
        mock_response.json.return_value = [{
            "symbol": "AAPL",
            "companyName": "Apple Inc.",
            "sector": "Technology"
        }]
        mock_get.return_value = mock_response
        
        client = FMPClient(mock_config)
        result = client.get_company_profile("AAPL")
        
        assert result["symbol"] == "AAPL"
        assert result["companyName"] == "Apple Inc."
        mock_get.assert_called_once()
    
    @patch('requests.Session.get')
    def test_get_company_profile_not_found(self, mock_get, mock_config, mock_response):
        mock_response.json.return_value = []
        mock_get.return_value = mock_response
        
        client = FMPClient(mock_config)
        with pytest.raises(FMPAPIError, match="No company profile found"):
            client.get_company_profile("INVALID")
    
    @patch('requests.Session.get')
    def test_get_historical_prices_success(self, mock_get, mock_config, mock_response):
        mock_response.json.return_value = {
            "symbol": "AAPL",
            "historical": [
                {"date": "2023-01-01", "close": 150.0},
                {"date": "2023-01-02", "close": 151.0}
            ]
        }
        mock_get.return_value = mock_response
        
        client = FMPClient(mock_config)
        result = client.get_historical_prices("AAPL", from_date="2023-01-01")
        
        assert len(result) == 2
        assert result[0]["close"] == 150.0
    
    @patch('requests.Session.get')
    def test_api_error_response(self, mock_get, mock_config, mock_response):
        mock_response.json.return_value = {"Error Message": "Invalid API key"}
        mock_get.return_value = mock_response
        
        client = FMPClient(mock_config)
        with pytest.raises(FMPAPIError, match="API Error: Invalid API key"):
            client.get_company_profile("AAPL")
    
    @patch('requests.Session.get')
    def test_http_error_401(self, mock_get, mock_config):
        mock_response = MagicMock()
        mock_response.status_code = 401
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError(response=mock_response)
        mock_get.return_value = mock_response
        
        client = FMPClient(mock_config)
        with pytest.raises(FMPAPIError, match="Invalid API key"):
            client.get_company_profile("AAPL")
    
    @patch('requests.Session.get')
    def test_batch_get_company_profiles(self, mock_get, mock_config):
        # Create different mock responses for each symbol
        mock_responses = {
            "AAPL": [{"symbol": "AAPL", "companyName": "Apple Inc."}],
            "MSFT": [{"symbol": "MSFT", "companyName": "Microsoft Corporation"}],
            "GOOGL": [{"symbol": "GOOGL", "companyName": "Alphabet Inc."}]
        }
        
        def side_effect(url, **kwargs):
            # Extract symbol from params
            symbol = kwargs.get('params', {}).get('symbol', '')
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.raise_for_status = MagicMock()
            mock_response.json.return_value = mock_responses.get(symbol, [])
            return mock_response
        
        mock_get.side_effect = side_effect
        
        client = FMPClient(mock_config)
        symbols = ["AAPL", "MSFT", "GOOGL"]
        result = client.batch_get_company_profiles(symbols)
        
        assert len(result) == 3
        assert result["AAPL"]["companyName"] == "Apple Inc."
        assert result["MSFT"]["companyName"] == "Microsoft Corporation"
        assert result["GOOGL"]["companyName"] == "Alphabet Inc."
    
    @patch('requests.Session.get')
    def test_get_income_statement(self, mock_get, mock_config, mock_response):
        mock_response.json.return_value = [
            {"date": "2023-12-31", "revenue": 1000000, "netIncome": 200000}
        ]
        mock_get.return_value = mock_response
        
        client = FMPClient(mock_config)
        result = client.get_income_statement("AAPL", period="annual", limit=1)
        
        assert len(result) == 1
        assert result[0]["revenue"] == 1000000