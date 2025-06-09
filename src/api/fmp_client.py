import requests
from typing import Dict, List, Any, Optional, Union
from datetime import datetime, date
from urllib.parse import urljoin
from ratelimit import limits, sleep_and_retry
from loguru import logger
from src.utils.config import FMPConfig


class FMPAPIError(Exception):
    """Custom exception for FMP API errors"""
    pass


class FMPClient:
    """
    FMP API Client configured for the /stable/ API endpoints
    Based on the official FMP API documentation
    """
    def __init__(self, config: FMPConfig):
        self.config = config
        self.base_url = config.base_url
        self.api_key = config.api_key
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'FinancialDataService/1.0'
        })
    
    def _build_url(self, endpoint: str) -> str:
        """Build full URL for endpoint"""
        # Ensure base URL ends with /
        base = self.base_url.rstrip('/') + '/'
        # Remove leading slash from endpoint if present
        endpoint = endpoint.lstrip('/')
        return urljoin(base, endpoint)
    
    def _add_api_key(self, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Add API key to parameters"""
        if params is None:
            params = {}
        params['apikey'] = self.api_key
        return params
    
    @sleep_and_retry
    @limits(calls=300, period=60)  # 300 calls per minute
    def _make_request(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> Union[Dict, List]:
        """Make API request with rate limiting and error handling"""
        url = self._build_url(endpoint)
        params = self._add_api_key(params)
        
        try:
            logger.debug(f"Making request to {endpoint} with params: {params}")
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            # Check for API errors in response
            if isinstance(data, dict) and "Error Message" in data:
                raise FMPAPIError(f"API Error: {data['Error Message']}")
            
            return data
            
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:
                logger.warning("Rate limit exceeded, waiting...")
                raise  # Let rate limiter handle retry
            elif e.response.status_code == 401:
                raise FMPAPIError("Invalid API key")
            else:
                raise FMPAPIError(f"HTTP Error: {e}")
        except requests.exceptions.RequestException as e:
            raise FMPAPIError(f"Request failed: {e}")
        except ValueError as e:
            raise FMPAPIError(f"Invalid JSON response: {e}")
    
    def get_company_profile(self, symbol: str) -> Dict[str, Any]:
        """
        Get company profile data
        Endpoint: profile?symbol=AAPL
        """
        endpoint = "profile"
        params = {'symbol': symbol}
        
        data = self._make_request(endpoint, params)
        
        if isinstance(data, list) and len(data) > 0:
            return data[0]
        else:
            raise FMPAPIError(f"No company profile found for {symbol}")
    
    def get_historical_prices(self, 
                            symbol: str, 
                            from_date: Optional[Union[str, date]] = None,
                            to_date: Optional[Union[str, date]] = None) -> List[Dict[str, Any]]:
        """
        Get historical price data
        Endpoint: historical-price-eod/full?symbol=AAPL
        """
        endpoint = "historical-price-eod/full"
        params = {'symbol': symbol}
        
        if from_date:
            params['from'] = str(from_date)
        if to_date:
            params['to'] = str(to_date)
        
        data = self._make_request(endpoint, params)
        
        # Handle both possible response formats
        if isinstance(data, list):
            return data
        elif isinstance(data, dict) and 'historical' in data:
            return data['historical']
        else:
            raise FMPAPIError(f"No historical price data found for {symbol}")
    
    def get_income_statement(self, 
                           symbol: str, 
                           period: str = 'annual',
                           limit: int = 5) -> List[Dict[str, Any]]:
        """
        Get income statement data
        Endpoint: income-statement?symbol=AAPL
        """
        endpoint = "income-statement"
        params = {
            'symbol': symbol,
            'limit': limit
        }
        
        # Handle period parameter
        if period.lower() == 'annual':
            params['period'] = 'FY'
        elif period.lower() == 'quarterly':
            # Use 'quarter' to get recent quarterly data
            params['period'] = 'quarter'
        else:
            params['period'] = period
        
        data = self._make_request(endpoint, params)
        
        if isinstance(data, list):
            return data
        else:
            raise FMPAPIError(f"No income statement data found for {symbol}")
    
    def get_balance_sheet(self, 
                         symbol: str, 
                         period: str = 'annual',
                         limit: int = 5) -> List[Dict[str, Any]]:
        """
        Get balance sheet data
        Endpoint: balance-sheet-statement?symbol=AAPL
        """
        endpoint = "balance-sheet-statement"
        params = {
            'symbol': symbol,
            'limit': limit
        }
        
        if period.lower() == 'annual':
            params['period'] = 'FY'
        elif period.lower() == 'quarterly':
            params['period'] = 'quarter'
        else:
            params['period'] = period
        
        data = self._make_request(endpoint, params)
        
        if isinstance(data, list):
            return data
        else:
            raise FMPAPIError(f"No balance sheet data found for {symbol}")
    
    def get_cash_flow(self, 
                     symbol: str, 
                     period: str = 'annual',
                     limit: int = 5) -> List[Dict[str, Any]]:
        """
        Get cash flow statement data
        Endpoint: cash-flow-statement?symbol=AAPL
        """
        endpoint = "cash-flow-statement"
        params = {
            'symbol': symbol,
            'limit': limit
        }
        
        if period.lower() == 'annual':
            params['period'] = 'FY'
        elif period.lower() == 'quarterly':
            params['period'] = 'quarter'
        else:
            params['period'] = period
        
        data = self._make_request(endpoint, params)
        
        if isinstance(data, list):
            return data
        else:
            raise FMPAPIError(f"No cash flow data found for {symbol}")
    
    def get_financial_ratios_ttm(self, symbol: str) -> Dict[str, Any]:
        """
        Get trailing twelve-month financial ratios
        Endpoint: ratios-ttm?symbol=AAPL
        """
        endpoint = "ratios-ttm"
        params = {'symbol': symbol}
        
        data = self._make_request(endpoint, params)
        
        if isinstance(data, list) and len(data) > 0:
            return data[0]
        else:
            raise FMPAPIError(f"No financial ratios found for {symbol}")
    
    def get_key_metrics_ttm(self, symbol: str) -> Dict[str, Any]:
        """
        Get trailing twelve-month key metrics
        Endpoint: key-metrics-ttm?symbol=AAPL
        """
        endpoint = "key-metrics-ttm"
        params = {'symbol': symbol}
        
        data = self._make_request(endpoint, params)
        
        if isinstance(data, list) and len(data) > 0:
            return data[0]
        else:
            raise FMPAPIError(f"No key metrics found for {symbol}")
    
    def get_historical_market_cap(self, 
                                 symbol: str,
                                 limit: int = 100,
                                 from_date: Optional[Union[str, date]] = None,
                                 to_date: Optional[Union[str, date]] = None) -> List[Dict[str, Any]]:
        """
        Get historical market capitalization data
        Endpoint: historical-market-capitalization?symbol=AAPL
        """
        endpoint = "historical-market-capitalization"
        params = {
            'symbol': symbol,
            'limit': limit
        }
        
        if from_date:
            params['from'] = str(from_date)
        if to_date:
            params['to'] = str(to_date)
        
        data = self._make_request(endpoint, params)
        
        if isinstance(data, list):
            return data
        else:
            raise FMPAPIError(f"No historical market cap data found for {symbol}")
    
    def get_sp500_constituents(self) -> List[Dict[str, Any]]:
        """
        Get list of S&P 500 constituents
        Endpoint: sp500-constituent
        """
        endpoint = "sp500-constituent"
        data = self._make_request(endpoint)
        
        if isinstance(data, list):
            return data
        else:
            raise FMPAPIError("Failed to get S&P 500 constituents")
    
    def get_treasury_rates(self,
                          from_date: Optional[Union[str, date]] = None,
                          to_date: Optional[Union[str, date]] = None) -> List[Dict[str, Any]]:
        """
        Get Treasury rates for all maturities
        Endpoint: treasury-rates
        Note: Maximum 90-day date range
        """
        endpoint = "treasury-rates"
        params = {}
        
        if from_date:
            params['from'] = str(from_date)
        if to_date:
            params['to'] = str(to_date)
        
        data = self._make_request(endpoint, params)
        
        if isinstance(data, list):
            return data
        else:
            raise FMPAPIError("Failed to get Treasury rates")
    
    def get_economic_indicator(self,
                              indicator_name: str,
                              from_date: Optional[Union[str, date]] = None,
                              to_date: Optional[Union[str, date]] = None) -> List[Dict[str, Any]]:
        """
        Get economic indicator data
        Endpoint: economic-indicators?name=GDP
        Available indicators: GDP, CPI, unemploymentRate, etc.
        """
        endpoint = "economic-indicators"
        params = {'name': indicator_name}
        
        if from_date:
            params['from'] = str(from_date)
        if to_date:
            params['to'] = str(to_date)
        
        data = self._make_request(endpoint, params)
        
        if isinstance(data, list):
            return data
        else:
            raise FMPAPIError(f"Failed to get economic indicator: {indicator_name}")
    
    def get_sector_pe_snapshot(self,
                              date: Union[str, date],
                              exchange: Optional[str] = None,
                              sector: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Get sector P/E ratios for a specific date
        Endpoint: sector-pe-snapshot?date=2024-02-01
        """
        endpoint = "sector-pe-snapshot"
        params = {'date': str(date)}
        
        if exchange:
            params['exchange'] = exchange
        if sector:
            params['sector'] = sector
        
        data = self._make_request(endpoint, params)
        
        if isinstance(data, list):
            return data
        else:
            raise FMPAPIError("Failed to get sector P/E snapshot")
    
    def batch_get_company_profiles(self, symbols: List[str]) -> Dict[str, Dict[str, Any]]:
        """Get multiple company profiles"""
        results = {}
        
        # The stable API might not support batch profile requests
        # Get profiles individually
        for symbol in symbols:
            try:
                results[symbol] = self.get_company_profile(symbol)
            except FMPAPIError as e:
                logger.error(f"Failed to get profile for {symbol}: {e}")
                continue
        
        return results
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.session.close()