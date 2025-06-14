"""
Data models for FMP API responses and transformations
"""
from dataclasses import dataclass
from typing import Dict, Any, Optional, List
from datetime import date, datetime, timezone
import json


class DateTimeEncoder(json.JSONEncoder):
    """Custom JSON encoder for datetime objects"""
    def default(self, obj):
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        return super().default(obj)


@dataclass
class CompanyProfile:
    """Model for company profile data"""
    symbol: str
    company_name: str
    sector: Optional[str]
    industry: Optional[str]
    exchange: Optional[str]
    market_cap: Optional[float]
    description: Optional[str]
    website: Optional[str]
    ceo: Optional[str]
    employees: Optional[int]
    city: Optional[str]
    state: Optional[str]
    country: Optional[str]
    
    @classmethod
    def from_fmp_response(cls, data: Dict[str, Any]) -> 'CompanyProfile':
        """Create CompanyProfile from FMP API response"""
        return cls(
            symbol=data.get('symbol'),
            company_name=data.get('companyName'),
            sector=data.get('sector'),
            industry=data.get('industry'),
            exchange=data.get('exchange'),
            market_cap=data.get('marketCap'),
            description=data.get('description'),
            website=data.get('website'),
            ceo=data.get('ceo'),
            employees=int(data.get('fullTimeEmployees', 0)) if data.get('fullTimeEmployees') else None,
            city=data.get('city'),
            state=data.get('state'),
            country=data.get('country')
        )
    
    def to_raw_record(self) -> Dict[str, Any]:
        """Convert to raw table record format"""
        return {
            'symbol': self.symbol,
            'raw_data': json.dumps(self.__dict__, cls=DateTimeEncoder),  # Convert to JSON string for VARIANT
            'api_source': 'FMP',
            'loaded_timestamp': datetime.now(timezone.utc)
        }
    
    def to_staging_record(self) -> Dict[str, Any]:
        """Convert to staging table record format"""
        return {
            'symbol': self.symbol,
            'company_name': self.company_name,
            'sector': self.sector,
            'industry': self.industry,
            'exchange': self.exchange,
            'market_cap': self.market_cap,
            'description': self.description,
            'website': self.website,
            'ceo': self.ceo,
            'employees': self.employees,
            'headquarters_city': self.city,
            'headquarters_state': self.state,
            'headquarters_country': self.country,
            'loaded_timestamp': datetime.now(timezone.utc)
        }


@dataclass
class HistoricalPrice:
    """Model for historical price data"""
    symbol: str
    price_date: date
    open_price: float
    high_price: float
    low_price: float
    close_price: float
    adj_close: float
    volume: int
    change_percent: Optional[float]
    
    @classmethod
    def from_fmp_response(cls, symbol: str, data: Dict[str, Any]) -> 'HistoricalPrice':
        """Create HistoricalPrice from FMP API response"""
        return cls(
            symbol=symbol,
            price_date=datetime.strptime(data.get('date'), '%Y-%m-%d').date(),
            open_price=float(data.get('open', 0)),
            high_price=float(data.get('high', 0)),
            low_price=float(data.get('low', 0)),
            close_price=float(data.get('close', 0)),
            adj_close=float(data.get('adjClose', data.get('close', 0))),
            volume=int(data.get('volume', 0)),
            change_percent=data.get('changePercent')
        )
    
    def to_raw_record(self) -> Dict[str, Any]:
        """Convert to raw table record format"""
        return {
            'symbol': self.symbol,
            'price_date': self.price_date,
            'raw_data': json.dumps({
                'open': self.open_price,
                'high': self.high_price,
                'low': self.low_price,
                'close': self.close_price,
                'adjClose': self.adj_close,
                'volume': self.volume,
                'changePercent': self.change_percent
            }, cls=DateTimeEncoder),
            'api_source': 'FMP',
            'loaded_timestamp': datetime.now(timezone.utc)
        }
    
    def to_staging_record(self) -> Dict[str, Any]:
        """Convert to staging table record format"""
        return {
            'symbol': self.symbol,
            'price_date': self.price_date,
            'open_price': self.open_price,
            'high_price': self.high_price,
            'low_price': self.low_price,
            'close_price': self.close_price,
            'adj_close': self.adj_close,
            'volume': self.volume,
            'change_percent': self.change_percent,
            'loaded_timestamp': datetime.now(timezone.utc)
        }


@dataclass
class FinancialStatement:
    """Base model for financial statements"""
    symbol: str
    fiscal_date: date
    period: str
    reported_currency: str
    filing_date: Optional[date]
    accepted_date: Optional[datetime]
    
    def to_raw_base(self) -> Dict[str, Any]:
        """Base raw record format"""
        return {
            'symbol': self.symbol,
            'fiscal_date': self.fiscal_date,
            'period': self.period,
            'api_source': 'FMP',
            'loaded_timestamp': datetime.now(timezone.utc)
        }


@dataclass
class IncomeStatement(FinancialStatement):
    """Model for income statement data"""
    revenue: Optional[float]
    cost_of_revenue: Optional[float]
    gross_profit: Optional[float]
    operating_expenses: Optional[float]
    operating_income: Optional[float]
    net_income: Optional[float]
    eps: Optional[float]
    eps_diluted: Optional[float]
    shares_outstanding: Optional[float]
    shares_outstanding_diluted: Optional[float]
    
    @classmethod
    def from_fmp_response(cls, data: Dict[str, Any]) -> 'IncomeStatement':
        """Create IncomeStatement from FMP API response"""
        # Parse filing date
        filing_date = None
        if data.get('filingDate'):
            filing_date = datetime.strptime(data.get('filingDate'), '%Y-%m-%d').date()
        
        # Parse accepted date (includes time)
        accepted_date = None
        if data.get('acceptedDate'):
            accepted_date = datetime.strptime(data.get('acceptedDate'), '%Y-%m-%d %H:%M:%S')
        
        return cls(
            symbol=data.get('symbol'),
            fiscal_date=datetime.strptime(data.get('date'), '%Y-%m-%d').date(),
            period=data.get('period', 'FY'),
            reported_currency=data.get('reportedCurrency', 'USD'),
            filing_date=filing_date,
            accepted_date=accepted_date,
            revenue=data.get('revenue'),
            cost_of_revenue=data.get('costOfRevenue'),
            gross_profit=data.get('grossProfit'),
            operating_expenses=data.get('operatingExpenses'),
            operating_income=data.get('operatingIncome'),
            net_income=data.get('netIncome'),
            eps=data.get('eps'),
            eps_diluted=data.get('epsDiluted'),
            shares_outstanding=data.get('weightedAverageShsOut'),
            shares_outstanding_diluted=data.get('weightedAverageShsOutDil')
        )
    
    def to_raw_record(self) -> Dict[str, Any]:
        """Convert to raw table record format"""
        record = self.to_raw_base()
        record['raw_data'] = json.dumps(self.__dict__, cls=DateTimeEncoder)
        return record
    
    def to_staging_record(self) -> Dict[str, Any]:
        """Convert to staging table record format"""
        return {
            'symbol': self.symbol,
            'fiscal_date': self.fiscal_date,
            'period': self.period,
            'filing_date': self.filing_date,
            'accepted_date': self.accepted_date,
            'revenue': self.revenue,
            'cost_of_revenue': self.cost_of_revenue,
            'gross_profit': self.gross_profit,
            'operating_expenses': self.operating_expenses,
            'operating_income': self.operating_income,
            'net_income': self.net_income,
            'eps': self.eps,
            'eps_diluted': self.eps_diluted,
            'shares_outstanding': self.shares_outstanding,
            'shares_outstanding_diluted': self.shares_outstanding_diluted,
            'loaded_timestamp': datetime.now(timezone.utc)
        }


@dataclass 
class BalanceSheet(FinancialStatement):
    """Model for balance sheet data"""
    total_assets: Optional[float]
    current_assets: Optional[float]
    total_liabilities: Optional[float]
    current_liabilities: Optional[float]
    total_equity: Optional[float]
    cash_and_equivalents: Optional[float]
    total_debt: Optional[float]
    net_debt: Optional[float]
    
    @classmethod
    def from_fmp_response(cls, data: Dict[str, Any]) -> 'BalanceSheet':
        """Create BalanceSheet from FMP API response"""
        # Parse filing date
        filing_date = None
        if data.get('filingDate'):
            filing_date = datetime.strptime(data.get('filingDate'), '%Y-%m-%d').date()
        
        # Parse accepted date (includes time)
        accepted_date = None
        if data.get('acceptedDate'):
            accepted_date = datetime.strptime(data.get('acceptedDate'), '%Y-%m-%d %H:%M:%S')
        
        return cls(
            symbol=data.get('symbol'),
            fiscal_date=datetime.strptime(data.get('date'), '%Y-%m-%d').date(),
            period=data.get('period', 'FY'),
            reported_currency=data.get('reportedCurrency', 'USD'),
            filing_date=filing_date,
            accepted_date=accepted_date,
            total_assets=data.get('totalAssets'),
            current_assets=data.get('totalCurrentAssets'),
            total_liabilities=data.get('totalLiabilities'),
            current_liabilities=data.get('totalCurrentLiabilities'),
            total_equity=data.get('totalEquity') or data.get('totalStockholdersEquity'),
            cash_and_equivalents=data.get('cashAndCashEquivalents'),
            total_debt=data.get('totalDebt'),
            net_debt=data.get('netDebt')
        )
    
    def to_raw_record(self) -> Dict[str, Any]:
        """Convert to raw table record format"""
        record = self.to_raw_base()
        record['raw_data'] = json.dumps(self.__dict__, cls=DateTimeEncoder)
        return record
    
    def to_staging_record(self) -> Dict[str, Any]:
        """Convert to staging table record format"""
        return {
            'symbol': self.symbol,
            'fiscal_date': self.fiscal_date,
            'period': self.period,
            'filing_date': self.filing_date,
            'accepted_date': self.accepted_date,
            'total_assets': self.total_assets,
            'current_assets': self.current_assets,
            'total_liabilities': self.total_liabilities,
            'current_liabilities': self.current_liabilities,
            'total_equity': self.total_equity,
            'cash_and_equivalents': self.cash_and_equivalents,
            'total_debt': self.total_debt,
            'net_debt': self.net_debt,
            'loaded_timestamp': datetime.now(timezone.utc)
        }


@dataclass
class CashFlow(FinancialStatement):
    """Model for cash flow statement data"""
    operating_cash_flow: Optional[float]
    investing_cash_flow: Optional[float]
    financing_cash_flow: Optional[float]
    free_cash_flow: Optional[float]
    capital_expenditures: Optional[float]
    dividends_paid: Optional[float]
    
    @classmethod
    def from_fmp_response(cls, data: Dict[str, Any]) -> 'CashFlow':
        """Create CashFlow from FMP API response"""
        # Parse filing date
        filing_date = None
        if data.get('filingDate'):
            filing_date = datetime.strptime(data.get('filingDate'), '%Y-%m-%d').date()
        
        # Parse accepted date (includes time)
        accepted_date = None
        if data.get('acceptedDate'):
            accepted_date = datetime.strptime(data.get('acceptedDate'), '%Y-%m-%d %H:%M:%S')
        
        return cls(
            symbol=data.get('symbol'),
            fiscal_date=datetime.strptime(data.get('date'), '%Y-%m-%d').date(),
            period=data.get('period', 'FY'),
            reported_currency=data.get('reportedCurrency', 'USD'),
            filing_date=filing_date,
            accepted_date=accepted_date,
            operating_cash_flow=data.get('operatingCashFlow'),
            investing_cash_flow=data.get('netCashProvidedByInvestingActivities'),
            financing_cash_flow=data.get('netCashProvidedByFinancingActivities'),
            free_cash_flow=data.get('freeCashFlow'),
            capital_expenditures=data.get('capitalExpenditure'),
            dividends_paid=data.get('commonDividendsPaid') or data.get('netDividendsPaid')
        )
    
    def to_raw_record(self) -> Dict[str, Any]:
        """Convert to raw table record format"""
        record = self.to_raw_base()
        record['raw_data'] = json.dumps(self.__dict__, cls=DateTimeEncoder)
        return record
    
    def to_staging_record(self) -> Dict[str, Any]:
        """Convert to staging table record format"""
        return {
            'symbol': self.symbol,
            'fiscal_date': self.fiscal_date,
            'period': self.period,
            'filing_date': self.filing_date,
            'accepted_date': self.accepted_date,
            'operating_cash_flow': self.operating_cash_flow,
            'investing_cash_flow': self.investing_cash_flow,
            'financing_cash_flow': self.financing_cash_flow,
            'free_cash_flow': self.free_cash_flow,
            'capital_expenditures': self.capital_expenditures,
            'dividends_paid': self.dividends_paid,
            'loaded_timestamp': datetime.now(timezone.utc)
        }