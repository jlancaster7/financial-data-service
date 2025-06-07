"""
FMP API data transformer for converting API responses to Snowflake-ready formats
"""
from typing import List, Dict, Any, Union, Optional
from datetime import datetime, timezone
from loguru import logger

from src.models.fmp_models import (
    CompanyProfile,
    HistoricalPrice,
    IncomeStatement,
    BalanceSheet,
    CashFlow
)


class FMPTransformer:
    """Transform FMP API responses to structured data formats"""
    
    def __init__(self):
        """Initialize transformer"""
        self.transformation_stats = {
            'records_processed': 0,
            'records_failed': 0,
            'last_run': None
        }
    
    def transform_company_profile(self, profile_data: Union[Dict, List[Dict]]) -> Dict[str, List[Dict[str, Any]]]:
        """
        Transform company profile data from FMP API
        
        Args:
            profile_data: FMP profile response (single dict or list of dicts)
            
        Returns:
            Dict with 'raw' and 'staging' keys containing transformed records
        """
        raw_records = []
        staging_records = []
        
        # Handle both single profile and list of profiles
        profiles = profile_data if isinstance(profile_data, list) else [profile_data]
        
        for profile in profiles:
            try:
                # Validate required fields
                if not profile.get('symbol'):
                    logger.warning(f"Skipping profile without symbol: {profile}")
                    self.transformation_stats['records_failed'] += 1
                    continue
                
                # Create model instance
                company = CompanyProfile.from_fmp_response(profile)
                
                # Generate records for both layers
                raw_records.append(company.to_raw_record())
                staging_records.append(company.to_staging_record())
                
                self.transformation_stats['records_processed'] += 1
                
            except Exception as e:
                logger.error(f"Error transforming profile for {profile.get('symbol', 'Unknown')}: {e}")
                self.transformation_stats['records_failed'] += 1
        
        self.transformation_stats['last_run'] = datetime.now(timezone.utc)
        
        return {
            'raw': raw_records,
            'staging': staging_records
        }
    
    def transform_historical_prices(self, symbol: str, price_data: List[Dict]) -> Dict[str, List[Dict[str, Any]]]:
        """
        Transform historical price data from FMP API
        
        Args:
            symbol: Stock symbol
            price_data: List of price records from FMP
            
        Returns:
            Dict with 'raw' and 'staging' keys containing transformed records
        """
        raw_records = []
        staging_records = []
        
        for price_record in price_data:
            try:
                # Skip records without date
                if not price_record.get('date'):
                    logger.warning(f"Skipping price record without date for {symbol}")
                    self.transformation_stats['records_failed'] += 1
                    continue
                
                # Create model instance
                price = HistoricalPrice.from_fmp_response(symbol, price_record)
                
                # Generate records for both layers
                raw_records.append(price.to_raw_record())
                staging_records.append(price.to_staging_record())
                
                self.transformation_stats['records_processed'] += 1
                
            except Exception as e:
                logger.error(f"Error transforming price for {symbol} on {price_record.get('date', 'Unknown')}: {e}")
                self.transformation_stats['records_failed'] += 1
        
        self.transformation_stats['last_run'] = datetime.now(timezone.utc)
        
        return {
            'raw': raw_records,
            'staging': staging_records
        }
    
    def transform_income_statements(self, statement_data: List[Dict]) -> Dict[str, List[Dict[str, Any]]]:
        """
        Transform income statement data from FMP API
        
        Args:
            statement_data: List of income statement records from FMP
            
        Returns:
            Dict with 'raw' and 'staging' keys containing transformed records
        """
        raw_records = []
        staging_records = []
        
        for statement in statement_data:
            try:
                # Validate required fields
                if not statement.get('symbol') or not statement.get('date'):
                    logger.warning(f"Skipping income statement without symbol/date: {statement}")
                    self.transformation_stats['records_failed'] += 1
                    continue
                
                # Create model instance
                income = IncomeStatement.from_fmp_response(statement)
                
                # Generate records for both layers
                raw_records.append(income.to_raw_record())
                staging_records.append(income.to_staging_record())
                
                self.transformation_stats['records_processed'] += 1
                
            except Exception as e:
                logger.error(f"Error transforming income statement for {statement.get('symbol', 'Unknown')}: {e}")
                self.transformation_stats['records_failed'] += 1
        
        self.transformation_stats['last_run'] = datetime.now(timezone.utc)
        
        return {
            'raw': raw_records,
            'staging': staging_records
        }
    
    def transform_balance_sheets(self, balance_data: List[Dict]) -> Dict[str, List[Dict[str, Any]]]:
        """
        Transform balance sheet data from FMP API
        
        Args:
            balance_data: List of balance sheet records from FMP
            
        Returns:
            Dict with 'raw' and 'staging' keys containing transformed records
        """
        raw_records = []
        staging_records = []
        
        for balance in balance_data:
            try:
                # Validate required fields
                if not balance.get('symbol') or not balance.get('date'):
                    logger.warning(f"Skipping balance sheet without symbol/date: {balance}")
                    self.transformation_stats['records_failed'] += 1
                    continue
                
                # Create model instance
                sheet = BalanceSheet.from_fmp_response(balance)
                
                # Generate records for both layers
                raw_records.append(sheet.to_raw_record())
                staging_records.append(sheet.to_staging_record())
                
                self.transformation_stats['records_processed'] += 1
                
            except Exception as e:
                logger.error(f"Error transforming balance sheet for {balance.get('symbol', 'Unknown')}: {e}")
                self.transformation_stats['records_failed'] += 1
        
        self.transformation_stats['last_run'] = datetime.now(timezone.utc)
        
        return {
            'raw': raw_records,
            'staging': staging_records
        }
    
    def transform_cash_flows(self, cashflow_data: List[Dict]) -> Dict[str, List[Dict[str, Any]]]:
        """
        Transform cash flow statement data from FMP API
        
        Args:
            cashflow_data: List of cash flow records from FMP
            
        Returns:
            Dict with 'raw' and 'staging' keys containing transformed records
        """
        raw_records = []
        staging_records = []
        
        for cashflow in cashflow_data:
            try:
                # Validate required fields
                if not cashflow.get('symbol') or not cashflow.get('date'):
                    logger.warning(f"Skipping cash flow without symbol/date: {cashflow}")
                    self.transformation_stats['records_failed'] += 1
                    continue
                
                # Create model instance
                flow = CashFlow.from_fmp_response(cashflow)
                
                # Generate records for both layers
                raw_records.append(flow.to_raw_record())
                staging_records.append(flow.to_staging_record())
                
                self.transformation_stats['records_processed'] += 1
                
            except Exception as e:
                logger.error(f"Error transforming cash flow for {cashflow.get('symbol', 'Unknown')}: {e}")
                self.transformation_stats['records_failed'] += 1
        
        self.transformation_stats['last_run'] = datetime.now(timezone.utc)
        
        return {
            'raw': raw_records,
            'staging': staging_records
        }
    
    def transform_batch(self, data_type: str, data: Any, **kwargs) -> Dict[str, List[Dict[str, Any]]]:
        """
        Transform a batch of data based on type
        
        Args:
            data_type: Type of data ('profile', 'prices', 'income', 'balance', 'cashflow')
            data: Raw data from FMP API
            **kwargs: Additional arguments (e.g., symbol for prices)
            
        Returns:
            Dict with 'raw' and 'staging' keys containing transformed records
        """
        transformers = {
            'profile': self.transform_company_profile,
            'prices': lambda d: self.transform_historical_prices(kwargs.get('symbol'), d),
            'income': self.transform_income_statements,
            'balance': self.transform_balance_sheets,
            'cashflow': self.transform_cash_flows
        }
        
        transformer = transformers.get(data_type)
        if not transformer:
            raise ValueError(f"Unknown data type: {data_type}")
        
        return transformer(data)
    
    def get_stats(self) -> Dict[str, Any]:
        """Get transformation statistics"""
        return self.transformation_stats.copy()
    
    def reset_stats(self):
        """Reset transformation statistics"""
        self.transformation_stats = {
            'records_processed': 0,
            'records_failed': 0,
            'last_run': None
        }