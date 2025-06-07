"""
Data quality validation for transformed FMP data
"""
from typing import Dict, List, Any, Optional
from datetime import date, datetime, timezone
from loguru import logger


class DataQualityValidator:
    """Validate data quality for FMP transformations"""
    
    def __init__(self):
        """Initialize validator with rules"""
        self.validation_stats = {
            'total_checked': 0,
            'passed': 0,
            'failed': 0,
            'warnings': 0
        }
    
    def validate_company_profile(self, profile: Dict[str, Any]) -> tuple[bool, List[str]]:
        """
        Validate company profile data
        
        Args:
            profile: Profile data dictionary
            
        Returns:
            Tuple of (is_valid, list_of_issues)
        """
        issues = []
        
        # Required fields
        required_fields = ['symbol', 'company_name']
        for field in required_fields:
            if not profile.get(field):
                issues.append(f"Missing required field: {field}")
        
        # Symbol validation
        symbol = profile.get('symbol', '')
        if symbol and (len(symbol) > 10 or not symbol.isalnum()):
            issues.append(f"Invalid symbol format: {symbol}")
        
        # Market cap validation
        market_cap = profile.get('market_cap')
        if market_cap is not None and market_cap < 0:
            issues.append(f"Invalid market cap: {market_cap}")
        
        # Employee count validation
        employees = profile.get('employees')
        if employees is not None and employees < 0:
            issues.append(f"Invalid employee count: {employees}")
        
        self.validation_stats['total_checked'] += 1
        if issues:
            self.validation_stats['failed'] += 1
        else:
            self.validation_stats['passed'] += 1
        
        return len(issues) == 0, issues
    
    def validate_historical_price(self, price: Dict[str, Any]) -> tuple[bool, List[str]]:
        """
        Validate historical price data
        
        Args:
            price: Price data dictionary
            
        Returns:
            Tuple of (is_valid, list_of_issues)
        """
        issues = []
        warnings = []
        
        # Required fields
        required_fields = ['symbol', 'price_date', 'close_price']
        for field in required_fields:
            if not price.get(field):
                issues.append(f"Missing required field: {field}")
        
        # Price validation
        price_fields = ['open_price', 'high_price', 'low_price', 'close_price', 'adj_close']
        for field in price_fields:
            value = price.get(field)
            if value is not None and value < 0:
                issues.append(f"Negative price for {field}: {value}")
        
        # Logical price checks
        high = price.get('high_price', 0)
        low = price.get('low_price', 0)
        open_price = price.get('open_price', 0)
        close = price.get('close_price', 0)
        
        if high > 0 and low > 0:
            if low > high:
                issues.append(f"Low price ({low}) greater than high price ({high})")
            if open_price > 0 and (open_price > high or open_price < low):
                warnings.append(f"Open price ({open_price}) outside of high/low range")
            if close > 0 and (close > high or close < low):
                warnings.append(f"Close price ({close}) outside of high/low range")
        
        # Volume validation
        volume = price.get('volume')
        if volume is not None and volume < 0:
            issues.append(f"Negative volume: {volume}")
        
        # Date validation
        price_date = price.get('price_date')
        if price_date:
            if isinstance(price_date, str):
                try:
                    price_date = datetime.strptime(price_date, '%Y-%m-%d').date()
                except ValueError:
                    issues.append(f"Invalid date format: {price_date}")
            
            if isinstance(price_date, date) and price_date > date.today():
                issues.append(f"Future date not allowed: {price_date}")
        
        self.validation_stats['total_checked'] += 1
        if issues:
            self.validation_stats['failed'] += 1
        elif warnings:
            self.validation_stats['warnings'] += len(warnings)
            self.validation_stats['passed'] += 1
        else:
            self.validation_stats['passed'] += 1
        
        return len(issues) == 0, issues + warnings
    
    def validate_financial_statement(self, statement: Dict[str, Any], statement_type: str) -> tuple[bool, List[str]]:
        """
        Validate financial statement data
        
        Args:
            statement: Statement data dictionary
            statement_type: Type of statement ('income', 'balance', 'cashflow')
            
        Returns:
            Tuple of (is_valid, list_of_issues)
        """
        issues = []
        
        # Required fields for all statements
        required_fields = ['symbol', 'fiscal_date', 'period']
        for field in required_fields:
            if not statement.get(field):
                issues.append(f"Missing required field: {field}")
        
        # Period validation
        period = statement.get('period', '')
        valid_periods = ['FY', 'Q1', 'Q2', 'Q3', 'Q4']
        if period and period not in valid_periods:
            issues.append(f"Invalid period: {period}")
        
        # Date validation
        fiscal_date = statement.get('fiscal_date')
        if fiscal_date:
            if isinstance(fiscal_date, str):
                try:
                    fiscal_date = datetime.strptime(fiscal_date, '%Y-%m-%d').date()
                except ValueError:
                    issues.append(f"Invalid date format: {fiscal_date}")
            
            if isinstance(fiscal_date, date) and fiscal_date > date.today():
                issues.append(f"Future fiscal date not allowed: {fiscal_date}")
        
        # Statement-specific validations
        if statement_type == 'income':
            # Revenue should generally be positive
            revenue = statement.get('revenue')
            if revenue is not None and revenue < 0:
                issues.append(f"Negative revenue unusual: {revenue}")
            
            # Gross profit check
            revenue = statement.get('revenue', 0)
            cost_of_revenue = statement.get('cost_of_revenue', 0)
            gross_profit = statement.get('gross_profit')
            if gross_profit is not None and revenue > 0 and cost_of_revenue >= 0:
                expected_gross = revenue - cost_of_revenue
                if abs(gross_profit - expected_gross) > 0.01:
                    issues.append(f"Gross profit mismatch: {gross_profit} != {revenue} - {cost_of_revenue}")
        
        elif statement_type == 'balance':
            # Balance sheet equation check
            assets = statement.get('total_assets', 0)
            liabilities = statement.get('total_liabilities', 0)
            equity = statement.get('total_equity', 0)
            
            if assets > 0 and liabilities >= 0 and equity != 0:
                if abs(assets - (liabilities + equity)) > 0.01:
                    issues.append(f"Balance sheet equation violation: {assets} != {liabilities} + {equity}")
        
        elif statement_type == 'cashflow':
            # Free cash flow check
            operating_cf = statement.get('operating_cash_flow', 0)
            capex = statement.get('capital_expenditures', 0)
            free_cf = statement.get('free_cash_flow')
            
            if free_cf is not None and operating_cf != 0 and capex != 0:
                expected_fcf = operating_cf - abs(capex)
                if abs(free_cf - expected_fcf) > 0.01:
                    issues.append(f"Free cash flow mismatch: {free_cf} != {operating_cf} - {abs(capex)}")
        
        self.validation_stats['total_checked'] += 1
        if issues:
            self.validation_stats['failed'] += 1
        else:
            self.validation_stats['passed'] += 1
        
        return len(issues) == 0, issues
    
    def validate_batch(self, records: List[Dict[str, Any]], data_type: str) -> Dict[str, Any]:
        """
        Validate a batch of records
        
        Args:
            records: List of records to validate
            data_type: Type of data ('profile', 'price', 'income', 'balance', 'cashflow')
            
        Returns:
            Validation summary with details
        """
        validation_results = {
            'total_records': len(records),
            'valid_records': 0,
            'invalid_records': 0,
            'issues_by_record': []
        }
        
        validators = {
            'profile': self.validate_company_profile,
            'price': self.validate_historical_price,
            'income': lambda r: self.validate_financial_statement(r, 'income'),
            'balance': lambda r: self.validate_financial_statement(r, 'balance'),
            'cashflow': lambda r: self.validate_financial_statement(r, 'cashflow')
        }
        
        validator = validators.get(data_type)
        if not validator:
            raise ValueError(f"Unknown data type: {data_type}")
        
        for i, record in enumerate(records):
            is_valid, issues = validator(record)
            
            if is_valid:
                validation_results['valid_records'] += 1
            else:
                validation_results['invalid_records'] += 1
                validation_results['issues_by_record'].append({
                    'record_index': i,
                    'record_identifier': record.get('symbol', 'Unknown'),
                    'issues': issues
                })
        
        return validation_results
    
    def get_stats(self) -> Dict[str, Any]:
        """Get validation statistics"""
        return self.validation_stats.copy()
    
    def reset_stats(self):
        """Reset validation statistics"""
        self.validation_stats = {
            'total_checked': 0,
            'passed': 0,
            'failed': 0,
            'warnings': 0
        }