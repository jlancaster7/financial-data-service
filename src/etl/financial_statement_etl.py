"""
Financial Statement ETL Pipeline
"""
from typing import List, Dict, Any, Optional
from datetime import date, datetime, timedelta, timezone
from loguru import logger

from src.etl.base_etl import BaseETL, ETLResult, ETLStatus
from src.api.fmp_client import FMPClient
from src.db.snowflake_connector import SnowflakeConnector
from src.transformations.fmp_transformer import FMPTransformer
from src.transformations.data_quality import DataQualityValidator
from src.utils.config import Config


class FinancialStatementETL(BaseETL):
    """ETL pipeline for financial statement data (Income, Balance Sheet, Cash Flow)"""
    
    def __init__(self, config: Config):
        """Initialize Financial Statement ETL pipeline"""
        # Create instances
        snowflake_connector = SnowflakeConnector(config.snowflake)
        fmp_client = FMPClient(config.fmp)
        
        # Initialize base class
        super().__init__(
            job_name="FinancialStatementETL",
            snowflake_connector=snowflake_connector,
            fmp_client=fmp_client,
            batch_size=config.app.batch_size,
            enable_monitoring=config.app.enable_monitoring
        )
        
        # Store config for later use
        self.config = config
        
        # Initialize tracking lists
        self.job_errors = []
        self.data_quality_issues = []
        
    def extract(self, symbols: List[str], period: str = 'annual', 
                limit: Optional[int] = None) -> Dict[str, List[Dict[str, Any]]]:
        """
        Extract financial statement data from FMP API
        
        Args:
            symbols: List of stock symbols
            period: 'annual' or 'quarterly'
            limit: Number of periods to fetch (None for no limit)
            
        Returns:
            Dict with 'income', 'balance', 'cashflow' keys containing raw data
        """
        all_statement_data = {
            'income': [],
            'balance': [],
            'cashflow': []
        }
        
        logger.info(f"Extracting financial statements for {len(symbols)} symbols (period: {period}, limit: {limit})")
        
        for symbol in symbols:
            try:
                # Extract income statements
                income_data = self.fmp_client.get_income_statement(
                    symbol=symbol,
                    period=period,
                    limit=limit
                )
                if income_data:
                    all_statement_data['income'].extend(income_data)
                    logger.info(f"Extracted {len(income_data)} income statements for {symbol}")
                
                # Extract balance sheets
                balance_data = self.fmp_client.get_balance_sheet(
                    symbol=symbol,
                    period=period,
                    limit=limit
                )
                if balance_data:
                    all_statement_data['balance'].extend(balance_data)
                    logger.info(f"Extracted {len(balance_data)} balance sheets for {symbol}")
                
                # Extract cash flow statements
                cashflow_data = self.fmp_client.get_cash_flow(
                    symbol=symbol,
                    period=period,
                    limit=limit
                )
                if cashflow_data:
                    all_statement_data['cashflow'].extend(cashflow_data)
                    logger.info(f"Extracted {len(cashflow_data)} cash flow statements for {symbol}")
                    
            except Exception as e:
                logger.error(f"Failed to extract financial statements for {symbol}: {e}")
                self.job_errors.append({
                    'symbol': symbol,
                    'error': str(e),
                    'phase': 'extract'
                })
        
        total_extracted = sum(len(data) for data in all_statement_data.values())
        logger.info(f"Total financial statements extracted: {total_extracted}")
        return all_statement_data
    
    def transform(self, raw_data: Dict[str, List[Dict[str, Any]]]) -> Dict[str, Dict[str, List[Dict[str, Any]]]]:
        """
        Transform raw financial statement data to structured format
        
        Args:
            raw_data: Dict with 'income', 'balance', 'cashflow' keys
            
        Returns:
            Dict with statement types as keys, each containing 'raw' and 'staging' data
        """
        transformed_data = {
            'income': {'raw': [], 'staging': []},
            'balance': {'raw': [], 'staging': []},
            'cashflow': {'raw': [], 'staging': []}
        }
        
        # Transform income statements
        if raw_data.get('income'):
            try:
                income_transformed = self.transformer.transform_income_statements(raw_data['income'])
                
                # Validate staging records
                validated_staging = []
                for record in income_transformed['staging']:
                    is_valid, issues = self.validator.validate_financial_statement(record, 'income')
                    if is_valid:
                        validated_staging.append(record)
                    else:
                        logger.warning(f"Validation failed for income statement {record.get('symbol')} on {record.get('fiscal_date')}: {issues}")
                        self.data_quality_issues.extend(issues)
                
                transformed_data['income']['raw'] = income_transformed['raw']
                transformed_data['income']['staging'] = validated_staging
                
            except Exception as e:
                logger.error(f"Failed to transform income statements: {e}")
                self.job_errors.append({
                    'error': str(e),
                    'phase': 'transform_income'
                })
        
        # Transform balance sheets
        if raw_data.get('balance'):
            try:
                balance_transformed = self.transformer.transform_balance_sheets(raw_data['balance'])
                
                # Validate staging records
                validated_staging = []
                for record in balance_transformed['staging']:
                    is_valid, issues = self.validator.validate_financial_statement(record, 'balance')
                    if is_valid:
                        validated_staging.append(record)
                    else:
                        logger.warning(f"Validation failed for balance sheet {record.get('symbol')} on {record.get('fiscal_date')}: {issues}")
                        self.data_quality_issues.extend(issues)
                
                transformed_data['balance']['raw'] = balance_transformed['raw']
                transformed_data['balance']['staging'] = validated_staging
                
            except Exception as e:
                logger.error(f"Failed to transform balance sheets: {e}")
                self.job_errors.append({
                    'error': str(e),
                    'phase': 'transform_balance'
                })
        
        # Transform cash flow statements
        if raw_data.get('cashflow'):
            try:
                cashflow_transformed = self.transformer.transform_cash_flows(raw_data['cashflow'])
                
                # Validate staging records
                validated_staging = []
                for record in cashflow_transformed['staging']:
                    is_valid, issues = self.validator.validate_financial_statement(record, 'cashflow')
                    if is_valid:
                        validated_staging.append(record)
                    else:
                        logger.warning(f"Validation failed for cash flow {record.get('symbol')} on {record.get('fiscal_date')}: {issues}")
                        self.data_quality_issues.extend(issues)
                
                transformed_data['cashflow']['raw'] = cashflow_transformed['raw']
                transformed_data['cashflow']['staging'] = validated_staging
                
            except Exception as e:
                logger.error(f"Failed to transform cash flows: {e}")
                self.job_errors.append({
                    'error': str(e),
                    'phase': 'transform_cashflow'
                })
        
        total_transformed = sum(
            len(stmt_type['staging']) 
            for stmt_type in transformed_data.values()
        )
        logger.info(f"Transformed {total_transformed} financial statement records")
        return transformed_data
    
    def load(self, transformed_data: Dict[str, Dict[str, List[Dict[str, Any]]]]) -> int:
        """
        Load transformed data to Snowflake
        
        Args:
            transformed_data: Dict with statement types and their transformed data
            
        Returns:
            Number of records loaded
        """
        total_loaded = 0
        
        with self.snowflake as conn:
            # Load income statements
            if transformed_data['income']['raw']:
                try:
                    affected = conn.bulk_insert(
                        table='RAW_DATA.RAW_INCOME_STATEMENT',
                        data=transformed_data['income']['raw']
                    )
                    total_loaded += affected or 0
                    logger.info(f"Loaded {affected} records to RAW_INCOME_STATEMENT")
                except Exception as e:
                    logger.error(f"Failed to load raw income statement data: {e}")
                    self.job_errors.append({
                        'error': str(e),
                        'phase': 'load_raw_income'
                    })
            
            if transformed_data['income']['staging']:
                try:
                    affected = conn.merge(
                        table='STAGING.STG_INCOME_STATEMENT',
                        data=transformed_data['income']['staging'],
                        merge_keys=['symbol', 'fiscal_date', 'period'],
                        update_columns=['filing_date', 'accepted_date', 'revenue', 'cost_of_revenue', 
                                      'gross_profit', 'operating_expenses', 'operating_income', 'net_income', 
                                      'eps', 'eps_diluted', 'shares_outstanding', 'shares_outstanding_diluted', 
                                      'loaded_timestamp']
                    )
                    total_loaded += affected or 0
                    logger.info(f"Merged {affected} records to STG_INCOME_STATEMENT")
                except Exception as e:
                    logger.error(f"Failed to load staging income statement data: {e}")
                    self.job_errors.append({
                        'error': str(e),
                        'phase': 'load_staging_income'
                    })
            
            # Load balance sheets
            if transformed_data['balance']['raw']:
                try:
                    affected = conn.bulk_insert(
                        table='RAW_DATA.RAW_BALANCE_SHEET',
                        data=transformed_data['balance']['raw']
                    )
                    total_loaded += affected or 0
                    logger.info(f"Loaded {affected} records to RAW_BALANCE_SHEET")
                except Exception as e:
                    logger.error(f"Failed to load raw balance sheet data: {e}")
                    self.job_errors.append({
                        'error': str(e),
                        'phase': 'load_raw_balance'
                    })
            
            if transformed_data['balance']['staging']:
                try:
                    affected = conn.merge(
                        table='STAGING.STG_BALANCE_SHEET',
                        data=transformed_data['balance']['staging'],
                        merge_keys=['symbol', 'fiscal_date', 'period'],
                        update_columns=['filing_date', 'accepted_date', 'total_assets', 'current_assets',
                                      'total_liabilities', 'current_liabilities', 'total_equity', 
                                      'cash_and_equivalents', 'total_debt', 'net_debt', 'loaded_timestamp']
                    )
                    total_loaded += affected or 0
                    logger.info(f"Merged {affected} records to STG_BALANCE_SHEET")
                except Exception as e:
                    logger.error(f"Failed to load staging balance sheet data: {e}")
                    self.job_errors.append({
                        'error': str(e),
                        'phase': 'load_staging_balance'
                    })
            
            # Load cash flow statements
            if transformed_data['cashflow']['raw']:
                try:
                    affected = conn.bulk_insert(
                        table='RAW_DATA.RAW_CASH_FLOW',
                        data=transformed_data['cashflow']['raw']
                    )
                    total_loaded += affected or 0
                    logger.info(f"Loaded {affected} records to RAW_CASH_FLOW")
                except Exception as e:
                    logger.error(f"Failed to load raw cash flow data: {e}")
                    self.job_errors.append({
                        'error': str(e),
                        'phase': 'load_raw_cashflow'
                    })
            
            if transformed_data['cashflow']['staging']:
                try:
                    affected = conn.merge(
                        table='STAGING.STG_CASH_FLOW',
                        data=transformed_data['cashflow']['staging'],
                        merge_keys=['symbol', 'fiscal_date', 'period']
                    )
                    total_loaded += affected or 0
                    logger.info(f"Merged {affected} records to STG_CASH_FLOW")
                except Exception as e:
                    logger.error(f"Failed to load staging cash flow data: {e}")
                    self.job_errors.append({
                        'error': str(e),
                        'phase': 'load_staging_cashflow'
                    })
        
        return total_loaded
    
    def update_fact_table(self, symbols: List[str], from_date: Optional[date] = None):
        """
        Update FACT_FINANCIALS with raw financial data
        
        Args:
            symbols: List of symbols to update
            from_date: Start date for updates (defaults to oldest staging data)
        """
        with self.snowflake as conn:
            try:
                # If no from_date, get the oldest date in staging tables
                if not from_date:
                    # Format symbol list for IN clause
                    symbol_placeholders = ','.join(['%s' for _ in symbols])
                    query = f"""
                    SELECT MIN(fiscal_date) as min_date
                    FROM (
                        SELECT MIN(fiscal_date) as fiscal_date
                        FROM STAGING.STG_INCOME_STATEMENT
                        WHERE symbol IN ({symbol_placeholders})
                        UNION ALL
                        SELECT MIN(fiscal_date) as fiscal_date
                        FROM STAGING.STG_BALANCE_SHEET
                        WHERE symbol IN ({symbol_placeholders})
                        UNION ALL
                        SELECT MIN(fiscal_date) as fiscal_date
                        FROM STAGING.STG_CASH_FLOW
                        WHERE symbol IN ({symbol_placeholders})
                    ) t
                    """
                    # Provide symbols three times for the three IN clauses
                    params = list(symbols) * 3
                    result = conn.fetch_all(query, tuple(params))
                    result = result[0] if result else None
                    from_date = result['MIN_DATE'] if result and result.get('MIN_DATE') else date.today() - timedelta(days=365*5)
                
                # Format symbol list for IN clause
                symbol_placeholders = ','.join(['%s' for _ in symbols])
                
                # Insert/update FACT_FINANCIALS with raw financial data
                merge_query = f"""
                MERGE INTO ANALYTICS.FACT_FINANCIALS AS target
                USING (
                    SELECT 
                        c.company_key,
                        fiscal_d.date_key as fiscal_date_key,
                        filing_d.date_key as filing_date_key,
                        COALESCE(i.accepted_date, b.accepted_date, cf.accepted_date) as accepted_date,
                        COALESCE(i.period, b.period, cf.period) as period_type,
                        -- Income Statement fields
                        i.revenue,
                        i.cost_of_revenue,
                        i.gross_profit,
                        i.operating_expenses,
                        i.operating_income,
                        i.net_income,
                        i.eps,
                        i.eps_diluted,
                        i.shares_outstanding,
                        -- Balance Sheet fields
                        b.total_assets,
                        b.current_assets,
                        b.total_liabilities,
                        b.current_liabilities,
                        b.total_equity,
                        b.cash_and_equivalents,
                        b.total_debt,
                        b.net_debt,
                        -- Cash Flow fields
                        cf.operating_cash_flow,
                        cf.investing_cash_flow,
                        cf.financing_cash_flow,
                        cf.free_cash_flow,
                        cf.capital_expenditures,
                        cf.dividends_paid
                    FROM ANALYTICS.DIM_COMPANY c
                    LEFT JOIN STAGING.STG_INCOME_STATEMENT i 
                        ON c.symbol = i.symbol
                    LEFT JOIN STAGING.STG_BALANCE_SHEET b 
                        ON c.symbol = b.symbol 
                        AND i.fiscal_date = b.fiscal_date
                        AND i.period = b.period
                    LEFT JOIN STAGING.STG_CASH_FLOW cf 
                        ON c.symbol = cf.symbol 
                        AND i.fiscal_date = cf.fiscal_date
                        AND i.period = cf.period
                    INNER JOIN ANALYTICS.DIM_DATE fiscal_d 
                        ON fiscal_d.date = i.fiscal_date
                    LEFT JOIN ANALYTICS.DIM_DATE filing_d 
                        ON filing_d.date = COALESCE(i.filing_date, b.filing_date, cf.filing_date)
                    WHERE c.symbol IN ({symbol_placeholders})
                      AND c.is_current = TRUE
                      AND i.fiscal_date >= %s
                      AND i.fiscal_date IS NOT NULL
                ) AS source
                ON target.company_key = source.company_key 
                   AND target.fiscal_date_key = source.fiscal_date_key
                   AND target.period_type = source.period_type
                WHEN MATCHED THEN UPDATE SET
                    filing_date_key = source.filing_date_key,
                    accepted_date = source.accepted_date,
                    revenue = source.revenue,
                    cost_of_revenue = source.cost_of_revenue,
                    gross_profit = source.gross_profit,
                    operating_expenses = source.operating_expenses,
                    operating_income = source.operating_income,
                    net_income = source.net_income,
                    eps = source.eps,
                    eps_diluted = source.eps_diluted,
                    shares_outstanding = source.shares_outstanding,
                    total_assets = source.total_assets,
                    current_assets = source.current_assets,
                    total_liabilities = source.total_liabilities,
                    current_liabilities = source.current_liabilities,
                    total_equity = source.total_equity,
                    cash_and_equivalents = source.cash_and_equivalents,
                    total_debt = source.total_debt,
                    net_debt = source.net_debt,
                    operating_cash_flow = source.operating_cash_flow,
                    investing_cash_flow = source.investing_cash_flow,
                    financing_cash_flow = source.financing_cash_flow,
                    free_cash_flow = source.free_cash_flow,
                    capital_expenditures = source.capital_expenditures,
                    dividends_paid = source.dividends_paid
                WHEN NOT MATCHED THEN INSERT (
                    company_key, fiscal_date_key, filing_date_key, accepted_date, period_type,
                    revenue, cost_of_revenue, gross_profit, operating_expenses, operating_income, net_income, eps, eps_diluted, shares_outstanding,
                    total_assets, current_assets, total_liabilities, current_liabilities, total_equity, cash_and_equivalents, total_debt, net_debt,
                    operating_cash_flow, investing_cash_flow, financing_cash_flow, free_cash_flow, 
                    capital_expenditures, dividends_paid
                ) VALUES (
                    source.company_key, source.fiscal_date_key, source.filing_date_key, source.accepted_date, source.period_type,
                    source.revenue, source.cost_of_revenue, source.gross_profit, source.operating_expenses, source.operating_income, source.net_income, source.eps, source.eps_diluted, source.shares_outstanding,
                    source.total_assets, source.current_assets, source.total_liabilities, source.current_liabilities, source.total_equity, source.cash_and_equivalents, source.total_debt, source.net_debt,
                    source.operating_cash_flow, source.investing_cash_flow, source.financing_cash_flow, source.free_cash_flow,
                    source.capital_expenditures, source.dividends_paid
                )
                """
                
                # Prepare parameters: symbols + from_date
                params = list(symbols) + [from_date]
                affected = conn.execute_with_rowcount(merge_query, tuple(params))
                logger.info(f"Updated {affected} records in FACT_FINANCIALS")
                
            except Exception as e:
                logger.error(f"Failed to update fact table: {e}")
                self.job_errors.append({
                    'error': str(e),
                    'phase': 'update_fact_table'
                })
    
    def run(self, symbols: List[str], period: str = 'annual',
            limit: int = 5, update_analytics: bool = True) -> ETLResult:
        """
        Run the complete ETL pipeline for financial statements
        
        Args:
            symbols: List of stock symbols
            period: 'annual' or 'quarterly'
            limit: Number of periods to fetch
            update_analytics: Whether to update FACT_FINANCIAL_METRICS
            
        Returns:
            ETLResult with job execution details
        """
        logger.info(f"Starting Financial Statement ETL for {len(symbols)} symbols (period: {period})")
        
        # Reset error tracking and result
        self.job_errors = []
        self.data_quality_issues = []
        self.result = ETLResult(
            job_name=self.job_name,
            status=ETLStatus.RUNNING,
            start_time=datetime.now(timezone.utc),
            end_time=None,
            records_extracted=0,
            records_transformed=0,
            records_loaded=0,
            errors=[],
            metadata={'symbols': symbols, 'period': period, 'limit': limit}
        )
        
        try:
            # Extract
            raw_data = self.extract(symbols, period, limit)
            total_extracted = sum(len(data) for data in raw_data.values())
            self.result.records_extracted = total_extracted
            
            if total_extracted == 0:
                logger.warning("No data extracted")
                self.result.status = ETLStatus.SUCCESS
                self.result.end_time = datetime.now(timezone.utc)
                self.result.metadata['message'] = "No data to process"
                return self.result
            
            # Transform
            transformed = self.transform(raw_data)
            total_transformed = sum(
                len(stmt_type['staging']) 
                for stmt_type in transformed.values()
            )
            self.result.records_transformed = total_transformed
            
            # Load
            records_loaded = self.load(transformed)
            self.result.records_loaded = records_loaded
            
            # Update analytics layer if requested
            if update_analytics and records_loaded > 0:
                self.update_fact_table(symbols)
            
            # Determine status and finalize result
            if self.job_errors:
                self.result.status = ETLStatus.PARTIAL if records_loaded > 0 else ETLStatus.FAILED
                self.result.errors = [str(e) for e in self.job_errors]
            else:
                self.result.status = ETLStatus.SUCCESS
            
            self.result.end_time = datetime.now(timezone.utc)
            self.result.metadata['message'] = f"Processed {total_extracted} financial statements for {len(symbols)} symbols"
            
            # Save monitoring data if enabled
            if self.monitor:
                self.monitor.save_job_result(self.result)
            
            return self.result
            
        except Exception as e:
            logger.error(f"Financial Statement ETL failed: {e}")
            self.result.status = ETLStatus.FAILED
            self.result.end_time = datetime.now(timezone.utc)
            self.result.errors = [str(e)]
            self.result.metadata['message'] = f"Pipeline failed: {str(e)}"
            
            if self.monitor:
                self.monitor.save_job_result(self.result)
                
            return self.result