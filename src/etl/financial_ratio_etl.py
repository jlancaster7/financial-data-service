"""
ETL pipeline for calculating and loading financial ratios
"""
from typing import Dict, List, Any, Optional
from datetime import datetime, timezone
from loguru import logger

from src.etl.base_etl import BaseETL
from src.db.snowflake_connector import SnowflakeConnector
from src.utils.config import Config


class FinancialRatioETL(BaseETL):
    """ETL pipeline for calculating financial ratios from FACT_FINANCIALS"""
    
    def __init__(self, config: Config):
        """
        Initialize Financial Ratio ETL
        
        Args:
            config: Application configuration
        """
        # Create Snowflake connector
        snowflake_connector = SnowflakeConnector(config.snowflake)
        
        # Initialize base class
        super().__init__(
            job_name="financial_ratio_etl",
            snowflake_connector=snowflake_connector,
            fmp_client=None,  # Not needed for ratio calculations
            batch_size=config.app.batch_size,
            enable_monitoring=config.app.enable_monitoring
        )
        
        # Store config for later use
        self.config = config
        
    def extract(self, symbols: Optional[List[str]] = None, 
                fiscal_start_date: Optional[str] = None,
                fiscal_end_date: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Extract financial data from FACT_FINANCIALS for ratio calculation
        
        Args:
            symbols: Optional list of symbols to process. If None, process all.
            fiscal_start_date: Optional start date for fiscal period filter
            fiscal_end_date: Optional end date for fiscal period filter
            
        Returns:
            List of financial records for ratio calculation
        """
        logger.info(f"Extracting financial data for ratio calculation")
        
        # Build query
        query = """
        SELECT 
            ff.financial_key,
            ff.company_key,
            ff.fiscal_date_key,
            dd.date AS fiscal_date,
            ff.period_type,
            -- Income Statement
            ff.revenue,
            ff.cost_of_revenue,
            ff.gross_profit,
            ff.operating_expenses,
            ff.operating_income,
            ff.net_income,
            ff.eps,
            ff.eps_diluted,
            ff.shares_outstanding,
            -- Balance Sheet
            ff.total_assets,
            ff.current_assets,
            ff.total_liabilities,
            ff.current_liabilities,
            ff.total_equity,
            ff.cash_and_equivalents,
            ff.total_debt,
            ff.net_debt,
            -- Cash Flow
            ff.operating_cash_flow,
            ff.dividends_paid,
            -- Company info
            dc.symbol
        FROM ANALYTICS.FACT_FINANCIALS ff
        JOIN ANALYTICS.DIM_DATE dd ON ff.fiscal_date_key = dd.date_key
        JOIN ANALYTICS.DIM_COMPANY dc ON ff.company_key = dc.company_key
        WHERE 1=1
        """
        
        params = []
        
        # Add symbol filter if provided
        if symbols:
            placeholders = ','.join(['%s' for _ in symbols])
            query += f" AND dc.symbol IN ({placeholders})"
            params.extend(symbols)
        
        # Add date filters if provided
        if fiscal_start_date:
            query += " AND dd.date >= %s"
            params.append(fiscal_start_date)
        
        if fiscal_end_date:
            query += " AND dd.date <= %s"
            params.append(fiscal_end_date)
        
        # Only process records that don't already have ratios calculated
        query += """
        AND NOT EXISTS (
            SELECT 1 
            FROM ANALYTICS.FACT_FINANCIAL_RATIOS fr
            WHERE fr.financial_key = ff.financial_key
        )
        """
        
        query += " ORDER BY dc.symbol, dd.date"
        
        try:
            records = self.snowflake.fetch_all(query, tuple(params) if params else None)
            logger.info(f"Extracted {len(records)} financial records for ratio calculation")
            
            # Store metadata
            self.result.metadata['records_extracted'] = len(records)
            
            return records
        except Exception as e:
            logger.error(f"Failed to extract financial data: {e}")
            self.result.errors.append(f"Extract failed: {str(e)}")
            raise
    
    def transform(self, raw_data: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
        """
        Calculate financial ratios from raw financial data
        
        Args:
            raw_data: List of financial records from FACT_FINANCIALS
            
        Returns:
            Dict with 'ratios' key containing calculated ratio records
        """
        logger.info(f"Calculating financial ratios for {len(raw_data)} records")
        
        ratios_data = []
        calculation_errors = 0
        
        for record in raw_data:
            try:
                ratio_record = {
                    'financial_key': record['FINANCIAL_KEY'],
                    'company_key': record['COMPANY_KEY'],
                    'calculation_date_key': record['FISCAL_DATE_KEY'],
                }
                
                # Profitability Ratios
                revenue = record.get('REVENUE', 0) or 0
                if revenue > 0:
                    gross_profit = record.get('GROSS_PROFIT', 0) or 0
                    operating_income = record.get('OPERATING_INCOME', 0) or 0
                    net_income = record.get('NET_INCOME', 0) or 0
                    
                    ratio_record['gross_margin'] = round((gross_profit / revenue) * 100, 2)
                    ratio_record['operating_margin'] = round((operating_income / revenue) * 100, 2)
                    ratio_record['profit_margin'] = round((net_income / revenue) * 100, 2)
                else:
                    ratio_record['gross_margin'] = None
                    ratio_record['operating_margin'] = None
                    ratio_record['profit_margin'] = None
                
                # Return on Equity (ROE)
                total_equity = record.get('TOTAL_EQUITY', 0) or 0
                if total_equity > 0:
                    net_income = record.get('NET_INCOME', 0) or 0
                    ratio_record['roe'] = round((net_income / total_equity) * 100, 2)
                else:
                    ratio_record['roe'] = None
                
                # Return on Assets (ROA)
                total_assets = record.get('TOTAL_ASSETS', 0) or 0
                if total_assets > 0:
                    net_income = record.get('NET_INCOME', 0) or 0
                    ratio_record['roa'] = round((net_income / total_assets) * 100, 2)
                else:
                    ratio_record['roa'] = None
                
                # Liquidity Ratios
                current_liabilities = record.get('CURRENT_LIABILITIES', 0) or 0
                if current_liabilities > 0:
                    current_assets = record.get('CURRENT_ASSETS', 0) or 0
                    cash = record.get('CASH_AND_EQUIVALENTS', 0) or 0
                    
                    ratio_record['current_ratio'] = round(current_assets / current_liabilities, 2)
                    ratio_record['quick_ratio'] = round((current_assets - 0) / current_liabilities, 2)  # Ideally subtract inventory
                else:
                    ratio_record['current_ratio'] = None
                    ratio_record['quick_ratio'] = None
                
                # Leverage Ratios
                if total_equity > 0:
                    total_debt = record.get('TOTAL_DEBT', 0) or 0
                    ratio_record['debt_to_equity'] = round(total_debt / total_equity, 2)
                else:
                    ratio_record['debt_to_equity'] = None
                
                if total_assets > 0:
                    total_debt = record.get('TOTAL_DEBT', 0) or 0
                    ratio_record['debt_to_assets'] = round(total_debt / total_assets, 2)
                else:
                    ratio_record['debt_to_assets'] = None
                
                # Efficiency Ratios
                if total_assets > 0 and revenue > 0:
                    ratio_record['asset_turnover'] = round(revenue / total_assets, 2)
                else:
                    ratio_record['asset_turnover'] = None
                
                # Per Share Metrics
                shares_outstanding = record.get('SHARES_OUTSTANDING', 0) or 0
                if shares_outstanding > 0 and total_equity > 0:
                    ratio_record['book_value_per_share'] = round(total_equity / shares_outstanding, 2)
                else:
                    ratio_record['book_value_per_share'] = None
                
                ratios_data.append(ratio_record)
                
            except Exception as e:
                logger.warning(f"Error calculating ratios for record {record.get('FINANCIAL_KEY')}: {e}")
                calculation_errors += 1
        
        # Store metadata
        self.result.metadata['records_transformed'] = len(ratios_data)
        self.result.metadata['calculation_errors'] = calculation_errors
        
        logger.info(f"Successfully calculated ratios for {len(ratios_data)} records")
        if calculation_errors > 0:
            logger.warning(f"Failed to calculate ratios for {calculation_errors} records")
        
        return {'ratios': ratios_data}
    
    def load(self, transformed_data: Dict[str, List[Dict[str, Any]]]) -> int:
        """
        Load calculated ratios to FACT_FINANCIAL_RATIOS
        
        Args:
            transformed_data: Dict with 'ratios' key containing ratio records
            
        Returns:
            Number of records loaded
        """
        ratios_data = transformed_data.get('ratios', [])
        
        if not ratios_data:
            logger.warning("No ratio data to load")
            return 0
        
        logger.info(f"Loading {len(ratios_data)} ratio records to FACT_FINANCIAL_RATIOS")
        
        try:
            # Add timestamp
            current_timestamp = datetime.now(timezone.utc)
            for record in ratios_data:
                record['created_timestamp'] = current_timestamp
            
            # Insert into FACT_FINANCIAL_RATIOS
            self.snowflake.bulk_insert('ANALYTICS.FACT_FINANCIAL_RATIOS', ratios_data)
            
            logger.info(f"Successfully loaded {len(ratios_data)} records to FACT_FINANCIAL_RATIOS")
            return len(ratios_data)
            
        except Exception as e:
            logger.error(f"Failed to load ratio data: {e}")
            self.result.errors.append(f"Load failed: {str(e)}")
            raise
    
    def run(self, symbols: Optional[List[str]] = None,
            fiscal_start_date: Optional[str] = None,
            fiscal_end_date: Optional[str] = None) -> Dict[str, Any]:
        """
        Run the complete financial ratio ETL pipeline
        
        Args:
            symbols: Optional list of symbols to process
            fiscal_start_date: Optional start date for fiscal period filter
            fiscal_end_date: Optional end date for fiscal period filter
            
        Returns:
            ETL result summary
        """
        logger.info(f"Starting {self.job_name} ETL pipeline")
        
        try:
            # Extract
            raw_data = self.extract(symbols, fiscal_start_date, fiscal_end_date)
            if not raw_data:
                logger.info("No new financial data to process for ratio calculation")
                return {
                    'status': 'success',
                    'records_processed': 0,
                    'message': 'No new financial data to process'
                }
            
            # Transform
            transformed_data = self.transform(raw_data)
            
            # Load
            records_loaded = self.load(transformed_data)
            
            return {
                'status': 'success',
                'records_extracted': len(raw_data),
                'records_loaded': records_loaded,
                'calculation_errors': self.result.metadata.get('calculation_errors', 0)
            }
            
        except Exception as e:
            logger.error(f"Financial ratio ETL pipeline failed: {e}")
            return {
                'status': 'failed',
                'error': str(e),
                'records_processed': 0
            }