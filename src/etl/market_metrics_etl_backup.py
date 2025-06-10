"""
ETL pipeline for calculating and loading daily market metrics
"""
from typing import Dict, List, Any, Optional
from datetime import datetime, timezone
from loguru import logger

from src.etl.base_etl import BaseETL
from src.db.snowflake_connector import SnowflakeConnector
from src.utils.config import Config


class MarketMetricsETL(BaseETL):
    """ETL pipeline for calculating daily market metrics (price + financial data)"""
    
    def __init__(self, config: Config):
        """
        Initialize Market Metrics ETL
        
        Args:
            config: Application configuration
        """
        # Create Snowflake connector
        snowflake_connector = SnowflakeConnector(config.snowflake)
        
        # Initialize base class
        super().__init__(
            job_name="market_metrics_etl",
            snowflake_connector=snowflake_connector,
            fmp_client=None,  # Not needed for metric calculations
            batch_size=config.app.batch_size,
            enable_monitoring=config.app.enable_monitoring
        )
        
        # Store config for later use
        self.config = config
        
    def extract(self, symbols: Optional[List[str]] = None, 
                start_date: Optional[str] = None,
                end_date: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Extract daily price data joined with most recent financial data
        
        Args:
            symbols: Optional list of symbols to process. If None, process all.
            start_date: Optional start date for price data
            end_date: Optional end date for price data
            
        Returns:
            List of records combining daily prices with financial data
        """
        logger.info("Extracting data for market metrics calculation")
        
        # Query to get daily prices with the most recent financial data
        # This query properly considers accepted_date for point-in-time TTM calculations
        query = """
        WITH price_dates AS (
            -- Get all price dates we need to process
            SELECT DISTINCT
                dp.price_key,
                dp.company_key,
                dp.date_key,
                pd.date AS price_date,
                dc.symbol,
                dc.sector,
                dp.close_price,
                dp.volume
            FROM ANALYTICS.FACT_DAILY_PRICES dp
            JOIN ANALYTICS.DIM_DATE pd ON dp.date_key = pd.date_key
            JOIN ANALYTICS.DIM_COMPANY dc ON dp.company_key = dc.company_key
            WHERE 1=1
        ),
        available_financials AS (
            -- Get financials available at each price date (using accepted_date)
            SELECT 
                p.price_key,
                p.company_key,
                p.date_key,
                p.price_date,
                p.symbol,
                p.sector,
                p.close_price,
                p.volume,
                ff.financial_key,
                ff.fiscal_date_key,
                ff.filing_date_key,
                ff.accepted_date,
                ff.period_type,
                ff.revenue,
                ff.net_income,
                ff.total_equity,
                ff.total_assets,
                ff.total_debt,
                ff.shares_outstanding,
                ff.operating_income,
                ff.operating_cash_flow,
                ff.dividends_paid,
                fd.date AS fiscal_date,
                -- Row number to identify the 4 most recent quarters available at price date
                ROW_NUMBER() OVER (
                    PARTITION BY p.price_key, ff.period_type 
                    ORDER BY ff.fiscal_date_key DESC
                ) as quarterly_rank,
                -- Row number to identify most recent annual available at price date
                ROW_NUMBER() OVER (
                    PARTITION BY p.price_key 
                    ORDER BY CASE WHEN ff.period_type = 'FY' THEN ff.fiscal_date_key END DESC NULLS LAST
                ) as annual_rank
            FROM price_dates p
            LEFT JOIN ANALYTICS.FACT_FINANCIALS ff 
                ON p.company_key = ff.company_key
                AND ff.accepted_date <= p.price_date  -- Only use financials available at price date
            JOIN ANALYTICS.DIM_DATE fd ON ff.fiscal_date_key = fd.date_key
        ),
        latest_annual AS (
            -- Get the most recent annual filing available at each price date
            SELECT * 
            FROM available_financials 
            WHERE period_type = 'FY' 
            AND annual_rank = 1
        ),
        ttm_quarters AS (
            -- Get the 4 most recent quarters available at each price date
            SELECT 
                price_key,
                company_key,
                date_key,
                price_date,
                fiscal_date_key,
                accepted_date,
                period_type,
                revenue,
                net_income,
                operating_income,
                operating_cash_flow,
                dividends_paid,
                total_equity,
                total_assets,
                total_debt,
                shares_outstanding,
                quarterly_rank
            FROM available_financials 
            WHERE period_type IN ('Q1', 'Q2', 'Q3', 'Q4')
            AND quarterly_rank <= 4
        ),
        ttm_financials AS (
            -- Calculate TTM metrics from the 4 most recent quarters
            SELECT 
                price_key,
                company_key,
                date_key,
                price_date,
                COUNT(*) as quarters_available,
                MAX(accepted_date) as latest_ttm_accepted_date,
                SUM(revenue) as ttm_revenue,
                SUM(net_income) as ttm_net_income,
                SUM(operating_income) as ttm_operating_income,
                SUM(operating_cash_flow) as ttm_operating_cash_flow,
                SUM(dividends_paid) as ttm_dividends_paid,
                -- For balance sheet items, use the most recent quarter's values (quarterly_rank = 1)
                MAX(CASE WHEN quarterly_rank = 1 THEN total_equity END) as latest_total_equity,
                MAX(CASE WHEN quarterly_rank = 1 THEN total_assets END) as latest_total_assets,
                MAX(CASE WHEN quarterly_rank = 1 THEN total_debt END) as latest_total_debt,
                MAX(CASE WHEN quarterly_rank = 1 THEN shares_outstanding END) as latest_shares_outstanding
            FROM ttm_quarters
            GROUP BY price_key, company_key, date_key, price_date
            HAVING COUNT(*) = 4  -- Only calculate TTM if we have all 4 quarters
        )
        SELECT DISTINCT
            pd.price_key,
            pd.company_key,
            pd.date_key,
            pd.price_date,
            pd.symbol,
            pd.sector,
            -- Price data
            pd.close_price,
            pd.volume,
            -- Latest annual financial data
            la.financial_key,
            la.period_type,
            la.revenue as annual_revenue,
            la.net_income as annual_net_income,
            la.total_equity,
            la.total_assets,
            la.total_debt,
            la.shares_outstanding,
            la.operating_income as annual_operating_income,
            la.dividends_paid as annual_dividends_paid,
            la.accepted_date as annual_accepted_date,
            -- TTM financial data
            ttm.ttm_revenue,
            ttm.ttm_net_income,
            ttm.ttm_operating_income,
            ttm.ttm_operating_cash_flow,
            ttm.ttm_dividends_paid,
            ttm.latest_shares_outstanding as ttm_shares_outstanding,
            ttm.latest_total_equity as ttm_total_equity,
            ttm.latest_total_assets as ttm_total_assets,
            ttm.latest_total_debt as ttm_total_debt,
            ttm.latest_ttm_accepted_date,
            ttm.quarters_available
        FROM price_dates pd
        LEFT JOIN latest_annual la 
            ON pd.price_key = la.price_key
        LEFT JOIN ttm_financials ttm 
            ON pd.price_key = ttm.price_key
        WHERE 1=1
        """
        
        params = []
        
        # Add symbol filter if provided
        if symbols:
            placeholders = ','.join(['%s' for _ in symbols])
            query += f" AND pd.symbol IN ({placeholders})"
            params.extend(symbols)
        
        # Add date filters if provided
        if start_date:
            query += " AND pd.price_date >= %s"
            params.append(start_date)
        
        if end_date:
            query += " AND pd.price_date <= %s"
            params.append(end_date)
        
        # Only process records that don't already have metrics calculated
        query += """
        AND NOT EXISTS (
            SELECT 1 
            FROM ANALYTICS.FACT_MARKET_METRICS mm
            WHERE mm.company_key = pd.company_key
            AND mm.date_key = pd.date_key
        )
        """
        
        # Only include records where we have financial data (either annual or TTM)
        query += " AND (la.financial_key IS NOT NULL OR ttm.quarters_available = 4)"
        
        query += " ORDER BY pd.symbol, pd.price_date"
        
        try:
            records = self.snowflake.fetch_all(query, tuple(params) if params else None)
            logger.info(f"Extracted {len(records)} price records with financial data")
            
            # Log sample of data for debugging
            if records and len(records) > 0:
                sample = records[0]
                logger.debug(f"Sample record - Symbol: {sample.get('SYMBOL')}, Date: {sample.get('PRICE_DATE')}, "
                           f"Annual data: {sample.get('ANNUAL_REVENUE') is not None}, "
                           f"TTM data: {sample.get('TTM_REVENUE') is not None}, "
                           f"Quarters available: {sample.get('QUARTERS_AVAILABLE', 0)}")
            
            # Store metadata
            self.result.metadata['records_extracted'] = len(records)
            
            return records
        except Exception as e:
            logger.error(f"Failed to extract market data: {e}")
            self.result.errors.append(f"Extract failed: {str(e)}")
            raise
    
    def transform(self, raw_data: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
        """
        Calculate market metrics from price and financial data
        
        Args:
            raw_data: List of records combining price and financial data
            
        Returns:
            Dict with 'metrics' key containing calculated metric records
        """
        logger.info(f"Calculating market metrics for {len(raw_data)} records")
        
        metrics_data = []
        calculation_errors = 0
        
        for record in raw_data:
            try:
                # Base record
                metric_record = {
                    'company_key': record['COMPANY_KEY'],
                    'date_key': record['DATE_KEY'],
                    'financial_key': record.get('FINANCIAL_KEY'),  # May be None if only TTM data
                    'close_price': record['CLOSE_PRICE'],
                    'fiscal_period': record.get('PERIOD_TYPE', 'TTM').upper() if record.get('PERIOD_TYPE') else 'TTM',
                }
                
                # Calculate market cap and enterprise value
                close_price = float(record.get('CLOSE_PRICE', 0) or 0)
                # Prefer annual shares outstanding, fall back to TTM average
                shares_outstanding = float(record.get('SHARES_OUTSTANDING', 0) or record.get('TTM_SHARES_OUTSTANDING', 0) or 0)
                
                if close_price > 0 and shares_outstanding > 0:
                    market_cap = close_price * shares_outstanding
                    metric_record['market_cap'] = round(market_cap, 2)
                    
                    # Enterprise Value = Market Cap + Total Debt - Cash
                    # Use TTM total debt if available, otherwise annual
                    total_debt = float(record.get('TTM_TOTAL_DEBT', 0) or record.get('TOTAL_DEBT', 0) or 0)
                    # Note: We don't have cash in the current query, would need to add
                    enterprise_value = market_cap + total_debt
                    metric_record['enterprise_value'] = round(enterprise_value, 2)
                else:
                    metric_record['market_cap'] = None
                    metric_record['enterprise_value'] = None
                    
                # P/E Ratio (Price to Earnings)
                annual_net_income = float(record.get('ANNUAL_NET_INCOME', 0) or 0)
                ttm_net_income = float(record.get('TTM_NET_INCOME', 0) or 0)
                
                if shares_outstanding > 0:
                    # Annual P/E
                    if annual_net_income > 0:
                        annual_eps = annual_net_income / shares_outstanding
                        metric_record['pe_ratio'] = round(close_price / annual_eps, 2)
                    else:
                        metric_record['pe_ratio'] = None
                    
                    # TTM P/E
                    if ttm_net_income > 0:
                        ttm_eps = ttm_net_income / shares_outstanding
                        metric_record['pe_ratio_ttm'] = round(close_price / ttm_eps, 2)
                    else:
                        metric_record['pe_ratio_ttm'] = None
                else:
                    metric_record['pe_ratio'] = None
                    metric_record['pe_ratio_ttm'] = None
                
                # P/B Ratio (Price to Book)
                # Use TTM total equity if available, otherwise annual
                total_equity = float(record.get('TTM_TOTAL_EQUITY', 0) or record.get('TOTAL_EQUITY', 0) or 0)
                if total_equity > 0 and shares_outstanding > 0:
                    book_value_per_share = total_equity / shares_outstanding
                    metric_record['pb_ratio'] = round(close_price / book_value_per_share, 2)
                else:
                    metric_record['pb_ratio'] = None
                
                # P/S Ratio (Price to Sales)
                annual_revenue = float(record.get('ANNUAL_REVENUE', 0) or 0)
                ttm_revenue = float(record.get('TTM_REVENUE', 0) or 0)
                
                if shares_outstanding > 0:
                    # Annual P/S
                    if annual_revenue > 0:
                        revenue_per_share = annual_revenue / shares_outstanding
                        metric_record['ps_ratio'] = round(close_price / revenue_per_share, 2)
                    else:
                        metric_record['ps_ratio'] = None
                    
                    # TTM P/S
                    if ttm_revenue > 0:
                        ttm_revenue_per_share = ttm_revenue / shares_outstanding
                        metric_record['ps_ratio_ttm'] = round(close_price / ttm_revenue_per_share, 2)
                    else:
                        metric_record['ps_ratio_ttm'] = None
                else:
                    metric_record['ps_ratio'] = None
                    metric_record['ps_ratio_ttm'] = None
                
                # EV/Revenue ratios
                if metric_record.get('enterprise_value') and metric_record['enterprise_value'] > 0:
                    # Annual EV/Revenue
                    if annual_revenue > 0:
                        metric_record['ev_to_revenue'] = round(metric_record['enterprise_value'] / annual_revenue, 2)
                    else:
                        metric_record['ev_to_revenue'] = None
                    
                    # TTM EV/Revenue
                    if ttm_revenue > 0:
                        metric_record['ev_to_revenue_ttm'] = round(metric_record['enterprise_value'] / ttm_revenue, 2)
                    else:
                        metric_record['ev_to_revenue_ttm'] = None
                    
                    # EV/EBITDA (using operating income as proxy)
                    annual_operating_income = float(record.get('ANNUAL_OPERATING_INCOME', 0) or 0)
                    if annual_operating_income > 0:
                        metric_record['ev_to_ebitda'] = round(metric_record['enterprise_value'] / annual_operating_income, 2)
                        metric_record['ev_to_ebit'] = metric_record['ev_to_ebitda']  # Same since we don't have D&A
                    else:
                        metric_record['ev_to_ebitda'] = None
                        metric_record['ev_to_ebit'] = None
                else:
                    metric_record['ev_to_revenue'] = None
                    metric_record['ev_to_revenue_ttm'] = None
                    metric_record['ev_to_ebitda'] = None
                    metric_record['ev_to_ebit'] = None
                
                # Dividend metrics
                annual_dividends = float(record.get('ANNUAL_DIVIDENDS_PAID', 0) or 0)
                ttm_dividends = float(record.get('TTM_DIVIDENDS_PAID', 0) or 0)
                
                # Dividend Yield (using TTM dividends)
                if close_price > 0 and ttm_dividends > 0 and shares_outstanding > 0:
                    dividends_per_share = abs(ttm_dividends) / shares_outstanding  # dividends are usually negative
                    metric_record['dividend_yield'] = round((dividends_per_share / close_price) * 100, 2)
                else:
                    metric_record['dividend_yield'] = None
                
                # Payout Ratio
                if ttm_net_income > 0 and ttm_dividends < 0:  # dividends are negative cash flow
                    metric_record['payout_ratio'] = round((abs(ttm_dividends) / ttm_net_income) * 100, 2)
                else:
                    metric_record['payout_ratio'] = None
                
                # PEG Ratio would require growth rates - skip for now
                metric_record['peg_ratio'] = None
                
                # Mark if using TTM data
                metric_record['is_ttm'] = True if ttm_revenue else False
                
                # Add metadata fields
                metric_record['annual_accepted_date'] = record.get('ANNUAL_ACCEPTED_DATE')
                metric_record['ttm_accepted_date'] = record.get('LATEST_TTM_ACCEPTED_DATE')
                metric_record['ttm_quarters_available'] = record.get('QUARTERS_AVAILABLE', 0)
                
                metrics_data.append(metric_record)
                
            except Exception as e:
                logger.warning(f"Error calculating metrics for record {record.get('PRICE_KEY')}: {e}")
                calculation_errors += 1
        
        # Store metadata
        self.result.metadata['records_transformed'] = len(metrics_data)
        self.result.metadata['calculation_errors'] = calculation_errors
        
        logger.info(f"Successfully calculated metrics for {len(metrics_data)} records")
        if calculation_errors > 0:
            logger.warning(f"Failed to calculate metrics for {calculation_errors} records")
        
        return {'metrics': metrics_data}
    
    def load(self, transformed_data: Dict[str, List[Dict[str, Any]]]) -> int:
        """
        Load calculated metrics to FACT_MARKET_METRICS
        
        Args:
            transformed_data: Dict with 'metrics' key containing metric records
            
        Returns:
            Number of records loaded
        """
        metrics_data = transformed_data.get('metrics', [])
        
        if not metrics_data:
            logger.warning("No metric data to load")
            return 0
        
        logger.info(f"Loading {len(metrics_data)} metric records to FACT_MARKET_METRICS")
        
        try:
            # Add timestamp
            current_timestamp = datetime.now(timezone.utc)
            for record in metrics_data:
                record['created_timestamp'] = current_timestamp
            
            # Insert into FACT_MARKET_METRICS
            self.snowflake.bulk_insert('ANALYTICS.FACT_MARKET_METRICS', metrics_data)
            
            logger.info(f"Successfully loaded {len(metrics_data)} records to FACT_MARKET_METRICS")
            return len(metrics_data)
            
        except Exception as e:
            logger.error(f"Failed to load metric data: {e}")
            self.result.errors.append(f"Load failed: {str(e)}")
            raise
    
    def run(self, symbols: Optional[List[str]] = None,
            start_date: Optional[str] = None,
            end_date: Optional[str] = None) -> Dict[str, Any]:
        """
        Run the complete market metrics ETL pipeline
        
        Args:
            symbols: Optional list of symbols to process
            start_date: Optional start date for price data
            end_date: Optional end date for price data
            
        Returns:
            ETL result summary
        """
        logger.info(f"Starting {self.job_name} ETL pipeline")
        
        try:
            # Extract
            raw_data = self.extract(symbols, start_date, end_date)
            if not raw_data:
                logger.info("No new data to process for market metrics")
                return {
                    'status': 'success',
                    'records_processed': 0,
                    'message': 'No new data to process'
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
            logger.error(f"Market metrics ETL pipeline failed: {e}")
            return {
                'status': 'failed',
                'error': str(e),
                'records_processed': 0
            }