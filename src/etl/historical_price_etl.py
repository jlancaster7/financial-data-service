"""
Historical Price ETL Pipeline
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


class HistoricalPriceETL(BaseETL):
    """ETL pipeline for historical price data"""
    
    def __init__(self, config: Config):
        """Initialize Historical Price ETL pipeline"""
        # Create instances
        snowflake_connector = SnowflakeConnector(config.snowflake)
        fmp_client = FMPClient(config.fmp)
        
        # Initialize base class
        super().__init__(
            job_name="HistoricalPriceETL",
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
        
    def extract(self, symbols: List[str], from_date: Optional[date] = None, 
                to_date: Optional[date] = None) -> List[Dict[str, Any]]:
        """
        Extract historical price data from FMP API
        
        Args:
            symbols: List of stock symbols
            from_date: Start date for historical data
            to_date: End date for historical data
            
        Returns:
            List of raw price data records
        """
        all_price_data = []
        
        # Default to last 30 days if no date range specified
        if not from_date:
            from_date = date.today() - timedelta(days=30)
        if not to_date:
            to_date = date.today()
            
        logger.info(f"Extracting historical prices for {len(symbols)} symbols from {from_date} to {to_date}")
        
        for symbol in symbols:
            try:
                price_data = self.fmp_client.get_historical_prices(
                    symbol=symbol,
                    from_date=from_date,
                    to_date=to_date
                )
                
                if price_data:
                    # Add symbol to each record since it might not be in the response
                    for record in price_data:
                        record['symbol'] = symbol
                    all_price_data.extend(price_data)
                    logger.info(f"Extracted {len(price_data)} price records for {symbol}")
                else:
                    logger.warning(f"No price data found for {symbol}")
                    
            except Exception as e:
                logger.error(f"Failed to extract prices for {symbol}: {e}")
                self.job_errors.append({
                    'symbol': symbol,
                    'error': str(e),
                    'phase': 'extract'
                })
        
        logger.info(f"Total price records extracted: {len(all_price_data)}")
        return all_price_data
    
    def transform(self, raw_data: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
        """
        Transform raw price data to structured format
        
        Args:
            raw_data: List of raw price records
            
        Returns:
            Dict with 'raw' and 'staging' transformed records
        """
        # Group by symbol for batch transformation
        symbol_data = {}
        for record in raw_data:
            symbol = record.get('symbol')
            if symbol not in symbol_data:
                symbol_data[symbol] = []
            symbol_data[symbol].append(record)
        
        all_transformed = {'raw': [], 'staging': []}
        
        for symbol, price_records in symbol_data.items():
            try:
                # Transform using FMPTransformer
                transformed = self.transformer.transform_historical_prices(symbol, price_records)
                
                # Validate staging records
                validated_staging = []
                for record in transformed['staging']:
                    is_valid, issues = self.validator.validate_historical_price(record)
                    if is_valid:
                        validated_staging.append(record)
                    else:
                        logger.warning(f"Validation failed for {symbol} on {record.get('price_date')}: {issues}")
                        self.data_quality_issues.extend(issues)
                
                all_transformed['raw'].extend(transformed['raw'])
                all_transformed['staging'].extend(validated_staging)
                
            except Exception as e:
                logger.error(f"Failed to transform prices for {symbol}: {e}")
                self.job_errors.append({
                    'symbol': symbol,
                    'error': str(e),
                    'phase': 'transform'
                })
        
        logger.info(f"Transformed {len(all_transformed['raw'])} raw and {len(all_transformed['staging'])} staging records")
        return all_transformed
    
    def load(self, transformed_data: Dict[str, List[Dict[str, Any]]]) -> int:
        """
        Load transformed data to Snowflake
        
        Args:
            transformed_data: Dict with 'raw' and 'staging' data
            
        Returns:
            Number of records loaded
        """
        total_loaded = 0
        
        with self.snowflake as conn:
            # Load to RAW layer
            if transformed_data['raw']:
                try:
                    affected = conn.bulk_insert(
                        table='RAW_DATA.RAW_HISTORICAL_PRICES',
                        data=transformed_data['raw']
                    )
                    total_loaded += affected or 0
                    logger.info(f"Loaded {affected} records to RAW_HISTORICAL_PRICES")
                except Exception as e:
                    logger.error(f"Failed to load raw price data: {e}")
                    self.job_errors.append({
                        'error': str(e),
                        'phase': 'load_raw'
                    })
            
            # Load to STAGING layer using MERGE to avoid duplicates
            if transformed_data['staging']:
                try:
                    affected = conn.merge(
                        table='STAGING.STG_HISTORICAL_PRICES',
                        data=transformed_data['staging'],
                        merge_keys=['symbol', 'price_date']  # Unique keys for historical prices
                    )
                    total_loaded += affected or 0
                    logger.info(f"Merged {affected} records to STG_HISTORICAL_PRICES")
                except Exception as e:
                    logger.error(f"Failed to load staging price data: {e}")
                    self.job_errors.append({
                        'error': str(e),
                        'phase': 'load_staging'
                    })
        
        return total_loaded
    
    def update_fact_table(self, symbols: List[str], from_date: Optional[date] = None):
        """
        Update FACT_DAILY_PRICES with calculated metrics
        
        Args:
            symbols: List of symbols to update
            from_date: Start date for updates (defaults to oldest staging data)
        """
        with self.snowflake as conn:
            try:
                # If no from_date, get the oldest date in staging
                if not from_date:
                    query = """
                    SELECT MIN(price_date) as min_date
                    FROM STAGING.STG_HISTORICAL_PRICES
                    WHERE symbol = ANY(%s)
                    """
                    result = conn.fetch_one(query, (symbols,))
                    from_date = result['min_date'] if result and result['min_date'] else date.today() - timedelta(days=30)
                
                # Insert/update FACT_DAILY_PRICES with calculated metrics
                # Format symbol list for IN clause
                symbol_placeholders = ','.join(['%s' for _ in symbols])
                
                merge_query = f"""
                MERGE INTO ANALYTICS.FACT_DAILY_PRICES AS target
                USING (
                    SELECT 
                        c.company_key,
                        d.date_key,
                        s.open_price,
                        s.high_price,
                        s.low_price,
                        s.close_price,
                        s.adj_close,
                        s.volume,
                        s.close_price - LAG(s.close_price) OVER (PARTITION BY s.symbol ORDER BY s.price_date) AS change_amount,
                        CASE 
                            WHEN LAG(s.close_price) OVER (PARTITION BY s.symbol ORDER BY s.price_date) > 0 
                            THEN ((s.close_price - LAG(s.close_price) OVER (PARTITION BY s.symbol ORDER BY s.price_date)) / 
                                  LAG(s.close_price) OVER (PARTITION BY s.symbol ORDER BY s.price_date)) * 100
                            ELSE NULL 
                        END AS change_percent
                    FROM STAGING.STG_HISTORICAL_PRICES s
                    INNER JOIN ANALYTICS.DIM_COMPANY c ON s.symbol = c.symbol AND c.is_current = TRUE
                    INNER JOIN ANALYTICS.DIM_DATE d ON s.price_date = d.date
                    WHERE s.symbol IN ({symbol_placeholders})
                      AND s.price_date >= %s
                ) AS source
                ON target.company_key = source.company_key 
                   AND target.date_key = source.date_key
                WHEN MATCHED THEN UPDATE SET
                    open_price = source.open_price,
                    high_price = source.high_price,
                    low_price = source.low_price,
                    close_price = source.close_price,
                    adj_close = source.adj_close,
                    volume = source.volume,
                    change_amount = source.change_amount,
                    change_percent = source.change_percent
                WHEN NOT MATCHED THEN INSERT (
                    company_key, date_key,
                    open_price, high_price, low_price, close_price,
                    adj_close, volume, change_amount, change_percent
                ) VALUES (
                    source.company_key, source.date_key,
                    source.open_price, source.high_price, source.low_price, source.close_price,
                    source.adj_close, source.volume, source.change_amount, source.change_percent
                )
                """
                
                # Prepare parameters: symbols + from_date
                params = list(symbols) + [from_date]
                affected = conn.execute(merge_query, tuple(params))
                logger.info(f"Updated {affected} records in FACT_DAILY_PRICES")
                
            except Exception as e:
                logger.error(f"Failed to update fact table: {e}")
                self.job_errors.append({
                    'error': str(e),
                    'phase': 'update_fact_table'
                })
    
    def run(self, symbols: List[str], from_date: Optional[date] = None,
            to_date: Optional[date] = None, update_analytics: bool = True) -> ETLResult:
        """
        Run the complete ETL pipeline for historical prices
        
        Args:
            symbols: List of stock symbols
            from_date: Start date for historical data
            to_date: End date for historical data
            update_analytics: Whether to update FACT_DAILY_PRICES
            
        Returns:
            ETLResult with job execution details
        """
        logger.info(f"Starting Historical Price ETL for {len(symbols)} symbols")
        
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
            metadata={'symbols': symbols, 'from_date': str(from_date), 'to_date': str(to_date)}
        )
        
        try:
            # Extract
            raw_data = self.extract(symbols, from_date, to_date)
            self.result.records_extracted = len(raw_data)
            
            if not raw_data:
                logger.warning("No data extracted")
                self.result.status = ETLStatus.SUCCESS
                self.result.end_time = datetime.now(timezone.utc)
                self.result.metadata['message'] = "No data to process"
                return self.result
            
            # Transform
            transformed = self.transform(raw_data)
            self.result.records_transformed = len(transformed.get('staging', []))
            
            # Load
            records_loaded = self.load(transformed)
            self.result.records_loaded = records_loaded
            
            # Update analytics layer if requested
            if update_analytics and records_loaded > 0:
                self.update_fact_table(symbols, from_date)
            
            # Determine status and finalize result
            if self.job_errors:
                self.result.status = ETLStatus.PARTIAL if records_loaded > 0 else ETLStatus.FAILED
                self.result.errors = [str(e) for e in self.job_errors]
            else:
                self.result.status = ETLStatus.SUCCESS
            
            self.result.end_time = datetime.now(timezone.utc)
            self.result.metadata['message'] = f"Processed {len(raw_data)} price records for {len(symbols)} symbols"
            
            # Save monitoring data if enabled
            if self.monitor:
                self.monitor.save_job_result(self.result)
            
            return self.result
            
        except Exception as e:
            logger.error(f"Historical Price ETL failed: {e}")
            self.result.status = ETLStatus.FAILED
            self.result.end_time = datetime.now(timezone.utc)
            self.result.errors = [str(e)]
            self.result.metadata['message'] = f"Pipeline failed: {str(e)}"
            
            if self.monitor:
                self.monitor.save_job_result(self.result)
                
            return self.result