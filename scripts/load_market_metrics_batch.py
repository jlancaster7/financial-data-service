#!/usr/bin/env python
"""
Load market metrics in smaller batches to avoid timeouts
"""
import argparse
from datetime import datetime, timedelta
from loguru import logger
import sys
import os

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.utils.config import Config
from src.etl.market_metrics_etl import MarketMetricsETL
from src.db.snowflake_connector import SnowflakeConnector


def process_date_range(etl, symbols, start_date, end_date):
    """Process a single date range"""
    try:
        logger.info(f"Processing {symbols} from {start_date} to {end_date}")
        result = etl.run(
            symbols=symbols,
            start_date=start_date.strftime('%Y-%m-%d'),
            end_date=end_date.strftime('%Y-%m-%d')
        )
        
        if result.get('status') == 'success':
            logger.info(f"✓ Loaded {result.get('records_loaded', 0)} records")
            return result.get('records_loaded', 0)
        else:
            logger.error(f"✗ Failed: {result.get('error', 'Unknown error')}")
            return 0
    except Exception as e:
        logger.error(f"✗ Exception: {e}")
        return 0


def main():
    parser = argparse.ArgumentParser(description="Load market metrics in batches")
    parser.add_argument("--symbols", nargs="+", required=True, help="Stock symbols to process")
    parser.add_argument("--from-date", required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--to-date", required=True, help="End date (YYYY-MM-DD)")
    parser.add_argument("--batch-days", type=int, default=90, help="Days per batch (default: 90)")
    
    args = parser.parse_args()
    
    # Parse dates
    start_date = datetime.strptime(args.from_date, '%Y-%m-%d')
    end_date = datetime.strptime(args.to_date, '%Y-%m-%d')
    
    logger.info(f"Loading market metrics for {args.symbols}")
    logger.info(f"Date range: {start_date.date()} to {end_date.date()}")
    logger.info(f"Batch size: {args.batch_days} days")
    
    # Initialize
    config = Config()
    snowflake = SnowflakeConnector(config.snowflake, use_pooling=True)
    
    try:
        snowflake.connect()
        
        # Create ETL instance
        etl = MarketMetricsETL(config)
        etl.snowflake = snowflake
        
        # Process each symbol separately
        for symbol in args.symbols:
            logger.info(f"\nProcessing {symbol}...")
            total_loaded = 0
            
            # Process in date batches
            current_start = start_date
            while current_start < end_date:
                batch_end = min(current_start + timedelta(days=args.batch_days), end_date)
                
                loaded = process_date_range(etl, [symbol], current_start, batch_end)
                total_loaded += loaded
                
                current_start = batch_end + timedelta(days=1)
            
            logger.info(f"Total loaded for {symbol}: {total_loaded} records")
        
        logger.info("\n✓ All batches completed successfully")
        
    finally:
        snowflake.disconnect()


if __name__ == "__main__":
    main()