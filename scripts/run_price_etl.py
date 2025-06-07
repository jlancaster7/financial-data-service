#!/usr/bin/env python3
"""
Run Historical Price ETL Pipeline
"""
import argparse
from datetime import datetime, date, timedelta
import sys
from pathlib import Path

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent))

from loguru import logger
from src.utils.config import Config
from src.etl.historical_price_etl import HistoricalPriceETL
from src.etl.etl_monitor import ETLMonitor
from src.api.fmp_client import FMPClient
from src.db.snowflake_connector import SnowflakeConnector


def parse_date(date_string: str) -> date:
    """Parse date string in YYYY-MM-DD format"""
    try:
        return datetime.strptime(date_string, "%Y-%m-%d").date()
    except ValueError:
        raise argparse.ArgumentTypeError(f"Invalid date format: {date_string}. Use YYYY-MM-DD")


def main():
    parser = argparse.ArgumentParser(description="Run Historical Price ETL Pipeline")
    parser.add_argument(
        "--symbols",
        nargs="+",
        help="Stock symbols to process (e.g., AAPL MSFT GOOGL). If not specified, uses S&P 500 list"
    )
    parser.add_argument(
        "--from-date",
        type=parse_date,
        help="Start date for historical data (YYYY-MM-DD). Default: 30 days ago"
    )
    parser.add_argument(
        "--to-date", 
        type=parse_date,
        help="End date for historical data (YYYY-MM-DD). Default: today"
    )
    parser.add_argument(
        "--days-back",
        type=int,
        default=30,
        help="Number of days back to fetch if from-date not specified (default: 30)"
    )
    parser.add_argument(
        "--skip-analytics",
        action="store_true",
        help="Skip updating FACT_DAILY_PRICES table"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Extract and transform data but don't load to database"
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=10,
        help="Number of symbols to process in each batch (default: 10)"
    )
    
    args = parser.parse_args()
    
    # Load configuration
    config = Config.load()
    
    # If no symbols specified, get S&P 500 list
    if not args.symbols:
        logger.info("No symbols specified, fetching S&P 500 list")
        try:
            fmp_client = FMPClient(config.fmp)
            sp500_data = fmp_client.get_sp500_constituents()
            args.symbols = [item['symbol'] for item in sp500_data]
            logger.info(f"Found {len(args.symbols)} S&P 500 symbols")
        except Exception as e:
            logger.error(f"Failed to fetch S&P 500 list: {e}")
            return 1
    
    # Set date range
    if not args.from_date:
        args.from_date = date.today() - timedelta(days=args.days_back)
    if not args.to_date:
        args.to_date = date.today()
    
    logger.info(f"Processing {len(args.symbols)} symbols from {args.from_date} to {args.to_date}")
    
    # Initialize ETL pipeline
    etl = HistoricalPriceETL(config)
    
    # Initialize monitoring if enabled
    monitor = None
    if config.app.enable_monitoring and not args.dry_run:
        monitor = ETLMonitor(SnowflakeConnector(config.snowflake))
    
    # Process in batches
    total_records = 0
    failed_batches = 0
    
    for i in range(0, len(args.symbols), args.batch_size):
        batch_symbols = args.symbols[i:i + args.batch_size]
        batch_num = (i // args.batch_size) + 1
        total_batches = (len(args.symbols) + args.batch_size - 1) // args.batch_size
        
        logger.info(f"Processing batch {batch_num}/{total_batches}: {', '.join(batch_symbols)}")
        
        if args.dry_run:
            logger.info("DRY RUN: Extracting and transforming data only")
            try:
                # Extract
                raw_data = etl.extract(batch_symbols, args.from_date, args.to_date)
                if raw_data:
                    # Transform
                    transformed = etl.transform(raw_data)
                    logger.info(f"Would load {len(transformed['raw'])} raw records and {len(transformed['staging'])} staging records")
                    total_records += len(transformed['raw'])
            except Exception as e:
                logger.error(f"Batch {batch_num} failed: {e}")
                failed_batches += 1
        else:
            # Run full ETL
            result = etl.run(
                symbols=batch_symbols,
                from_date=args.from_date,
                to_date=args.to_date,
                update_analytics=not args.skip_analytics
            )
            
            logger.info(f"Batch {batch_num} result: {result.status.value} - {result.metadata.get('message', 'Completed')}")
            
            if result.records_loaded > 0:
                total_records += result.records_loaded
            
            if result.status.value == "FAILED":
                failed_batches += 1
            
            # Save monitoring data
            if monitor:
                monitor.save_job_result(result)
    
    # Summary
    logger.info(f"\n{'='*60}")
    logger.info(f"ETL Pipeline Complete")
    logger.info(f"Total symbols processed: {len(args.symbols)}")
    logger.info(f"Total records: {total_records}")
    logger.info(f"Failed batches: {failed_batches}")
    logger.info(f"Date range: {args.from_date} to {args.to_date}")
    if args.skip_analytics:
        logger.info("Analytics layer update was skipped")
    logger.info(f"{'='*60}")
    
    return 0 if failed_batches == 0 else 1


if __name__ == "__main__":
    sys.exit(main())