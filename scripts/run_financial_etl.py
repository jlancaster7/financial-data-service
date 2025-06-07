#!/usr/bin/env python3
"""
Run Financial Statement ETL Pipeline
"""
import argparse
from datetime import datetime, date, timedelta
import sys
from pathlib import Path

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent))

from loguru import logger
from src.utils.config import Config
from src.etl.financial_statement_etl import FinancialStatementETL
from src.etl.etl_monitor import ETLMonitor
from src.api.fmp_client import FMPClient
from src.db.snowflake_connector import SnowflakeConnector


def main():
    parser = argparse.ArgumentParser(description="Run Financial Statement ETL Pipeline")
    parser.add_argument(
        "--symbols",
        nargs="+",
        help="Stock symbols to process (e.g., AAPL MSFT GOOGL). If not specified, uses S&P 500 list"
    )
    parser.add_argument(
        "--period",
        choices=['annual', 'quarterly'],
        default='annual',
        help="Period type for financial statements (default: annual)"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=5,
        help="Number of periods to fetch for each statement type (default: 5)"
    )
    parser.add_argument(
        "--skip-analytics",
        action="store_true",
        help="Skip updating FACT_FINANCIAL_METRICS table"
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
    
    logger.info(f"Processing {len(args.symbols)} symbols for {args.period} financial statements")
    
    # Initialize ETL pipeline
    etl = FinancialStatementETL(config)
    
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
                raw_data = etl.extract(batch_symbols, args.period, args.limit)
                if raw_data:
                    # Transform
                    transformed = etl.transform(raw_data)
                    total_staging = sum(
                        len(stmt_type['staging']) 
                        for stmt_type in transformed.values()
                    )
                    total_raw = sum(
                        len(stmt_type['raw']) 
                        for stmt_type in transformed.values()
                    )
                    logger.info(f"Would load {total_raw} raw records and {total_staging} staging records")
                    logger.info(f"  Income: {len(transformed['income']['staging'])} records")
                    logger.info(f"  Balance: {len(transformed['balance']['staging'])} records")
                    logger.info(f"  Cash Flow: {len(transformed['cashflow']['staging'])} records")
                    total_records += total_raw
            except Exception as e:
                logger.error(f"Batch {batch_num} failed: {e}")
                failed_batches += 1
        else:
            # Run full ETL
            result = etl.run(
                symbols=batch_symbols,
                period=args.period,
                limit=args.limit,
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
    logger.info(f"Financial Statement ETL Pipeline Complete")
    logger.info(f"Total symbols processed: {len(args.symbols)}")
    logger.info(f"Total records: {total_records}")
    logger.info(f"Failed batches: {failed_batches}")
    logger.info(f"Period: {args.period}")
    logger.info(f"Periods per symbol: {args.limit}")
    if args.skip_analytics:
        logger.info("Analytics layer update was skipped")
    logger.info(f"{'='*60}")
    
    return 0 if failed_batches == 0 else 1


if __name__ == "__main__":
    sys.exit(main())