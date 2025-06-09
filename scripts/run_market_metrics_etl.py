#!/usr/bin/env python3
"""
Run Market Metrics ETL pipeline to calculate daily market-based metrics
"""
import sys
import argparse
from pathlib import Path
from datetime import datetime, timedelta
sys.path.append(str(Path(__file__).parent.parent))

from loguru import logger
from src.utils.config import Config
from src.etl.market_metrics_etl import MarketMetricsETL


def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description="Calculate daily market metrics combining price and financial data"
    )
    
    parser.add_argument(
        "--symbols",
        nargs="+",
        help="Specific symbols to process (e.g., AAPL MSFT GOOGL)"
    )
    
    parser.add_argument(
        "--start-date",
        help="Start date for price data (YYYY-MM-DD)"
    )
    
    parser.add_argument(
        "--end-date", 
        help="End date for price data (YYYY-MM-DD)"
    )
    
    parser.add_argument(
        "--days-back",
        type=int,
        default=30,
        help="Number of days back from today to process (default: 30)"
    )
    
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be processed without running"
    )
    
    return parser.parse_args()


def main():
    """Main execution function"""
    args = parse_args()
    
    logger.info("=" * 80)
    logger.info("Market Metrics ETL Pipeline")
    logger.info("=" * 80)
    
    # Show parameters
    if args.symbols:
        logger.info(f"Symbols: {', '.join(args.symbols)}")
    else:
        logger.info("Symbols: All symbols with price and financial data")
    
    # Determine date range
    if args.start_date:
        start_date = args.start_date
    else:
        start_date = (datetime.now() - timedelta(days=args.days_back)).strftime('%Y-%m-%d')
    
    if args.end_date:
        end_date = args.end_date
    else:
        end_date = datetime.now().strftime('%Y-%m-%d')
    
    logger.info(f"Date Range: {start_date} to {end_date}")
    
    if args.dry_run:
        logger.info("DRY RUN MODE - No data will be loaded")
        logger.info("\nWould calculate market metrics for price data matching the criteria")
        return 0
    
    # Load configuration
    config = Config.load()
    
    # Create and run ETL
    try:
        etl = MarketMetricsETL(config)
        
        # Run the pipeline
        result = etl.run(
            symbols=args.symbols,
            start_date=start_date,
            end_date=end_date
        )
        
        # Display results
        logger.info("\n" + "=" * 80)
        logger.info("RESULTS")
        logger.info("=" * 80)
        
        if result['status'] == 'success':
            logger.info(f"✓ Status: {result['status'].upper()}")
            
            if result.get('records_processed', 0) == 0:
                logger.info("  No new data to process")
            else:
                logger.info(f"  Price records extracted: {result.get('records_extracted', 0)}")
                logger.info(f"  Market metrics calculated: {result.get('records_loaded', 0)}")
                
                if result.get('calculation_errors', 0) > 0:
                    logger.warning(f"  Calculation errors: {result.get('calculation_errors', 0)}")
            
            return 0
        else:
            logger.error(f"✗ Status: {result['status'].upper()}")
            logger.error(f"  Error: {result.get('error', 'Unknown error')}")
            return 1
            
    except Exception as e:
        logger.error(f"Failed to run market metrics ETL: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())