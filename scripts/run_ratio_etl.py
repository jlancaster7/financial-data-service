#!/usr/bin/env python3
"""
Run Financial Ratio ETL pipeline to calculate and load financial ratios
"""
import sys
import argparse
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

from loguru import logger
from src.utils.config import Config
from src.etl.financial_ratio_etl import FinancialRatioETL


def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description="Calculate financial ratios from FACT_FINANCIALS data"
    )
    
    parser.add_argument(
        "--symbols",
        nargs="+",
        help="Specific symbols to process (e.g., AAPL MSFT GOOGL)"
    )
    
    parser.add_argument(
        "--fiscal-start-date",
        help="Start date for fiscal period filter (YYYY-MM-DD)"
    )
    
    parser.add_argument(
        "--fiscal-end-date", 
        help="End date for fiscal period filter (YYYY-MM-DD)"
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
    logger.info("Financial Ratio ETL Pipeline")
    logger.info("=" * 80)
    
    # Show parameters
    if args.symbols:
        logger.info(f"Symbols: {', '.join(args.symbols)}")
    else:
        logger.info("Symbols: All symbols with financial data")
    
    if args.fiscal_start_date:
        logger.info(f"Fiscal Start Date: {args.fiscal_start_date}")
    if args.fiscal_end_date:
        logger.info(f"Fiscal End Date: {args.fiscal_end_date}")
    
    if args.dry_run:
        logger.info("DRY RUN MODE - No data will be loaded")
        logger.info("\nWould calculate ratios for financial data matching the criteria")
        return 0
    
    # Load configuration
    config = Config.load()
    
    # Create and run ETL
    try:
        etl = FinancialRatioETL(config)
        
        # Run the pipeline
        result = etl.run(
            symbols=args.symbols,
            fiscal_start_date=args.fiscal_start_date,
            fiscal_end_date=args.fiscal_end_date
        )
        
        # Display results
        logger.info("\n" + "=" * 80)
        logger.info("RESULTS")
        logger.info("=" * 80)
        
        if result['status'] == 'success':
            logger.info(f"✓ Status: {result['status'].upper()}")
            
            if result.get('records_processed', 0) == 0:
                logger.info("  No new financial data to process")
            else:
                logger.info(f"  Records extracted: {result.get('records_extracted', 0)}")
                logger.info(f"  Ratios calculated: {result.get('records_loaded', 0)}")
                
                if result.get('calculation_errors', 0) > 0:
                    logger.warning(f"  Calculation errors: {result.get('calculation_errors', 0)}")
            
            return 0
        else:
            logger.error(f"✗ Status: {result['status'].upper()}")
            logger.error(f"  Error: {result.get('error', 'Unknown error')}")
            return 1
            
    except Exception as e:
        logger.error(f"Failed to run financial ratio ETL: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())