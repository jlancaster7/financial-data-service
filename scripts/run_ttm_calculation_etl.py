#!/usr/bin/env python3
"""
Run TTM (Trailing Twelve Month) Calculation ETL Pipeline
"""
import argparse
import sys
from pathlib import Path

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent))

from loguru import logger
from src.utils.config import Config
from src.etl.ttm_calculation_etl import TTMCalculationETL


def main():
    parser = argparse.ArgumentParser(description="Run TTM Calculation ETL Pipeline")
    parser.add_argument(
        "--symbols",
        nargs="+",
        help="Stock symbols to process (e.g., AAPL MSFT GOOGL). If not specified, processes all symbols"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Extract and transform data but don't load to database"
    )
    
    args = parser.parse_args()
    
    logger.info("=" * 80)
    logger.info("TTM Calculation ETL Pipeline")
    logger.info("=" * 80)
    
    if args.symbols:
        logger.info(f"Symbols: {', '.join(args.symbols)}")
    else:
        logger.info("Symbols: All available")
    
    if args.dry_run:
        logger.info("Mode: DRY RUN (no database changes)")
    
    # Load configuration
    config = Config.load()
    
    # Initialize ETL pipeline
    etl = TTMCalculationETL(config)
    
    if args.dry_run:
        logger.info("\nDRY RUN: Extracting and transforming data only")
        try:
            # Extract
            opportunities = etl.extract(args.symbols)
            if opportunities:
                logger.info(f"Found {len(opportunities)} TTM calculation opportunities:")
                for opp in opportunities[:5]:  # Show first 5
                    logger.info(f"  {opp['SYMBOL']} - Date: {opp['CALCULATION_DATE']} - Quarters: {opp['QUARTERS_AVAILABLE']}")
                if len(opportunities) > 5:
                    logger.info(f"  ... and {len(opportunities) - 5} more")
                
                # Transform
                transformed = etl.transform(opportunities)
                ttm_records = transformed.get('ttm_records', [])
                logger.info(f"\nWould create {len(ttm_records)} TTM records")
                
                if ttm_records:
                    sample = ttm_records[0]
                    logger.info("\nSample TTM record:")
                    logger.info(f"  Company Key: {sample['company_key']}")
                    logger.info(f"  Calculation Date: {sample['calculation_date']}")
                    logger.info(f"  Quarters Included: {sample['quarters_included']}")
                    logger.info(f"  TTM Revenue: ${float(sample.get('ttm_revenue', 0)) / 1e9:.2f}B")
                    logger.info(f"  TTM Net Income: ${float(sample.get('ttm_net_income', 0)) / 1e9:.2f}B")
                    logger.info(f"  TTM EPS Diluted: ${float(sample.get('ttm_eps_diluted', 0)):.2f}")
            else:
                logger.info("No TTM calculation opportunities found")
        except Exception as e:
            logger.error(f"Dry run failed: {e}")
            return 1
    else:
        # Run full ETL
        result = etl.run(symbols=args.symbols)
        
        logger.info("\n" + "=" * 80)
        logger.info("RESULTS")
        logger.info("=" * 80)
        
        if result['status'] == 'success':
            logger.info(f"✓ Status: SUCCESS")
            if result.get('opportunities_found', 0) > 0:
                logger.info(f"  Opportunities Found: {result['opportunities_found']}")
                logger.info(f"  Records Loaded: {result['records_loaded']}")
            else:
                logger.info(f"  {result.get('message', 'No new calculations needed')}")
        else:
            logger.error(f"✗ Status: FAILED")
            logger.error(f"  Error: {result.get('error', 'Unknown error')}")
            return 1
    
    return 0


if __name__ == "__main__":
    sys.exit(main())