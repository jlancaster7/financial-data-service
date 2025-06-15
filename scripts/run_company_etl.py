#!/usr/bin/env python3
"""
Script to run company profile ETL pipeline
"""
import sys
import argparse
from pathlib import Path
from typing import List
from loguru import logger

# Add the src directory to the path
sys.path.append(str(Path(__file__).parent.parent))

from src.utils.config import Config
from src.db.snowflake_connector import SnowflakeConnector
from src.api.fmp_client import FMPClient
from src.etl.company_etl import CompanyETL


def get_sp500_symbols(fmp_client: FMPClient) -> List[str]:
    """Get S&P 500 constituent symbols"""
    try:
        constituents = fmp_client.get_sp500_constituents()
        symbols = [c['symbol'] for c in constituents if c.get('symbol')]
        logger.info(f"Retrieved {len(symbols)} S&P 500 symbols")
        return symbols
    except Exception as e:
        logger.error(f"Failed to get S&P 500 constituents: {e}")
        return []


def main():
    """Main execution function"""
    parser = argparse.ArgumentParser(description="Run company profile ETL pipeline")
    parser.add_argument(
        "--symbols",
        nargs="+",
        help="Specific symbols to process (e.g., AAPL MSFT GOOGL)"
    )
    parser.add_argument(
        "--sp500",
        action="store_true",
        help="Process all S&P 500 companies"
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=1000,
        help="Batch size for loading (default: 1000)"
    )
    parser.add_argument(
        "--no-analytics",
        action="store_true",
        help="Skip updating analytics layer (DIM_COMPANY)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Extract and transform only, don't load to database"
    )
    
    args = parser.parse_args()
    
    # Validate arguments
    if not args.symbols and not args.sp500:
        parser.error("Must specify either --symbols or --sp500")
    
    try:
        # Load configuration
        config = Config.load()
        logger.info("Configuration loaded")
        
        # Create connections
        snowflake = SnowflakeConnector(config.snowflake)
        fmp_client = FMPClient(config.fmp)
        
        # Connect to Snowflake
        snowflake.connect()
        logger.info("Connected to Snowflake")
        
        # Determine symbols to process
        if args.sp500:
            symbols = get_sp500_symbols(fmp_client)
            if not symbols:
                logger.error("No S&P 500 symbols retrieved")
                return False
        else:
            symbols = args.symbols
        
        logger.info(f"Processing {len(symbols)} symbols")
        
        # Create and configure ETL pipeline
        etl = CompanyETL(config)
        # Share the snowflake connection
        etl.snowflake = snowflake
        
        # Add monitoring hooks for visibility
        def log_extraction_complete(etl_instance, data=None, **kwargs):
            if data:
                logger.info(f"Extraction complete: {len(data)} profiles retrieved")
        
        def log_transformation_complete(etl_instance, data=None, **kwargs):
            if data:
                raw_count = len(data.get('raw', []))
                staging_count = len(data.get('staging', []))
                logger.info(f"Transformation complete: {raw_count} raw, {staging_count} staging records")
        
        etl.add_post_extract_hook(log_extraction_complete)
        etl.add_post_transform_hook(log_transformation_complete)
        
        # Run ETL pipeline
        if args.dry_run:
            logger.info("DRY RUN: Extracting and transforming only")
            
            # Extract
            raw_data = etl.extract(symbols=symbols, load_to_analytics=not args.no_analytics)
            etl.result.records_extracted = len(raw_data)
            
            # Transform
            transformed_data = etl.transform(raw_data)
            etl.result.records_transformed = len(transformed_data.get('staging', []))
            
            # Log results
            logger.info(f"Dry run complete:")
            logger.info(f"  - Extracted: {etl.result.records_extracted} profiles")
            logger.info(f"  - Transformed: {etl.result.records_transformed} records")
            logger.info(f"  - New companies: {etl.result.metadata.get('new_companies', 0)}")
            logger.info(f"  - Updated companies: {etl.result.metadata.get('updated_companies', 0)}")
            
            return True
        else:
            # Run full ETL pipeline
            # Extract
            raw_data = etl.extract(symbols=symbols, load_to_analytics=not args.no_analytics)
            
            # Transform
            transformed_data = etl.transform(raw_data)
            
            # Load
            etl.load(transformed_data)
            
            # Get result
            result = etl.result
            
            # Log summary
            logger.info("\nETL Pipeline Summary:")
            logger.info(f"  Status: {result.status.value}")
            logger.info(f"  Duration: {result.duration_seconds:.2f} seconds")
            logger.info(f"  Records extracted: {result.records_extracted}")
            logger.info(f"  Records transformed: {result.records_transformed}")
            logger.info(f"  Records loaded: {result.records_loaded}")
            logger.info(f"  New companies: {result.metadata.get('new_companies', 0)}")
            logger.info(f"  Updated companies: {result.metadata.get('updated_companies', 0)}")
            
            if result.errors:
                logger.warning(f"  Errors: {len(result.errors)}")
                for error in result.errors[:5]:
                    logger.warning(f"    - {error}")
            
            return result.status.value in ['success', 'partial']
        
    except Exception as e:
        logger.error(f"ETL pipeline failed: {e}")
        return False
    finally:
        # Clean up connections
        if 'snowflake' in locals() and snowflake:
            snowflake.disconnect()
            logger.info("Disconnected from Snowflake")


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)