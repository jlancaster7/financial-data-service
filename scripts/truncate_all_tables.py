#!/usr/bin/env python3
"""
Truncate all tables in the data warehouse
WARNING: This will DELETE ALL DATA from all tables!
"""
import sys
from pathlib import Path
from loguru import logger

# Add the src directory to the path
sys.path.append(str(Path(__file__).parent.parent))

from src.utils.config import Config
from src.db.snowflake_connector import SnowflakeConnector


def truncate_all_tables(config: Config, skip_confirmation: bool = False):
    """
    Truncate all tables in the data warehouse
    
    Args:
        config: Configuration object
        skip_confirmation: Skip confirmation prompt (dangerous!)
    """
    # Define tables in order (considering foreign key constraints)
    tables_to_truncate = [
        # Analytics layer first (they reference dimension tables)
        "ANALYTICS.FACT_MARKET_METRICS",
        "ANALYTICS.FACT_FINANCIAL_RATIOS", 
        "ANALYTICS.FACT_FINANCIALS_TTM",
        "ANALYTICS.FACT_FINANCIALS",
        "ANALYTICS.FACT_DAILY_PRICES",
        
        # Dimension tables
        "ANALYTICS.DIM_COMPANY",
        # Note: DIM_DATE is a static dimension, we should NOT truncate it
        
        # Staging layer
        "STAGING.STG_CASH_FLOW",
        "STAGING.STG_BALANCE_SHEET",
        "STAGING.STG_INCOME_STATEMENT",
        "STAGING.STG_HISTORICAL_PRICES",
        "STAGING.STG_COMPANY_PROFILE",
        
        # Raw layer
        "RAW_DATA.RAW_CASH_FLOW",
        "RAW_DATA.RAW_BALANCE_SHEET",
        "RAW_DATA.RAW_INCOME_STATEMENT",
        "RAW_DATA.RAW_HISTORICAL_PRICES",
        "RAW_DATA.RAW_COMPANY_PROFILE",
        
        # Monitoring tables (in RAW_DATA schema)
        "RAW_DATA.ETL_DATA_QUALITY_ISSUES",
        "RAW_DATA.ETL_JOB_METRICS",
        "RAW_DATA.ETL_JOB_ERRORS",
        "RAW_DATA.ETL_JOB_HISTORY"  # Must be last due to foreign keys
    ]
    
    # Show what will be truncated
    logger.warning("=" * 60)
    logger.warning("WARNING: This will DELETE ALL DATA from the following tables:")
    logger.warning("=" * 60)
    for table in tables_to_truncate:
        logger.warning(f"  - {table}")
    logger.warning("=" * 60)
    logger.warning("DIM_DATE will be preserved (static dimension)")
    logger.warning("=" * 60)
    
    if not skip_confirmation:
        response = input("\nAre you ABSOLUTELY SURE you want to delete all data? Type 'DELETE ALL DATA' to confirm: ")
        if response != "DELETE ALL DATA":
            logger.info("Operation cancelled")
            return
    
    # Connect to Snowflake
    with SnowflakeConnector(config.snowflake) as conn:
        logger.info("Connected to Snowflake")
        
        # Get current row counts before truncation
        logger.info("\nCurrent row counts:")
        total_rows = 0
        for table in tables_to_truncate:
            try:
                result = conn.fetch_one(f"SELECT COUNT(*) as count FROM {table}")
                count = result['COUNT'] if result else 0
                total_rows += count
                if count > 0:
                    logger.info(f"  {table}: {count:,} rows")
            except Exception as e:
                logger.debug(f"  {table}: Error getting count - {e}")
        
        logger.info(f"\nTotal rows to be deleted: {total_rows:,}")
        
        if total_rows == 0:
            logger.info("No data to delete - all tables are already empty")
            return
        
        # Truncate tables
        logger.info("\nTruncating tables...")
        success_count = 0
        error_count = 0
        
        for table in tables_to_truncate:
            try:
                logger.info(f"Truncating {table}...")
                conn.execute(f"TRUNCATE TABLE {table}")
                success_count += 1
            except Exception as e:
                logger.error(f"Failed to truncate {table}: {e}")
                error_count += 1
        
        # Verify all tables are empty
        logger.info("\nVerifying truncation...")
        remaining_rows = 0
        for table in tables_to_truncate:
            try:
                result = conn.fetch_one(f"SELECT COUNT(*) as count FROM {table}")
                count = result['COUNT'] if result else 0
                if count > 0:
                    logger.warning(f"  {table} still has {count} rows!")
                    remaining_rows += count
            except Exception:
                pass
        
        # Summary
        logger.info("\n" + "=" * 60)
        logger.info("TRUNCATION COMPLETE")
        logger.info("=" * 60)
        logger.info(f"Tables truncated successfully: {success_count}")
        logger.info(f"Tables with errors: {error_count}")
        logger.info(f"Total rows deleted: {total_rows - remaining_rows:,}")
        if remaining_rows > 0:
            logger.warning(f"WARNING: {remaining_rows:,} rows could not be deleted")
        logger.info("=" * 60)


def main():
    """Main execution function"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Truncate all tables in the data warehouse",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
WARNING: This script will DELETE ALL DATA from all tables!

Examples:
  # Truncate all tables (with confirmation)
  python truncate_all_tables.py
  
  # Truncate all tables without confirmation (DANGEROUS!)
  python truncate_all_tables.py --skip-confirmation
        """
    )
    
    parser.add_argument(
        "--skip-confirmation",
        action="store_true",
        help="Skip confirmation prompt (DANGEROUS!)"
    )
    
    args = parser.parse_args()
    
    # Load configuration
    try:
        config = Config.load()
    except Exception as e:
        logger.error(f"Failed to load configuration: {e}")
        return 1
    
    try:
        truncate_all_tables(config, args.skip_confirmation)
        return 0
    except KeyboardInterrupt:
        logger.info("\nOperation cancelled by user")
        return 1
    except Exception as e:
        logger.error(f"Failed to truncate tables: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())