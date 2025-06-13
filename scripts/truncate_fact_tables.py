#!/usr/bin/env python3
"""
Truncate only fact tables in the data warehouse
This preserves dimension tables (DIM_COMPANY, DIM_DATE)
"""
import sys
from pathlib import Path
from loguru import logger

# Add the src directory to the path
sys.path.append(str(Path(__file__).parent.parent))

from src.utils.config import Config
from src.db.snowflake_connector import SnowflakeConnector


def truncate_fact_tables(config: Config, skip_confirmation: bool = False):
    """
    Truncate only fact tables, preserving dimension tables
    
    Args:
        config: Configuration object
        skip_confirmation: Skip confirmation prompt
    """
    # Define only fact tables to truncate
    tables_to_truncate = [
        # Analytics fact tables
        "ANALYTICS.FACT_MARKET_METRICS",
        "ANALYTICS.FACT_FINANCIAL_RATIOS", 
        "ANALYTICS.FACT_FINANCIALS_TTM",
        "ANALYTICS.FACT_FINANCIALS",
        "ANALYTICS.FACT_DAILY_PRICES",
        
        # Staging layer (transient data)
        "STAGING.STG_CASH_FLOW",
        "STAGING.STG_BALANCE_SHEET",
        "STAGING.STG_INCOME_STATEMENT",
        "STAGING.STG_HISTORICAL_PRICES",
        "STAGING.STG_COMPANY_PROFILE",
        
        # Raw layer (can be reloaded)
        "RAW_DATA.RAW_CASH_FLOW",
        "RAW_DATA.RAW_BALANCE_SHEET",
        "RAW_DATA.RAW_INCOME_STATEMENT",
        "RAW_DATA.RAW_HISTORICAL_PRICES",
        "RAW_DATA.RAW_COMPANY_PROFILE",
    ]
    
    # Tables we're preserving
    preserved_tables = [
        "ANALYTICS.DIM_COMPANY",
        "ANALYTICS.DIM_DATE",
        "RAW_DATA.ETL_JOB_HISTORY",
        "RAW_DATA.ETL_JOB_ERRORS",
        "RAW_DATA.ETL_JOB_METRICS",
        "RAW_DATA.ETL_DATA_QUALITY_ISSUES"
    ]
    
    # Show what will be truncated
    logger.info("=" * 60)
    logger.info("This will DELETE DATA from the following tables:")
    logger.info("=" * 60)
    for table in tables_to_truncate:
        logger.info(f"  - {table}")
    
    logger.info("\nThe following tables will be PRESERVED:")
    for table in preserved_tables:
        logger.info(f"  + {table}")
    logger.info("=" * 60)
    
    if not skip_confirmation:
        response = input("\nAre you sure you want to delete fact table data? Type 'YES' to confirm: ")
        if response.upper() != "YES":
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
        
        # Show preserved table counts
        logger.info("\nPreserved table counts:")
        for table in preserved_tables:
            try:
                result = conn.fetch_one(f"SELECT COUNT(*) as count FROM {table}")
                count = result['COUNT'] if result else 0
                if count > 0:
                    logger.info(f"  {table}: {count:,} rows (keeping)")
            except Exception:
                pass
        
        if total_rows == 0:
            logger.info("\nNo data to delete - all fact tables are already empty")
            return
        
        # Truncate tables
        logger.info("\nTruncating fact tables...")
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
        
        # Summary
        logger.info("\n" + "=" * 60)
        logger.info("TRUNCATION COMPLETE")
        logger.info("=" * 60)
        logger.info(f"Tables truncated successfully: {success_count}")
        logger.info(f"Tables with errors: {error_count}")
        logger.info(f"Total rows deleted: ~{total_rows:,}")
        logger.info("Dimension tables preserved: DIM_COMPANY, DIM_DATE")
        logger.info("=" * 60)


def main():
    """Main execution function"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Truncate fact tables while preserving dimension tables",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
This script truncates fact tables while preserving dimension tables.

Examples:
  # Truncate fact tables (with confirmation)
  python truncate_fact_tables.py
  
  # Truncate fact tables without confirmation
  python truncate_fact_tables.py --skip-confirmation
        """
    )
    
    parser.add_argument(
        "--skip-confirmation",
        action="store_true",
        help="Skip confirmation prompt"
    )
    
    args = parser.parse_args()
    
    # Load configuration
    try:
        config = Config.load()
    except Exception as e:
        logger.error(f"Failed to load configuration: {e}")
        return 1
    
    try:
        truncate_fact_tables(config, args.skip_confirmation)
        return 0
    except KeyboardInterrupt:
        logger.info("\nOperation cancelled by user")
        return 1
    except Exception as e:
        logger.error(f"Failed to truncate tables: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())