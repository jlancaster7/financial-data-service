#!/usr/bin/env python3
"""
Script to check data in Snowflake tables
"""
import sys
from pathlib import Path
from datetime import datetime
from loguru import logger
import json

# Add the src directory to the path
sys.path.append(str(Path(__file__).parent.parent))

from src.utils.config import Config
from src.db.snowflake_connector import SnowflakeConnector


def check_table_data(snowflake: SnowflakeConnector, schema: str, table: str, sample_size: int = 5):
    """Check data in a specific table"""
    full_table_name = f"{schema}.{table}"
    
    # Get row count
    count_query = f"SELECT COUNT(*) as count FROM {full_table_name}"
    result = snowflake.fetch_all(count_query)
    row_count = result[0]['COUNT'] if result else 0
    
    logger.info(f"\n{'='*60}")
    logger.info(f"Table: {full_table_name}")
    logger.info(f"Row count: {row_count:,}")
    
    if row_count > 0:
        # Get sample data
        sample_query = f"SELECT * FROM {full_table_name} LIMIT {sample_size}"
        sample_data = snowflake.fetch_all(sample_query)
        
        if sample_data:
            logger.info(f"\nSample data (first {min(sample_size, row_count)} rows):")
            for i, row in enumerate(sample_data, 1):
                logger.info(f"\nRow {i}:")
                for key, value in row.items():
                    # Handle VARIANT columns
                    if isinstance(value, str) and (key == 'RAW_DATA' or 'raw_data' in key.lower()):
                        try:
                            # Try to parse as JSON for better display
                            parsed = json.loads(value)
                            logger.info(f"  {key}: {json.dumps(parsed, indent=2)[:200]}...")
                        except:
                            logger.info(f"  {key}: {str(value)[:200]}...")
                    else:
                        logger.info(f"  {key}: {value}")
        
        # Get column info
        column_query = f"""
        SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = '{schema}'
        AND TABLE_NAME = '{table}'
        ORDER BY ORDINAL_POSITION
        """
        columns = snowflake.fetch_all(column_query)
        
        logger.info(f"\nTable structure:")
        for col in columns:
            nullable = "NULL" if col['IS_NULLABLE'] == 'YES' else "NOT NULL"
            logger.info(f"  {col['COLUMN_NAME']}: {col['DATA_TYPE']} {nullable}")


def check_etl_monitoring(snowflake: SnowflakeConnector):
    """Check ETL monitoring tables"""
    logger.info("\n" + "="*60)
    logger.info("ETL MONITORING STATUS")
    logger.info("="*60)
    
    # Check job history
    job_query = """
    SELECT job_name, status, start_time, duration_seconds, 
           records_extracted, records_transformed, records_loaded
    FROM RAW_DATA.ETL_JOB_HISTORY
    ORDER BY start_time DESC
    LIMIT 10
    """
    
    jobs = snowflake.fetch_all(job_query)
    
    if jobs:
        logger.info(f"\nRecent ETL jobs:")
        for job in jobs:
            logger.info(f"\n  Job: {job['JOB_NAME']}")
            logger.info(f"  Status: {job['STATUS']}")
            logger.info(f"  Start: {job['START_TIME']}")
            logger.info(f"  Duration: {job['DURATION_SECONDS']:.2f}s" if job['DURATION_SECONDS'] else "  Duration: N/A")
            logger.info(f"  Records: {job['RECORDS_EXTRACTED']} extracted, "
                       f"{job['RECORDS_TRANSFORMED']} transformed, "
                       f"{job['RECORDS_LOADED']} loaded")
    else:
        logger.info("\nNo ETL jobs found in monitoring tables")
    
    # Check for errors
    error_query = """
    SELECT COUNT(*) as error_count
    FROM RAW_DATA.ETL_JOB_ERRORS
    WHERE error_timestamp >= DATEADD('day', -7, CURRENT_TIMESTAMP())
    """
    
    error_result = snowflake.fetch_all(error_query)
    error_count = error_result[0]['ERROR_COUNT'] if error_result else 0
    
    if error_count > 0:
        logger.warning(f"\n⚠️  {error_count} errors in the last 7 days")


def main():
    """Main execution function"""
    try:
        # Load configuration
        config = Config.load()
        logger.info("Configuration loaded")
        
        # Create Snowflake connection
        snowflake = SnowflakeConnector(config.snowflake)
        logger.info("Connected to Snowflake")
        
        # Check RAW layer tables
        logger.info("\n" + "="*60)
        logger.info("RAW DATA LAYER")
        logger.info("="*60)
        
        raw_tables = [
            'RAW_COMPANY_PROFILE',
            'RAW_HISTORICAL_PRICES', 
            'RAW_INCOME_STATEMENT',
            'RAW_BALANCE_SHEET',
            'RAW_CASH_FLOW'
        ]
        
        for table in raw_tables:
            if snowflake.table_exists(table, schema='RAW_DATA'):
                check_table_data(snowflake, 'RAW_DATA', table)
            else:
                logger.warning(f"Table RAW_DATA.{table} does not exist")
        
        # Check STAGING layer tables
        logger.info("\n" + "="*60)
        logger.info("STAGING DATA LAYER")
        logger.info("="*60)
        
        staging_tables = [
            'STG_COMPANY_PROFILE',
            'STG_HISTORICAL_PRICES',
            'STG_INCOME_STATEMENT', 
            'STG_BALANCE_SHEET',
            'STG_CASH_FLOW'
        ]
        
        for table in staging_tables:
            if snowflake.table_exists(table, schema='STAGING'):
                check_table_data(snowflake, 'STAGING', table)
            else:
                logger.warning(f"Table STAGING.{table} does not exist")
        
        # Check ANALYTICS layer
        logger.info("\n" + "="*60)
        logger.info("ANALYTICS LAYER")
        logger.info("="*60)
        
        analytics_tables = [
            'DIM_COMPANY',
            'DIM_DATE',
            'FACT_DAILY_PRICES',
            'FACT_FINANCIAL_METRICS'
        ]
        
        for table in analytics_tables:
            if snowflake.table_exists(table, schema='ANALYTICS'):
                check_table_data(snowflake, 'ANALYTICS', table, sample_size=3)
            else:
                logger.warning(f"Table ANALYTICS.{table} does not exist")
        
        # Check ETL monitoring
        check_etl_monitoring(snowflake)
        
        # Summary
        logger.info("\n" + "="*60)
        logger.info("SUMMARY")
        logger.info("="*60)
        
        summary_query = """
        SELECT 
            'RAW_DATA' as layer,
            COUNT(DISTINCT TABLE_NAME) as table_count,
            SUM(ROW_COUNT) as total_rows
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = 'RAW_DATA'
        AND TABLE_TYPE = 'BASE TABLE'
        
        UNION ALL
        
        SELECT 
            'STAGING' as layer,
            COUNT(DISTINCT TABLE_NAME) as table_count,
            SUM(ROW_COUNT) as total_rows
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = 'STAGING'
        AND TABLE_TYPE = 'BASE TABLE'
        
        UNION ALL
        
        SELECT 
            'ANALYTICS' as layer,
            COUNT(DISTINCT TABLE_NAME) as table_count,
            SUM(ROW_COUNT) as total_rows
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = 'ANALYTICS'
        AND TABLE_TYPE = 'BASE TABLE'
        
        ORDER BY layer
        """
        
        summary = snowflake.fetch_all(summary_query)
        
        logger.info("\nData by layer:")
        for row in summary:
            logger.info(f"  {row['LAYER']}: {row['TABLE_COUNT']} tables, {row['TOTAL_ROWS']:,} total rows")
        
        return True
        
    except Exception as e:
        logger.error(f"Failed to check Snowflake data: {e}")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)