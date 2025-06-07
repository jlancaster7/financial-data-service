#!/usr/bin/env python3
"""
Drop and recreate financial tables with new schema including filing dates
"""
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

from loguru import logger
from src.utils.config import Config
from src.db.snowflake_connector import SnowflakeConnector


def main():
    """Recreate financial tables"""
    logger.info("Recreating financial tables with new schema...")
    
    # Load configuration
    config = Config.load()
    
    # Connect to Snowflake
    connector = SnowflakeConnector(config.snowflake)
    
    try:
        with connector:
            # Drop existing financial tables
            logger.info("Dropping existing financial tables...")
            
            drop_statements = [
                # Drop fact tables first (due to foreign keys)
                "DROP TABLE IF EXISTS EQUITY_DATA.ANALYTICS.FACT_FINANCIAL_METRICS CASCADE",
                "DROP TABLE IF EXISTS EQUITY_DATA.ANALYTICS.FACT_FINANCIAL_RATIOS CASCADE",
                "DROP TABLE IF EXISTS EQUITY_DATA.ANALYTICS.FACT_FINANCIALS CASCADE",
                
                # Drop staging tables
                "DROP TABLE IF EXISTS EQUITY_DATA.STAGING.STG_INCOME_STATEMENT CASCADE",
                "DROP TABLE IF EXISTS EQUITY_DATA.STAGING.STG_BALANCE_SHEET CASCADE", 
                "DROP TABLE IF EXISTS EQUITY_DATA.STAGING.STG_CASH_FLOW CASCADE",
                
                # Drop raw tables
                "DROP TABLE IF EXISTS EQUITY_DATA.RAW_DATA.RAW_INCOME_STATEMENT CASCADE",
                "DROP TABLE IF EXISTS EQUITY_DATA.RAW_DATA.RAW_BALANCE_SHEET CASCADE",
                "DROP TABLE IF EXISTS EQUITY_DATA.RAW_DATA.RAW_CASH_FLOW CASCADE"
            ]
            
            for stmt in drop_statements:
                try:
                    connector.execute(stmt)
                    logger.info(f"✓ {stmt}")
                except Exception as e:
                    logger.warning(f"✗ {stmt} - {e}")
            
            # Re-run the table creation SQL
            logger.info("\nRecreating tables with new schema...")
            
            # Read SQL file
            sql_file = Path(__file__).parent.parent / "sql" / "03_table_definitions.sql"
            with open(sql_file, 'r') as f:
                sql_content = f.read()
            
            # Execute only the financial table creation statements
            # We need to be selective to avoid recreating all tables
            
            # Create RAW tables
            logger.info("Creating RAW financial tables...")
            connector.execute("USE SCHEMA EQUITY_DATA.RAW_DATA")
            
            raw_tables = [
                """CREATE TABLE IF NOT EXISTS RAW_INCOME_STATEMENT (
                    symbol VARCHAR(10),
                    fiscal_date DATE,
                    period VARCHAR(10),
                    raw_data VARIANT,
                    api_source VARCHAR(50),
                    loaded_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                    PRIMARY KEY (symbol, fiscal_date, period, loaded_timestamp)
                )""",
                """CREATE TABLE IF NOT EXISTS RAW_BALANCE_SHEET (
                    symbol VARCHAR(10),
                    fiscal_date DATE,
                    period VARCHAR(10),
                    raw_data VARIANT,
                    api_source VARCHAR(50),
                    loaded_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                    PRIMARY KEY (symbol, fiscal_date, period, loaded_timestamp)
                )""",
                """CREATE TABLE IF NOT EXISTS RAW_CASH_FLOW (
                    symbol VARCHAR(10),
                    fiscal_date DATE,
                    period VARCHAR(10),
                    raw_data VARIANT,
                    api_source VARCHAR(50),
                    loaded_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                    PRIMARY KEY (symbol, fiscal_date, period, loaded_timestamp)
                )"""
            ]
            
            for stmt in raw_tables:
                connector.execute(stmt)
                logger.info("✓ Created RAW table")
            
            # Create STAGING tables (with filing dates)
            logger.info("\nCreating STAGING financial tables...")
            connector.execute("USE SCHEMA EQUITY_DATA.STAGING")
            
            staging_tables = [
                """CREATE TABLE IF NOT EXISTS STG_INCOME_STATEMENT (
                    symbol VARCHAR(10),
                    fiscal_date DATE,
                    period VARCHAR(10),
                    filing_date DATE,
                    accepted_date TIMESTAMP_NTZ,
                    revenue NUMBER(20,2),
                    cost_of_revenue NUMBER(20,2),
                    gross_profit NUMBER(20,2),
                    operating_income NUMBER(20,2),
                    net_income NUMBER(20,2),
                    eps NUMBER(10,4),
                    eps_diluted NUMBER(10,4),
                    loaded_timestamp TIMESTAMP_NTZ,
                    PRIMARY KEY (symbol, fiscal_date, period)
                )""",
                """CREATE TABLE IF NOT EXISTS STG_BALANCE_SHEET (
                    symbol VARCHAR(10),
                    fiscal_date DATE,
                    period VARCHAR(10),
                    filing_date DATE,
                    accepted_date TIMESTAMP_NTZ,
                    total_assets NUMBER(20,2),
                    total_liabilities NUMBER(20,2),
                    total_equity NUMBER(20,2),
                    cash_and_equivalents NUMBER(20,2),
                    total_debt NUMBER(20,2),
                    net_debt NUMBER(20,2),
                    loaded_timestamp TIMESTAMP_NTZ,
                    PRIMARY KEY (symbol, fiscal_date, period)
                )""",
                """CREATE TABLE IF NOT EXISTS STG_CASH_FLOW (
                    symbol VARCHAR(10),
                    fiscal_date DATE,
                    period VARCHAR(10),
                    filing_date DATE,
                    accepted_date TIMESTAMP_NTZ,
                    operating_cash_flow NUMBER(20,2),
                    investing_cash_flow NUMBER(20,2),
                    financing_cash_flow NUMBER(20,2),
                    free_cash_flow NUMBER(20,2),
                    capital_expenditures NUMBER(20,2),
                    dividends_paid NUMBER(20,2),
                    loaded_timestamp TIMESTAMP_NTZ,
                    PRIMARY KEY (symbol, fiscal_date, period)
                )"""
            ]
            
            for stmt in staging_tables:
                connector.execute(stmt)
                logger.info("✓ Created STAGING table")
            
            # Create ANALYTICS fact tables
            logger.info("\nCreating ANALYTICS fact tables...")
            connector.execute("USE SCHEMA EQUITY_DATA.ANALYTICS")
            
            # Execute the fact table creation from the SQL file
            # Extract just the FACT_FINANCIALS and FACT_FINANCIAL_RATIOS creation
            start_idx = sql_content.find("-- Fact table for raw financial statement data")
            end_idx = sql_content.find("-- Grant table privileges")
            
            if start_idx != -1 and end_idx != -1:
                fact_tables_sql = sql_content[start_idx:end_idx]
                
                # Split and execute
                statements = [stmt.strip() for stmt in fact_tables_sql.split(';') if stmt.strip() and not stmt.strip().startswith('--')]
                
                for stmt in statements:
                    try:
                        connector.execute(stmt)
                        logger.info("✓ Created fact table or index")
                    except Exception as e:
                        logger.error(f"Failed to execute: {e}")
                        logger.error(f"Statement: {stmt[:100]}...")
                        raise
            
            # Grant permissions
            connector.execute("GRANT SELECT ON ALL TABLES IN SCHEMA RAW_DATA TO ROLE EQUITY_DATA_READER")
            connector.execute("GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA RAW_DATA TO ROLE EQUITY_DATA_LOADER")
            connector.execute("GRANT SELECT ON ALL TABLES IN SCHEMA STAGING TO ROLE EQUITY_DATA_READER")
            connector.execute("GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA STAGING TO ROLE EQUITY_DATA_LOADER")
            connector.execute("GRANT SELECT ON ALL TABLES IN SCHEMA ANALYTICS TO ROLE EQUITY_DATA_READER")
            connector.execute("GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA ANALYTICS TO ROLE EQUITY_DATA_LOADER")
            
            logger.info("\n✓ Financial tables recreated successfully!")
            
            # Verify new structure
            logger.info("\nVerifying new table structure...")
            
            # Check staging tables for filing dates
            for table in ['STG_INCOME_STATEMENT', 'STG_BALANCE_SHEET', 'STG_CASH_FLOW']:
                query = f"""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_schema = 'STAGING' 
                AND table_name = '{table}'
                AND column_name IN ('filing_date', 'accepted_date')
                ORDER BY ordinal_position
                """
                result = connector.fetch_all(query)
                if result:
                    logger.info(f"\n{table}:")
                    for row in result:
                        logger.info(f"  ✓ {row['COLUMN_NAME']}: {row['DATA_TYPE']}")
            
            # Check FACT_FINANCIALS structure
            query = """
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_schema = 'ANALYTICS' 
            AND table_name = 'FACT_FINANCIALS'
            AND column_name IN ('fiscal_date_key', 'filing_date_key', 'accepted_date')
            ORDER BY ordinal_position
            """
            result = connector.fetch_all(query)
            if result:
                logger.info("\nFACT_FINANCIALS:")
                for row in result:
                    logger.info(f"  ✓ {row['COLUMN_NAME']}: {row['DATA_TYPE']}")
                    
    except Exception as e:
        logger.error(f"Failed to recreate tables: {e}")
        return 1
    
    logger.info("\nNext steps:")
    logger.info("1. Update data models to include filing_date and accepted_date")
    logger.info("2. Update ETL pipelines to capture and store these dates")
    logger.info("3. Reload financial statement data with the new fields")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())