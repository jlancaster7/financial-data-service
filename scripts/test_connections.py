#!/usr/bin/env python3
"""
Test script to verify Snowflake and FMP API connections
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from loguru import logger
from src.utils.config import Config
from src.db.snowflake_connector import SnowflakeConnector
from src.api.fmp_client import FMPClient


def test_snowflake_connection():
    """Test Snowflake connection and basic operations"""
    logger.info("Testing Snowflake connection...")
    
    try:
        config = Config.load()
        
        with SnowflakeConnector(config.snowflake) as conn:
            # Test basic connection
            logger.info("✓ Connected to Snowflake successfully")
            
            # Test query execution
            result = conn.fetch_all("SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_DATABASE(), CURRENT_SCHEMA()")
            logger.info(f"✓ Current context: {result[0]}")
            
            # Test warehouse
            conn.execute(f"USE WAREHOUSE {config.snowflake.warehouse}")
            logger.info(f"✓ Using warehouse: {config.snowflake.warehouse}")
            
            # Check if database exists
            db_check = conn.fetch_all(
                "SELECT COUNT(*) as count FROM INFORMATION_SCHEMA.DATABASES WHERE DATABASE_NAME = %(db)s",
                {"db": config.snowflake.database}
            )
            
            if db_check[0]["COUNT"] > 0:
                logger.info(f"✓ Database {config.snowflake.database} exists")
                
                # Check schemas
                conn.execute(f"USE DATABASE {config.snowflake.database}")
                schemas = conn.fetch_all(
                    "SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE CATALOG_NAME = %(db)s",
                    {"db": config.snowflake.database}
                )
                logger.info(f"✓ Found schemas: {[s['SCHEMA_NAME'] for s in schemas]}")
                
                # Check if our tables exist
                for schema in ['RAW_DATA', 'STAGING', 'ANALYTICS']:
                    if conn.table_exists('DIM_DATE', schema='ANALYTICS'):
                        row_count = conn.get_table_row_count(f"{config.snowflake.database}.ANALYTICS.DIM_DATE")
                        logger.info(f"✓ DIM_DATE table has {row_count} rows")
                        break
            else:
                logger.warning(f"⚠ Database {config.snowflake.database} does not exist. Run SQL setup scripts first.")
                
        logger.success("Snowflake connection test completed successfully!")
        return True
        
    except Exception as e:
        logger.error(f"Snowflake connection test failed: {e}")
        return False


def test_fmp_connection():
    """Test FMP API connection and basic operations"""
    logger.info("\nTesting FMP API connection...")
    
    try:
        config = Config.load()
        
        with FMPClient(config.fmp) as client:
            # Test with a known symbol
            test_symbol = "AAPL"
            
            # Test company profile endpoint
            logger.info(f"Testing company profile for {test_symbol}...")
            profile = client.get_company_profile(test_symbol)
            logger.info(f"✓ Retrieved profile for {profile['companyName']}")
            logger.info(f"  - Sector: {profile.get('sector', 'N/A')}")
            logger.info(f"  - Industry: {profile.get('industry', 'N/A')}")
            logger.info(f"  - Market Cap: ${profile.get('mktCap', 0):,.0f}")
            
            # Test historical prices endpoint
            logger.info(f"\nTesting historical prices for {test_symbol}...")
            prices = client.get_historical_prices(test_symbol, from_date="2024-01-01", to_date="2024-01-31")
            logger.info(f"✓ Retrieved {len(prices)} days of price data")
            if prices:
                logger.info(f"  - Latest date: {prices[0]['date']}")
                logger.info(f"  - Latest close: ${prices[0]['close']}")
            
            # Test financial statements
            logger.info(f"\nTesting income statement for {test_symbol}...")
            income = client.get_income_statement(test_symbol, period='annual', limit=1)
            if income:
                logger.info(f"✓ Retrieved income statement for {income[0]['date']}")
                logger.info(f"  - Revenue: ${income[0].get('revenue', 0):,.0f}")
                logger.info(f"  - Net Income: ${income[0].get('netIncome', 0):,.0f}")
            
            # Test batch functionality
            logger.info("\nTesting batch company profiles...")
            symbols = ["MSFT", "GOOGL", "AMZN"]
            profiles = client.batch_get_company_profiles(symbols)
            logger.info(f"✓ Retrieved {len(profiles)} company profiles in batch")
            
        logger.success("FMP API connection test completed successfully!")
        return True
        
    except Exception as e:
        logger.error(f"FMP API connection test failed: {e}")
        return False


def main():
    """Run all connection tests"""
    logger.info("Starting connection tests...\n")
    
    snowflake_ok = test_snowflake_connection()
    fmp_ok = test_fmp_connection()
    
    logger.info("\n" + "="*50)
    logger.info("Test Summary:")
    logger.info(f"  - Snowflake: {'✓ PASSED' if snowflake_ok else '✗ FAILED'}")
    logger.info(f"  - FMP API: {'✓ PASSED' if fmp_ok else '✗ FAILED'}")
    logger.info("="*50)
    
    if not (snowflake_ok and fmp_ok):
        logger.error("\nSome tests failed. Please check your .env configuration.")
        sys.exit(1)
    else:
        logger.success("\nAll tests passed! Ready to proceed with ETL implementation.")


if __name__ == "__main__":
    main()