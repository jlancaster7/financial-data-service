#!/usr/bin/env python3
"""
Initial test script to verify basic Snowflake and FMP API connections
Uses default role to check if we need to run setup first
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from loguru import logger
from src.utils.config import Config
from src.db.snowflake_connector import SnowflakeConnector
from src.api.fmp_client import FMPClient
import snowflake.connector


def test_basic_snowflake_connection():
    """Test basic Snowflake connection with current user's default role"""
    logger.info("Testing basic Snowflake connection...")
    
    try:
        config = Config.load()
        
        # First try with default role
        logger.info("Attempting connection with default role...")
        basic_config = {
            "account": config.snowflake.account,
            "user": config.snowflake.user,
            "password": config.snowflake.password,
            "warehouse": config.snowflake.warehouse,
        }
        
        conn = snowflake.connector.connect(**basic_config)
        cursor = conn.cursor()
        
        # Get current context
        cursor.execute("SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_WAREHOUSE()")
        result = cursor.fetchone()
        logger.info(f"✓ Connected successfully!")
        logger.info(f"  - User: {result[0]}")
        logger.info(f"  - Role: {result[1]}")
        logger.info(f"  - Warehouse: {result[2]}")
        
        # Check available roles
        cursor.execute("SHOW ROLES")
        roles = cursor.fetchall()
        available_roles = [role[1] for role in roles]  # Role name is in second column
        logger.info(f"✓ Available roles: {available_roles}")
        
        # Check if EQUITY_DATA_LOADER role exists
        if 'EQUITY_DATA_LOADER' in available_roles:
            logger.info("✓ EQUITY_DATA_LOADER role exists")
        else:
            logger.warning("⚠ EQUITY_DATA_LOADER role does not exist - need to run setup")
        
        # Check databases
        cursor.execute("SHOW DATABASES")
        databases = cursor.fetchall()
        db_names = [db[1] for db in databases]  # Database name is in second column
        logger.info(f"✓ Available databases: {db_names}")
        
        if config.snowflake.database in db_names:
            logger.info(f"✓ Database {config.snowflake.database} exists")
        else:
            logger.warning(f"⚠ Database {config.snowflake.database} does not exist - need to run setup")
        
        cursor.close()
        conn.close()
        
        logger.success("Basic Snowflake connection test completed!")
        return True
        
    except Exception as e:
        logger.error(f"Snowflake connection test failed: {e}")
        return False


def test_fmp_connection():
    """Test FMP API connection"""
    logger.info("\nTesting FMP API connection...")
    
    try:
        config = Config.load()
        
        # Test direct API call to verify URL and key
        import requests
        
        # Try the correct FMP URL format
        test_url = "https://financialmodelingprep.com/api/v3/profile/AAPL"
        params = {"apikey": config.fmp.api_key}
        
        logger.info(f"Testing direct API call to {test_url}")
        response = requests.get(test_url, params=params)
        
        if response.status_code == 200:
            data = response.json()
            if data and len(data) > 0:
                logger.info(f"✓ Direct API call successful!")
                logger.info(f"  - Company: {data[0].get('companyName', 'N/A')}")
                logger.info(f"  - Symbol: {data[0].get('symbol', 'N/A')}")
                
                # Now test with our client
                with FMPClient(config.fmp) as client:
                    profile = client.get_company_profile("AAPL")
                    logger.info(f"✓ FMP Client test successful!")
                    
                return True
            else:
                logger.error("API returned empty data")
                return False
        else:
            logger.error(f"API returned status code: {response.status_code}")
            logger.error(f"Response: {response.text[:200]}")
            return False
            
    except Exception as e:
        logger.error(f"FMP API connection test failed: {e}")
        return False


def main():
    """Run initial connection tests"""
    logger.info("Starting initial connection tests...\n")
    
    snowflake_ok = test_basic_snowflake_connection()
    fmp_ok = test_fmp_connection()
    
    logger.info("\n" + "="*50)
    logger.info("Test Summary:")
    logger.info(f"  - Snowflake: {'✓ PASSED' if snowflake_ok else '✗ FAILED'}")
    logger.info(f"  - FMP API: {'✓ PASSED' if fmp_ok else '✗ FAILED'}")
    logger.info("="*50)
    
    if snowflake_ok:
        logger.info("\nNext steps:")
        logger.info("1. Run 'python scripts/setup_snowflake.py' to create database and roles")
        logger.info("2. Then run 'python scripts/test_connections.py' to verify full setup")
    

if __name__ == "__main__":
    main()