#!/usr/bin/env python3
"""
Setup script to initialize Snowflake database and tables
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pathlib import Path
from loguru import logger
from src.utils.config import Config
from src.db.snowflake_connector import SnowflakeConnector


def read_sql_file(filepath: Path) -> str:
    """Read SQL file content"""
    with open(filepath, 'r') as f:
        return f.read()


def execute_sql_file(conn: SnowflakeConnector, filepath: Path, split_statements: bool = True):
    """Execute SQL file"""
    logger.info(f"Executing {filepath.name}...")
    
    sql_content = read_sql_file(filepath)
    
    if split_statements:
        # Split by semicolon and filter empty statements
        statements = [stmt.strip() for stmt in sql_content.split(';') if stmt.strip()]
        
        for i, statement in enumerate(statements, 1):
            try:
                conn.execute(statement)
                logger.debug(f"  Statement {i}/{len(statements)} executed")
            except Exception as e:
                logger.error(f"  Statement {i} failed: {e}")
                logger.debug(f"  Failed SQL: {statement[:100]}...")
                raise
    else:
        conn.execute(sql_content)
    
    logger.success(f"✓ {filepath.name} executed successfully")


def setup_snowflake():
    """Setup Snowflake database, schemas, and tables"""
    logger.info("Starting Snowflake setup...")
    
    try:
        config = Config.load()
        sql_dir = Path(__file__).parent.parent / "sql"
        
        with SnowflakeConnector(config.snowflake) as conn:
            # Execute setup scripts in order
            sql_files = [
                "01_database_setup.sql",
                "02_schema_setup.sql",
                "03_table_definitions.sql",
                "04_populate_date_dimension.sql"
            ]
            
            for sql_file in sql_files:
                filepath = sql_dir / sql_file
                if filepath.exists():
                    execute_sql_file(conn, filepath)
                else:
                    logger.warning(f"SQL file not found: {filepath}")
            
            # Verify setup
            logger.info("\nVerifying setup...")
            
            # Check database
            conn.execute(f"USE DATABASE {config.snowflake.database}")
            logger.info(f"✓ Using database: {config.snowflake.database}")
            
            # Check schemas
            schemas = conn.fetch_all(
                "SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE CATALOG_NAME = CURRENT_DATABASE()"
            )
            schema_names = [s['SCHEMA_NAME'] for s in schemas]
            logger.info(f"✓ Found schemas: {schema_names}")
            
            # Check tables in each schema
            for schema in ['RAW_DATA', 'STAGING', 'ANALYTICS']:
                if schema in schema_names:
                    tables = conn.fetch_all(
                        "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = %(schema)s AND TABLE_CATALOG = CURRENT_DATABASE()",
                        {"schema": schema}
                    )
                    table_names = [t['TABLE_NAME'] for t in tables]
                    logger.info(f"✓ Tables in {schema}: {table_names}")
            
            # Check DIM_DATE population
            if conn.table_exists('DIM_DATE', schema='ANALYTICS'):
                row_count = conn.get_table_row_count(f"{config.snowflake.database}.ANALYTICS.DIM_DATE")
                logger.info(f"✓ DIM_DATE populated with {row_count} rows")
            
        logger.success("\nSnowflake setup completed successfully!")
        return True
        
    except Exception as e:
        logger.error(f"Snowflake setup failed: {e}")
        return False


def main():
    """Run setup"""
    if setup_snowflake():
        logger.info("\nYou can now run: python scripts/test_connections.py")
    else:
        logger.error("\nSetup failed. Please check your configuration and try again.")
        sys.exit(1)


if __name__ == "__main__":
    main()