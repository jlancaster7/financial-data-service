#!/usr/bin/env python3
"""
Script to set up ETL monitoring tables in Snowflake
"""
import sys
from pathlib import Path
from loguru import logger

# Add the src directory to the path
sys.path.append(str(Path(__file__).parent.parent))

from src.utils.config import Config
from src.db.snowflake_connector import SnowflakeConnector


def setup_etl_monitoring_tables():
    """Execute the ETL monitoring tables SQL script"""
    try:
        # Load configuration
        config = Config.load()
        logger.info("Loaded configuration")

        # Create Snowflake connection
        snowflake = SnowflakeConnector(config.snowflake)
        logger.info("Connected to Snowflake")

        # Read SQL file
        sql_file = Path(__file__).parent.parent / "sql" / "05_etl_monitoring_tables.sql"
        with open(sql_file, "r") as f:
            sql_content = f.read()

        # Remove comments and split into individual statements
        # First, remove single-line comments
        lines = sql_content.split('\n')
        cleaned_lines = []
        for line in lines:
            # Remove comment part of the line
            comment_pos = line.find('--')
            if comment_pos >= 0:
                line = line[:comment_pos]
            cleaned_lines.append(line)
        
        cleaned_sql = '\n'.join(cleaned_lines)
        
        # Split by semicolon and filter out empty statements
        statements = [stmt.strip() for stmt in cleaned_sql.split(';') if stmt.strip()]
        
        # Group statements by type for proper execution order
        use_statements = []
        create_table_statements = []
        alter_table_statements = []
        create_view_statements = []
        grant_statements = []
        
        for statement in statements:
            upper_stmt = statement.upper()
            if upper_stmt.startswith('USE '):
                use_statements.append(statement)
            elif upper_stmt.startswith('CREATE TABLE') or upper_stmt.startswith('CREATE OR REPLACE TABLE'):
                create_table_statements.append(statement)
            elif upper_stmt.startswith('ALTER TABLE'):
                alter_table_statements.append(statement)
            elif upper_stmt.startswith('CREATE VIEW') or upper_stmt.startswith('CREATE OR REPLACE VIEW'):
                create_view_statements.append(statement)
            elif upper_stmt.startswith('GRANT'):
                grant_statements.append(statement)
        
        # Execute in proper order
        all_statements = (
            use_statements + 
            create_table_statements + 
            alter_table_statements + 
            create_view_statements + 
            grant_statements
        )
        
        logger.info(f"Executing {len(all_statements)} SQL statements")
        logger.info(f"  USE statements: {len(use_statements)}")
        logger.info(f"  CREATE TABLE statements: {len(create_table_statements)}")
        logger.info(f"  ALTER TABLE statements: {len(alter_table_statements)}")
        logger.info(f"  CREATE VIEW statements: {len(create_view_statements)}")
        logger.info(f"  GRANT statements: {len(grant_statements)}")
        
        # Execute each statement
        for i, statement in enumerate(all_statements, 1):
            try:
                logger.info(f"Executing statement {i}/{len(all_statements)}")
                logger.debug(f"Statement preview: {statement[:80]}...")
                snowflake.execute(statement)
            except Exception as e:
                logger.error(f"Failed to execute statement {i}: {e}")
                logger.debug(f"Full statement: {statement}")
                raise

        logger.success("ETL monitoring tables created successfully!")

        # Verify tables were created
        logger.info("Verifying table creation...")
        tables = [
            "ETL_JOB_HISTORY",
            "ETL_JOB_ERRORS",
            "ETL_JOB_METRICS",
            "ETL_DATA_QUALITY_ISSUES",
        ]

        for table in tables:
            if snowflake.table_exists(table):
                # Get row count
                count_query = f"SELECT COUNT(*) as count FROM {table}"
                result = snowflake.fetch_all(count_query)
                count = result[0]['COUNT'] if result else 0
                logger.info(f"✓ {table} exists (rows: {count})")
            else:
                logger.error(f"✗ {table} not found")

        # Check views
        logger.info("Checking views...")
        view_check = """
        SELECT COUNT(*) as count
        FROM INFORMATION_SCHEMA.VIEWS
        WHERE TABLE_SCHEMA = %(schema)s
        AND TABLE_NAME IN ('V_ETL_JOB_CURRENT_STATUS', 'V_ETL_RECENT_ERRORS')
        """
        result = snowflake.fetch_all(view_check, {"schema": config.snowflake.schema})
        if result and result[0]["COUNT"] == 2:
            logger.info("✓ All views created successfully")
        else:
            view_count = result[0]["COUNT"] if result else 0
            logger.warning(f"Expected 2 views, found {view_count}")

    except Exception as e:
        logger.error(f"Failed to set up ETL monitoring tables: {e}")
        return False

    return True


if __name__ == "__main__":
    success = setup_etl_monitoring_tables()
    sys.exit(0 if success else 1)
