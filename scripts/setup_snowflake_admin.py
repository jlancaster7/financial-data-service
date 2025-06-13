#!/usr/bin/env python3
"""
Setup script to initialize Snowflake database and tables using ACCOUNTADMIN role
"""
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pathlib import Path
from loguru import logger
import snowflake.connector
from src.utils.config import Config


def read_sql_file(filepath: Path) -> str:
    """Read SQL file content"""
    with open(filepath, "r") as f:
        return f.read()


def execute_sql_file(conn, filepath: Path, split_statements: bool = True):
    """Execute SQL file with raw connection"""
    logger.info(f"Executing {filepath.name}...")

    sql_content = read_sql_file(filepath)
    cursor = conn.cursor()

    try:
        if split_statements:
            # Split by semicolon and filter empty statements
            statements = [
                stmt.strip() for stmt in sql_content.split(";") if stmt.strip()
            ]

            for i, statement in enumerate(statements, 1):
                try:
                    cursor.execute(statement)
                    logger.debug(f"  Statement {i}/{len(statements)} executed")
                except Exception as e:
                    logger.error(f"  Statement {i} failed: {e}")
                    logger.debug(f"  Failed SQL: {statement[:100]}...")
                    # Continue with other statements for setup
        else:
            cursor.execute(sql_content)

        logger.success(f"✓ {filepath.name} executed successfully")
    finally:
        cursor.close()


def setup_snowflake():
    """Setup Snowflake database, schemas, and tables using ACCOUNTADMIN role"""
    logger.info("Starting Snowflake setup with ACCOUNTADMIN role...")

    try:
        config = Config.load()
        sql_dir = Path(__file__).parent.parent / "sql"

        # Connect with ACCOUNTADMIN role
        logger.info("Connecting to Snowflake as ACCOUNTADMIN...")
        conn = snowflake.connector.connect(
            account=config.snowflake.account,
            user=config.snowflake.user,
            password=config.snowflake.password,
            role="ACCOUNTADMIN",  # Use ACCOUNTADMIN for setup
            warehouse=config.snowflake.warehouse,
            disable_ocsp_checks=True,
        )

        logger.info("✓ Connected to Snowflake as ACCOUNTADMIN")

        # Execute setup scripts in order
        sql_files = [
            "01_database_setup.sql",
            "02_schema_setup.sql",
            "03_table_definitions.sql",
            "04_populate_date_dimension.sql",
        ]

        for sql_file in sql_files:
            filepath = sql_dir / sql_file
            if filepath.exists():
                execute_sql_file(conn, filepath)
            else:
                logger.warning(f"SQL file not found: {filepath}")

        # Grant the new role to current user
        cursor = conn.cursor()
        try:
            cursor.execute(
                f"GRANT ROLE EQUITY_DATA_LOADER TO USER {config.snowflake.user}"
            )
            cursor.execute(
                f"GRANT ROLE EQUITY_DATA_READER TO USER {config.snowflake.user}"
            )
            logger.info(f"✓ Granted roles to user {config.snowflake.user}")
        except Exception as e:
            logger.warning(f"Could not grant roles: {e}")
        finally:
            cursor.close()

        # Verify setup
        logger.info("\nVerifying setup...")
        cursor = conn.cursor()

        try:
            # Check database
            cursor.execute(f"USE DATABASE {config.snowflake.database}")
            logger.info(f"✓ Using database: {config.snowflake.database}")

            # Check schemas
            cursor.execute("SHOW SCHEMAS IN DATABASE")
            schemas = cursor.fetchall()
            schema_names = [s[1] for s in schemas]
            logger.info(f"✓ Found schemas: {schema_names}")

            # Check tables in each schema
            for schema in ["RAW_DATA", "STAGING", "ANALYTICS"]:
                if schema in schema_names:
                    cursor.execute(f"SHOW TABLES IN SCHEMA {schema}")
                    tables = cursor.fetchall()
                    table_names = [t[1] for t in tables]
                    logger.info(f"✓ Tables in {schema}: {table_names}")

            # Check DIM_DATE population
            cursor.execute(
                f"SELECT COUNT(*) FROM {config.snowflake.database}.ANALYTICS.DIM_DATE"
            )
            row_count = cursor.fetchone()[0]
            logger.info(f"✓ DIM_DATE populated with {row_count} rows")

        except Exception as e:
            logger.warning(f"Verification warning: {e}")
        finally:
            cursor.close()

        conn.close()
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
