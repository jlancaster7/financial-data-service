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
                "DROP TABLE IF EXISTS EQUITY_DATA.ANALYTICS.FACT_MARKET_METRICS CASCADE",
                "DROP TABLE IF EXISTS EQUITY_DATA.ANALYTICS.FACT_FINANCIAL_RATIOS CASCADE",
                "DROP TABLE IF EXISTS EQUITY_DATA.ANALYTICS.FACT_FINANCIALS_TTM CASCADE",
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
                    operating_expenses NUMBER(20,2),
                    operating_income NUMBER(20,2),
                    net_income NUMBER(20,2),
                    eps NUMBER(10,4),
                    eps_diluted NUMBER(10,4),
                    shares_outstanding NUMBER(20),
                    shares_outstanding_diluted NUMBER(20),
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
                    current_assets NUMBER(20,2),
                    total_liabilities NUMBER(20,2),
                    current_liabilities NUMBER(20,2),
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
            
            # Create FACT_FINANCIALS table
            fact_financials_sql = """
            CREATE TABLE IF NOT EXISTS FACT_FINANCIALS (
                financial_key NUMBER AUTOINCREMENT PRIMARY KEY,
                company_key NUMBER NOT NULL,
                fiscal_date_key NUMBER NOT NULL,
                filing_date_key NUMBER NOT NULL,
                accepted_date TIMESTAMP_NTZ NOT NULL,
                period_type VARCHAR(10) NOT NULL,
                -- Income Statement fields
                revenue NUMBER(20,2),
                cost_of_revenue NUMBER(20,2),
                gross_profit NUMBER(20,2),
                operating_expenses NUMBER(20,2),
                operating_income NUMBER(20,2),
                net_income NUMBER(20,2),
                eps NUMBER(10,4),
                eps_diluted NUMBER(10,4),
                shares_outstanding NUMBER(20),
                -- Balance Sheet fields
                total_assets NUMBER(20,2),
                current_assets NUMBER(20,2),
                total_liabilities NUMBER(20,2),
                current_liabilities NUMBER(20,2),
                total_equity NUMBER(20,2),
                cash_and_equivalents NUMBER(20,2),
                total_debt NUMBER(20,2),
                net_debt NUMBER(20,2),
                -- Cash Flow fields
                operating_cash_flow NUMBER(20,2),
                investing_cash_flow NUMBER(20,2),
                financing_cash_flow NUMBER(20,2),
                free_cash_flow NUMBER(20,2),
                capital_expenditures NUMBER(20,2),
                dividends_paid NUMBER(20,2),
                -- Metadata
                created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                FOREIGN KEY (company_key) REFERENCES DIM_COMPANY(company_key),
                FOREIGN KEY (fiscal_date_key) REFERENCES DIM_DATE(date_key),
                FOREIGN KEY (filing_date_key) REFERENCES DIM_DATE(date_key)
            )
            """
            connector.execute(fact_financials_sql)
            logger.info("✓ Created FACT_FINANCIALS table")
            
            # Create FACT_FINANCIAL_RATIOS table
            fact_ratios_sql = """
            CREATE TABLE IF NOT EXISTS FACT_FINANCIAL_RATIOS (
                ratio_key NUMBER AUTOINCREMENT PRIMARY KEY,
                financial_key NUMBER NOT NULL,
                company_key NUMBER NOT NULL,
                calculation_date_key NUMBER NOT NULL,
                -- Profitability Ratios
                gross_margin NUMBER(10,4),
                operating_margin NUMBER(10,4),
                profit_margin NUMBER(10,4),
                roe NUMBER(10,4),  -- Return on Equity
                roa NUMBER(10,4),  -- Return on Assets
                -- Liquidity Ratios
                current_ratio NUMBER(10,4),
                quick_ratio NUMBER(10,4),
                -- Leverage Ratios
                debt_to_equity NUMBER(10,4),
                debt_to_assets NUMBER(10,4),
                -- Efficiency Ratios
                asset_turnover NUMBER(10,4),
                -- Per Share Metrics
                book_value_per_share NUMBER(10,4),
                revenue_per_share NUMBER(10,4),
                -- Metadata
                created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                FOREIGN KEY (financial_key) REFERENCES FACT_FINANCIALS(financial_key),
                FOREIGN KEY (company_key) REFERENCES DIM_COMPANY(company_key),
                FOREIGN KEY (calculation_date_key) REFERENCES DIM_DATE(date_key)
            )
            """
            connector.execute(fact_ratios_sql)
            logger.info("✓ Created FACT_FINANCIAL_RATIOS table")
            
            # Create FACT_MARKET_METRICS table
            fact_market_metrics_sql = """
            CREATE TABLE IF NOT EXISTS FACT_MARKET_METRICS (
                market_metric_key NUMBER AUTOINCREMENT PRIMARY KEY,
                company_key NUMBER NOT NULL,
                date_key NUMBER NOT NULL,
                financial_key NUMBER NOT NULL,
                -- Price data (from FACT_DAILY_PRICES)
                close_price NUMBER(10,2),
                market_cap NUMBER(20,2),
                enterprise_value NUMBER(20,2),
                -- Market-based valuation ratios
                pe_ratio NUMBER(10,4),          -- Price to Earnings
                pe_ratio_ttm NUMBER(10,4),      -- Price to Earnings (Trailing Twelve Months)
                pb_ratio NUMBER(10,4),          -- Price to Book
                ps_ratio NUMBER(10,4),          -- Price to Sales
                ps_ratio_ttm NUMBER(10,4),      -- Price to Sales (TTM)
                peg_ratio NUMBER(10,4),         -- Price/Earnings to Growth
                ev_to_revenue NUMBER(10,4),     -- Enterprise Value to Revenue
                ev_to_revenue_ttm NUMBER(10,4), -- Enterprise Value to Revenue (TTM)
                ev_to_ebitda NUMBER(10,4),      -- Enterprise Value to EBITDA
                ev_to_ebit NUMBER(10,4),        -- Enterprise Value to EBIT
                -- Dividend metrics
                dividend_yield NUMBER(10,4),
                payout_ratio NUMBER(10,4),
                -- Metadata
                fiscal_period VARCHAR(10),       -- Q1, Q2, Q3, Q4, or ANNUAL
                is_ttm BOOLEAN DEFAULT FALSE,    -- Whether metrics use TTM calculations
                created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                FOREIGN KEY (company_key) REFERENCES DIM_COMPANY(company_key),
                FOREIGN KEY (date_key) REFERENCES DIM_DATE(date_key),
                FOREIGN KEY (financial_key) REFERENCES FACT_FINANCIALS(financial_key)
            )
            """
            connector.execute(fact_market_metrics_sql)
            logger.info("✓ Created FACT_MARKET_METRICS table")
            
            # Create FACT_FINANCIALS_TTM table
            fact_financials_ttm_sql = """
            CREATE TABLE IF NOT EXISTS FACT_FINANCIALS_TTM (
                ttm_key NUMBER AUTOINCREMENT PRIMARY KEY,
                company_key NUMBER NOT NULL,
                calculation_date DATE NOT NULL,
                accepted_date TIMESTAMP_NTZ NOT NULL,
                -- Quarters included in calculation
                quarters_included NUMBER NOT NULL,
                oldest_quarter_date DATE NOT NULL,
                newest_quarter_date DATE NOT NULL,
                -- TTM Flow Metrics (SUM of 4 quarters)
                ttm_revenue NUMBER(20,2),
                ttm_cost_of_revenue NUMBER(20,2),
                ttm_gross_profit NUMBER(20,2),
                ttm_operating_expenses NUMBER(20,2),
                ttm_operating_income NUMBER(20,2),
                ttm_net_income NUMBER(20,2),
                ttm_eps NUMBER(10,4),
                ttm_eps_diluted NUMBER(10,4),
                ttm_operating_cash_flow NUMBER(20,2),
                ttm_investing_cash_flow NUMBER(20,2),
                ttm_financing_cash_flow NUMBER(20,2),
                ttm_free_cash_flow NUMBER(20,2),
                ttm_capital_expenditures NUMBER(20,2),
                ttm_dividends_paid NUMBER(20,2),
                -- Point-in-time Stock Metrics (from most recent quarter)
                latest_shares_outstanding NUMBER(20),
                latest_total_assets NUMBER(20,2),
                latest_current_assets NUMBER(20,2),
                latest_total_liabilities NUMBER(20,2),
                latest_current_liabilities NUMBER(20,2),
                latest_total_equity NUMBER(20,2),
                latest_cash_and_equivalents NUMBER(20,2),
                latest_total_debt NUMBER(20,2),
                latest_net_debt NUMBER(20,2),
                -- Metadata
                created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                -- Constraints
                FOREIGN KEY (company_key) REFERENCES DIM_COMPANY(company_key),
                UNIQUE (company_key, calculation_date)
            )
            """
            connector.execute(fact_financials_ttm_sql)
            logger.info("✓ Created FACT_FINANCIALS_TTM table")
            
            # Create clustering keys for performance optimization
            clustering_keys = [
                "ALTER TABLE FACT_FINANCIALS CLUSTER BY (company_key, fiscal_date_key)",
                "ALTER TABLE FACT_FINANCIAL_RATIOS CLUSTER BY (company_key, calculation_date_key)",
                "ALTER TABLE FACT_MARKET_METRICS CLUSTER BY (company_key, date_key)",
                "ALTER TABLE FACT_FINANCIALS_TTM CLUSTER BY (company_key, calculation_date)"
            ]
            
            for cluster_sql in clustering_keys:
                try:
                    connector.execute(cluster_sql)
                    logger.info("✓ Added clustering key")
                except Exception as e:
                    logger.warning(f"Clustering key warning: {e}")
            
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
    logger.info("1. Implement financial ratio calculations for FACT_FINANCIAL_RATIOS")
    logger.info("2. Implement market metrics calculations for FACT_MARKET_METRICS")
    logger.info("3. Create ETL pipelines for both fact tables")
    logger.info("4. Test star schema query performance")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())