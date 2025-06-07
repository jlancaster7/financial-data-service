-- Snowflake Table Definitions for Financial Data Service
-- Story 1.1: Set up Snowflake Environment

USE DATABASE EQUITY_DATA;
USE SCHEMA RAW_DATA;

-- Raw data tables
CREATE TABLE IF NOT EXISTS RAW_COMPANY_PROFILE (
    symbol VARCHAR(10),
    raw_data VARIANT,
    api_source VARCHAR(50),
    loaded_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (symbol, loaded_timestamp)
);

CREATE TABLE IF NOT EXISTS RAW_HISTORICAL_PRICES (
    symbol VARCHAR(10),
    price_date DATE,
    raw_data VARIANT,
    api_source VARCHAR(50),
    loaded_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (symbol, price_date, loaded_timestamp)
);

CREATE TABLE IF NOT EXISTS RAW_INCOME_STATEMENT (
    symbol VARCHAR(10),
    fiscal_date DATE,
    period VARCHAR(10),
    raw_data VARIANT,
    api_source VARCHAR(50),
    loaded_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (symbol, fiscal_date, period, loaded_timestamp)
);

CREATE TABLE IF NOT EXISTS RAW_BALANCE_SHEET (
    symbol VARCHAR(10),
    fiscal_date DATE,
    period VARCHAR(10),
    raw_data VARIANT,
    api_source VARCHAR(50),
    loaded_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (symbol, fiscal_date, period, loaded_timestamp)
);

CREATE TABLE IF NOT EXISTS RAW_CASH_FLOW (
    symbol VARCHAR(10),
    fiscal_date DATE,
    period VARCHAR(10),
    raw_data VARIANT,
    api_source VARCHAR(50),
    loaded_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (symbol, fiscal_date, period, loaded_timestamp)
);

-- Staging tables
USE SCHEMA STAGING;

CREATE TABLE IF NOT EXISTS STG_COMPANY_PROFILE (
    symbol VARCHAR(10) PRIMARY KEY,
    company_name VARCHAR(255),
    sector VARCHAR(100),
    industry VARCHAR(100),
    exchange VARCHAR(50),
    market_cap NUMBER(20,2),
    description TEXT,
    website VARCHAR(255),
    ceo VARCHAR(255),
    employees NUMBER,
    headquarters_city VARCHAR(100),
    headquarters_state VARCHAR(50),
    headquarters_country VARCHAR(50),
    loaded_timestamp TIMESTAMP_NTZ
);

CREATE TABLE IF NOT EXISTS STG_HISTORICAL_PRICES (
    symbol VARCHAR(10),
    price_date DATE,
    open_price NUMBER(10,2),
    high_price NUMBER(10,2),
    low_price NUMBER(10,2),
    close_price NUMBER(10,2),
    adj_close NUMBER(10,2),
    volume NUMBER(20),
    change_percent NUMBER(10,4),
    loaded_timestamp TIMESTAMP_NTZ,
    PRIMARY KEY (symbol, price_date)
);

CREATE TABLE IF NOT EXISTS STG_INCOME_STATEMENT (
    symbol VARCHAR(10),
    fiscal_date DATE,
    period VARCHAR(10),
    revenue NUMBER(20,2),
    cost_of_revenue NUMBER(20,2),
    gross_profit NUMBER(20,2),
    operating_income NUMBER(20,2),
    net_income NUMBER(20,2),
    eps NUMBER(10,4),
    eps_diluted NUMBER(10,4),
    loaded_timestamp TIMESTAMP_NTZ,
    PRIMARY KEY (symbol, fiscal_date, period)
);

CREATE TABLE IF NOT EXISTS STG_BALANCE_SHEET (
    symbol VARCHAR(10),
    fiscal_date DATE,
    period VARCHAR(10),
    total_assets NUMBER(20,2),
    total_liabilities NUMBER(20,2),
    total_equity NUMBER(20,2),
    cash_and_equivalents NUMBER(20,2),
    total_debt NUMBER(20,2),
    net_debt NUMBER(20,2),
    loaded_timestamp TIMESTAMP_NTZ,
    PRIMARY KEY (symbol, fiscal_date, period)
);

CREATE TABLE IF NOT EXISTS STG_CASH_FLOW (
    symbol VARCHAR(10),
    fiscal_date DATE,
    period VARCHAR(10),
    operating_cash_flow NUMBER(20,2),
    investing_cash_flow NUMBER(20,2),
    financing_cash_flow NUMBER(20,2),
    free_cash_flow NUMBER(20,2),
    capital_expenditures NUMBER(20,2),
    dividends_paid NUMBER(20,2),
    loaded_timestamp TIMESTAMP_NTZ,
    PRIMARY KEY (symbol, fiscal_date, period)
);

-- Analytics dimension tables
USE SCHEMA ANALYTICS;

CREATE TABLE IF NOT EXISTS DIM_COMPANY (
    company_key NUMBER AUTOINCREMENT PRIMARY KEY,
    symbol VARCHAR(10) UNIQUE NOT NULL,
    company_name VARCHAR(255),
    sector VARCHAR(100),
    industry VARCHAR(100),
    exchange VARCHAR(50),
    market_cap_category VARCHAR(50),
    headquarters_location VARCHAR(255),
    is_current BOOLEAN DEFAULT TRUE,
    valid_from TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    valid_to TIMESTAMP_NTZ DEFAULT '9999-12-31'::TIMESTAMP_NTZ
);

CREATE TABLE IF NOT EXISTS DIM_DATE (
    date_key NUMBER PRIMARY KEY,
    date DATE UNIQUE NOT NULL,
    year NUMBER,
    quarter NUMBER,
    month NUMBER,
    day NUMBER,
    day_of_week NUMBER,
    day_name VARCHAR(20),
    month_name VARCHAR(20),
    is_weekend BOOLEAN,
    is_month_end BOOLEAN,
    is_quarter_end BOOLEAN,
    is_year_end BOOLEAN
);

-- Analytics fact tables
CREATE TABLE IF NOT EXISTS FACT_DAILY_PRICES (
    price_key NUMBER AUTOINCREMENT PRIMARY KEY,
    company_key NUMBER,
    date_key NUMBER,
    open_price NUMBER(10,2),
    high_price NUMBER(10,2),
    low_price NUMBER(10,2),
    close_price NUMBER(10,2),
    adj_close NUMBER(10,2),
    volume NUMBER(20),
    change_amount NUMBER(10,2),
    change_percent NUMBER(10,4),
    FOREIGN KEY (company_key) REFERENCES DIM_COMPANY(company_key),
    FOREIGN KEY (date_key) REFERENCES DIM_DATE(date_key)
);

CREATE TABLE IF NOT EXISTS FACT_FINANCIAL_METRICS (
    metric_key NUMBER AUTOINCREMENT PRIMARY KEY,
    company_key NUMBER,
    date_key NUMBER,
    period_type VARCHAR(10),
    revenue NUMBER(20,2),
    gross_profit NUMBER(20,2),
    operating_income NUMBER(20,2),
    net_income NUMBER(20,2),
    eps NUMBER(10,4),
    total_assets NUMBER(20,2),
    total_equity NUMBER(20,2),
    total_debt NUMBER(20,2),
    operating_cash_flow NUMBER(20,2),
    free_cash_flow NUMBER(20,2),
    profit_margin NUMBER(10,4),
    roe NUMBER(10,4),
    roa NUMBER(10,4),
    debt_to_equity NUMBER(10,4),
    FOREIGN KEY (company_key) REFERENCES DIM_COMPANY(company_key),
    FOREIGN KEY (date_key) REFERENCES DIM_DATE(date_key)
);

-- Grant table privileges
GRANT SELECT ON ALL TABLES IN SCHEMA RAW_DATA TO ROLE EQUITY_DATA_READER;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA RAW_DATA TO ROLE EQUITY_DATA_LOADER;

GRANT SELECT ON ALL TABLES IN SCHEMA STAGING TO ROLE EQUITY_DATA_READER;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA STAGING TO ROLE EQUITY_DATA_LOADER;

GRANT SELECT ON ALL TABLES IN SCHEMA ANALYTICS TO ROLE EQUITY_DATA_READER;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA ANALYTICS TO ROLE EQUITY_DATA_LOADER;