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
);

CREATE TABLE IF NOT EXISTS STG_BALANCE_SHEET (
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
);

CREATE TABLE IF NOT EXISTS STG_CASH_FLOW (
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

-- Fact table for raw financial statement data
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
);

-- Fact table for calculated financial ratios
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
    -- Price-based ratios (to be calculated daily with price data)
    pe_ratio NUMBER(10,4),
    pb_ratio NUMBER(10,4),
    ps_ratio NUMBER(10,4),
    ev_to_ebitda NUMBER(10,4),
    -- Metadata
    created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    FOREIGN KEY (financial_key) REFERENCES FACT_FINANCIALS(financial_key),
    FOREIGN KEY (company_key) REFERENCES DIM_COMPANY(company_key),
    FOREIGN KEY (calculation_date_key) REFERENCES DIM_DATE(date_key)
);

-- Create indexes for performance
CREATE INDEX idx_fact_financials_company_date ON FACT_FINANCIALS(company_key, fiscal_date_key);
CREATE INDEX idx_fact_financials_accepted ON FACT_FINANCIALS(accepted_date);
CREATE INDEX idx_fact_ratios_company_date ON FACT_FINANCIAL_RATIOS(company_key, calculation_date_key);

-- Grant table privileges
GRANT SELECT ON ALL TABLES IN SCHEMA RAW_DATA TO ROLE EQUITY_DATA_READER;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA RAW_DATA TO ROLE EQUITY_DATA_LOADER;

GRANT SELECT ON ALL TABLES IN SCHEMA STAGING TO ROLE EQUITY_DATA_READER;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA STAGING TO ROLE EQUITY_DATA_LOADER;

GRANT SELECT ON ALL TABLES IN SCHEMA ANALYTICS TO ROLE EQUITY_DATA_READER;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA ANALYTICS TO ROLE EQUITY_DATA_LOADER;