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
    operating_expenses NUMBER(20,2),
    operating_income NUMBER(20,2),
    net_income NUMBER(20,2),
    eps NUMBER(10,4),
    eps_diluted NUMBER(10,4),
    shares_outstanding NUMBER(20),
    shares_outstanding_diluted NUMBER(20),
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
    current_assets NUMBER(20,2),
    total_liabilities NUMBER(20,2),
    current_liabilities NUMBER(20,2),
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
    revenue_per_share NUMBER(10,4),
    -- Metadata
    created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    FOREIGN KEY (financial_key) REFERENCES FACT_FINANCIALS(financial_key),
    FOREIGN KEY (company_key) REFERENCES DIM_COMPANY(company_key),
    FOREIGN KEY (calculation_date_key) REFERENCES DIM_DATE(date_key)
);

-- Fact table for daily market metrics (combines price and financial data)
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
    created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    FOREIGN KEY (company_key) REFERENCES DIM_COMPANY(company_key),
    FOREIGN KEY (date_key) REFERENCES DIM_DATE(date_key),
    FOREIGN KEY (financial_key) REFERENCES FACT_FINANCIALS(financial_key)
);

-- Fact table for trailing twelve month (TTM) financial calculations
CREATE TABLE IF NOT EXISTS FACT_FINANCIALS_TTM (
    ttm_key NUMBER AUTOINCREMENT PRIMARY KEY,
    company_key NUMBER NOT NULL,
    calculation_date DATE NOT NULL,
    accepted_date TIMESTAMP_NTZ NOT NULL,  -- When the TTM data became available
    
    -- Quarters included in calculation
    quarters_included NUMBER NOT NULL,  -- Should be 4 for complete TTM
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
    UNIQUE (company_key, calculation_date)  -- One TTM calculation per company per date
);

-- Add clustering keys for performance optimization in Snowflake
ALTER TABLE FACT_FINANCIALS CLUSTER BY (company_key, fiscal_date_key);
ALTER TABLE FACT_FINANCIAL_RATIOS CLUSTER BY (company_key, calculation_date_key);
ALTER TABLE FACT_MARKET_METRICS CLUSTER BY (company_key, date_key);
ALTER TABLE FACT_FINANCIALS_TTM CLUSTER BY (company_key, calculation_date);

-- Grant table privileges
GRANT SELECT ON ALL TABLES IN SCHEMA RAW_DATA TO ROLE EQUITY_DATA_READER;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA RAW_DATA TO ROLE EQUITY_DATA_LOADER;

GRANT SELECT ON ALL TABLES IN SCHEMA STAGING TO ROLE EQUITY_DATA_READER;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA STAGING TO ROLE EQUITY_DATA_LOADER;

GRANT SELECT ON ALL TABLES IN SCHEMA ANALYTICS TO ROLE EQUITY_DATA_READER;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA ANALYTICS TO ROLE EQUITY_DATA_LOADER;