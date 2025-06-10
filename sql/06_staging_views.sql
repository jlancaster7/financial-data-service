-- Staging Layer Views
-- These views provide convenient access to staging data for analysis and monitoring

USE DATABASE EQUITY_DATA;
USE SCHEMA STAGING;

-- =====================================================
-- DATA QUALITY MONITORING VIEWS
-- =====================================================

-- View to monitor data freshness across all staging tables
CREATE OR REPLACE VIEW V_STAGING_DATA_FRESHNESS AS
SELECT 
    'STG_COMPANY_PROFILE' as table_name,
    COUNT(*) as record_count,
    MAX(loaded_timestamp) as last_load_time,
    DATEDIFF('hour', MAX(loaded_timestamp), CURRENT_TIMESTAMP()) as hours_since_load
FROM STG_COMPANY_PROFILE
UNION ALL
SELECT 
    'STG_HISTORICAL_PRICES' as table_name,
    COUNT(*) as record_count,
    MAX(loaded_timestamp) as last_load_time,
    DATEDIFF('hour', MAX(loaded_timestamp), CURRENT_TIMESTAMP()) as hours_since_load
FROM STG_HISTORICAL_PRICES
UNION ALL
SELECT 
    'STG_INCOME_STATEMENT' as table_name,
    COUNT(*) as record_count,
    MAX(loaded_timestamp) as last_load_time,
    DATEDIFF('hour', MAX(loaded_timestamp), CURRENT_TIMESTAMP()) as hours_since_load
FROM STG_INCOME_STATEMENT
UNION ALL
SELECT 
    'STG_BALANCE_SHEET' as table_name,
    COUNT(*) as record_count,
    MAX(loaded_timestamp) as last_load_time,
    DATEDIFF('hour', MAX(loaded_timestamp), CURRENT_TIMESTAMP()) as hours_since_load
FROM STG_BALANCE_SHEET
UNION ALL
SELECT 
    'STG_CASH_FLOW' as table_name,
    COUNT(*) as record_count,
    MAX(loaded_timestamp) as last_load_time,
    DATEDIFF('hour', MAX(loaded_timestamp), CURRENT_TIMESTAMP()) as hours_since_load
FROM STG_CASH_FLOW;

-- View to detect missing financial statement components
CREATE OR REPLACE VIEW V_MISSING_FINANCIAL_DATA AS
SELECT 
    i.symbol,
    i.fiscal_date,
    i.period,
    CASE WHEN b.symbol IS NULL THEN 'Missing Balance Sheet' END as balance_sheet_status,
    CASE WHEN c.symbol IS NULL THEN 'Missing Cash Flow' END as cash_flow_status,
    CASE 
        WHEN b.symbol IS NULL AND c.symbol IS NULL THEN 'Missing Both'
        WHEN b.symbol IS NULL THEN 'Missing Balance Sheet Only'
        WHEN c.symbol IS NULL THEN 'Missing Cash Flow Only'
        ELSE 'Complete'
    END as data_completeness
FROM STG_INCOME_STATEMENT i
LEFT JOIN STG_BALANCE_SHEET b 
    ON i.symbol = b.symbol 
    AND i.fiscal_date = b.fiscal_date 
    AND i.period = b.period
LEFT JOIN STG_CASH_FLOW c 
    ON i.symbol = c.symbol 
    AND i.fiscal_date = c.fiscal_date 
    AND i.period = c.period
WHERE b.symbol IS NULL OR c.symbol IS NULL
ORDER BY i.symbol, i.fiscal_date DESC;

-- View to monitor price data gaps
CREATE OR REPLACE VIEW V_PRICE_DATA_GAPS AS
WITH daily_prices AS (
    SELECT 
        symbol,
        price_date,
        LAG(price_date) OVER (PARTITION BY symbol ORDER BY price_date) as prev_date,
        DATEDIFF('day', LAG(price_date) OVER (PARTITION BY symbol ORDER BY price_date), price_date) as days_gap
    FROM STG_HISTORICAL_PRICES
)
SELECT 
    symbol,
    prev_date as gap_start,
    price_date as gap_end,
    days_gap,
    CASE 
        WHEN days_gap > 5 THEN 'Significant Gap'
        WHEN days_gap > 3 THEN 'Weekend/Holiday'
        ELSE 'Normal'
    END as gap_type
FROM daily_prices
WHERE days_gap > 3  -- Only show gaps larger than weekends
ORDER BY symbol, price_date;

-- =====================================================
-- LATEST DATA VIEWS
-- =====================================================

-- View for latest company information
CREATE OR REPLACE VIEW V_LATEST_COMPANY_INFO AS
SELECT *
FROM STG_COMPANY_PROFILE
QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY loaded_timestamp DESC) = 1;

-- View for latest financial statements by company
CREATE OR REPLACE VIEW V_LATEST_FINANCIALS AS
WITH latest_income AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY fiscal_date DESC, 
                             CASE period WHEN 'FY' THEN 1 WHEN 'Q4' THEN 2 WHEN 'Q3' THEN 3 WHEN 'Q2' THEN 4 WHEN 'Q1' THEN 5 END) as rn
    FROM STG_INCOME_STATEMENT
),
latest_balance AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY fiscal_date DESC,
                             CASE period WHEN 'FY' THEN 1 WHEN 'Q4' THEN 2 WHEN 'Q3' THEN 3 WHEN 'Q2' THEN 4 WHEN 'Q1' THEN 5 END) as rn
    FROM STG_BALANCE_SHEET
),
latest_cashflow AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY fiscal_date DESC,
                             CASE period WHEN 'FY' THEN 1 WHEN 'Q4' THEN 2 WHEN 'Q3' THEN 3 WHEN 'Q2' THEN 4 WHEN 'Q1' THEN 5 END) as rn
    FROM STG_CASH_FLOW
)
SELECT 
    COALESCE(i.symbol, b.symbol, c.symbol) as symbol,
    COALESCE(i.fiscal_date, b.fiscal_date, c.fiscal_date) as fiscal_date,
    COALESCE(i.period, b.period, c.period) as period,
    -- Income Statement metrics
    i.revenue,
    i.gross_profit,
    i.operating_income,
    i.net_income,
    i.eps_diluted,
    i.shares_outstanding,
    -- Balance Sheet metrics
    b.total_assets,
    b.total_liabilities,
    b.total_equity,
    b.cash_and_equivalents,
    b.total_debt,
    -- Cash Flow metrics
    c.operating_cash_flow,
    c.free_cash_flow,
    c.dividends_paid
FROM latest_income i
FULL OUTER JOIN latest_balance b 
    ON i.symbol = b.symbol 
    AND i.fiscal_date = b.fiscal_date 
    AND i.period = b.period
    AND b.rn = 1
FULL OUTER JOIN latest_cashflow c 
    ON COALESCE(i.symbol, b.symbol) = c.symbol 
    AND COALESCE(i.fiscal_date, b.fiscal_date) = c.fiscal_date 
    AND COALESCE(i.period, b.period) = c.period
    AND c.rn = 1
WHERE i.rn = 1 OR b.rn = 1 OR c.rn = 1;

-- View for latest quarterly financials only
CREATE OR REPLACE VIEW V_LATEST_QUARTERLY_FINANCIALS AS
SELECT * 
FROM V_LATEST_FINANCIALS
WHERE period IN ('Q1', 'Q2', 'Q3', 'Q4');

-- View for latest annual financials only
CREATE OR REPLACE VIEW V_LATEST_ANNUAL_FINANCIALS AS
SELECT * 
FROM V_LATEST_FINANCIALS
WHERE period = 'FY';

-- =====================================================
-- FINANCIAL METRICS VIEWS
-- =====================================================

-- View for basic financial ratios from staging data
CREATE OR REPLACE VIEW V_STAGING_FINANCIAL_RATIOS AS
SELECT 
    i.symbol,
    i.fiscal_date,
    i.period,
    -- Profitability metrics
    CASE WHEN i.revenue > 0 THEN (i.gross_profit / i.revenue) * 100 END as gross_margin_pct,
    CASE WHEN i.revenue > 0 THEN (i.operating_income / i.revenue) * 100 END as operating_margin_pct,
    CASE WHEN i.revenue > 0 THEN (i.net_income / i.revenue) * 100 END as net_margin_pct,
    -- Leverage metrics
    CASE WHEN b.total_equity > 0 THEN b.total_debt / b.total_equity END as debt_to_equity,
    CASE WHEN b.total_assets > 0 THEN b.total_debt / b.total_assets END as debt_to_assets,
    -- Liquidity metrics
    CASE WHEN b.current_liabilities > 0 THEN b.current_assets / b.current_liabilities END as current_ratio,
    -- Per share metrics
    CASE WHEN i.shares_outstanding > 0 THEN i.revenue / i.shares_outstanding END as revenue_per_share,
    CASE WHEN i.shares_outstanding > 0 THEN b.total_equity / i.shares_outstanding END as book_value_per_share
FROM STG_INCOME_STATEMENT i
JOIN STG_BALANCE_SHEET b 
    ON i.symbol = b.symbol 
    AND i.fiscal_date = b.fiscal_date 
    AND i.period = b.period;

-- View for quarter-over-quarter growth metrics
CREATE OR REPLACE VIEW V_QUARTERLY_GROWTH_METRICS AS
WITH quarterly_data AS (
    SELECT 
        symbol,
        fiscal_date,
        period,
        revenue,
        net_income,
        LAG(revenue) OVER (PARTITION BY symbol ORDER BY fiscal_date) as prev_revenue,
        LAG(net_income) OVER (PARTITION BY symbol ORDER BY fiscal_date) as prev_net_income,
        LAG(fiscal_date) OVER (PARTITION BY symbol ORDER BY fiscal_date) as prev_fiscal_date
    FROM STG_INCOME_STATEMENT
    WHERE period IN ('Q1', 'Q2', 'Q3', 'Q4')
)
SELECT 
    symbol,
    fiscal_date,
    period,
    revenue,
    prev_revenue,
    CASE 
        WHEN prev_revenue > 0 
        THEN ((revenue - prev_revenue) / prev_revenue) * 100 
    END as revenue_growth_pct,
    net_income,
    prev_net_income,
    CASE 
        WHEN prev_net_income > 0 
        THEN ((net_income - prev_net_income) / prev_net_income) * 100 
    END as net_income_growth_pct,
    DATEDIFF('day', prev_fiscal_date, fiscal_date) as days_between_quarters
FROM quarterly_data
WHERE prev_fiscal_date IS NOT NULL
ORDER BY symbol, fiscal_date DESC;

-- =====================================================
-- DATA VALIDATION VIEWS
-- =====================================================

-- View to check for data quality issues
CREATE OR REPLACE VIEW V_DATA_QUALITY_ISSUES AS
-- Check for negative revenue
SELECT 
    'Negative Revenue' as issue_type,
    'STG_INCOME_STATEMENT' as table_name,
    symbol,
    fiscal_date::varchar as reference_date,
    period,
    'Revenue: ' || revenue as issue_detail
FROM STG_INCOME_STATEMENT
WHERE revenue < 0
UNION ALL
-- Check for future filing dates
SELECT 
    'Future Filing Date' as issue_type,
    'STG_INCOME_STATEMENT' as table_name,
    symbol,
    fiscal_date::varchar as reference_date,
    period,
    'Filing Date: ' || filing_date as issue_detail
FROM STG_INCOME_STATEMENT
WHERE filing_date > CURRENT_DATE()
UNION ALL
-- Check for zero total assets
SELECT 
    'Zero Total Assets' as issue_type,
    'STG_BALANCE_SHEET' as table_name,
    symbol,
    fiscal_date::varchar as reference_date,
    period,
    'Total Assets: 0' as issue_detail
FROM STG_BALANCE_SHEET
WHERE total_assets = 0 OR total_assets IS NULL
UNION ALL
-- Check for price outliers
SELECT 
    'Price Outlier' as issue_type,
    'STG_HISTORICAL_PRICES' as table_name,
    symbol,
    price_date::varchar as reference_date,
    'DAILY' as period,
    'Close Price: ' || close_price as issue_detail
FROM STG_HISTORICAL_PRICES
WHERE close_price > 100000 OR close_price <= 0
ORDER BY issue_type, table_name, symbol, reference_date DESC;

-- =====================================================
-- SUMMARY STATISTICS VIEW
-- =====================================================

-- View for staging layer summary statistics
CREATE OR REPLACE VIEW V_STAGING_SUMMARY_STATS AS
SELECT 
    'Company Coverage' as metric_name,
    COUNT(DISTINCT symbol) as metric_value,
    'companies' as metric_unit
FROM STG_COMPANY_PROFILE
UNION ALL
SELECT 
    'Price History Days' as metric_name,
    COUNT(DISTINCT price_date) as metric_value,
    'days' as metric_unit
FROM STG_HISTORICAL_PRICES
UNION ALL
SELECT 
    'Financial Statements' as metric_name,
    COUNT(*) as metric_value,
    'records' as metric_unit
FROM STG_INCOME_STATEMENT
UNION ALL
SELECT 
    'Latest Data Date' as metric_name,
    MAX(fiscal_date)::varchar as metric_value,
    'date' as metric_unit
FROM STG_INCOME_STATEMENT
UNION ALL
SELECT 
    'Earliest Data Date' as metric_name,
    MIN(fiscal_date)::varchar as metric_value,
    'date' as metric_unit
FROM STG_INCOME_STATEMENT;

-- Grant permissions
GRANT SELECT ON ALL VIEWS IN SCHEMA STAGING TO ROLE EQUITY_DATA_READER;