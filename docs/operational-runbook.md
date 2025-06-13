# Financial Data Service - Operational Runbook

**Last Updated**: 2025-06-12  
**Version**: 1.0

## Table of Contents
1. [Service Overview](#service-overview)
2. [Common Issues & Solutions](#common-issues--solutions)
3. [Pipeline Troubleshooting](#pipeline-troubleshooting)
4. [Performance Troubleshooting](#performance-troubleshooting)
5. [Data Recovery Procedures](#data-recovery-procedures)
6. [Maintenance Procedures](#maintenance-procedures)
7. [Emergency Contacts](#emergency-contacts)
8. [Quick Reference](#quick-reference)

---

## Service Overview

### What This Service Does
The Financial Data Service is a high-performance ETL pipeline that:
- Fetches equity market data from Financial Modeling Prep (FMP) API
- Loads data into Snowflake using a three-layer architecture (Raw → Staging → Analytics)
- Calculates financial ratios, TTM metrics, and market metrics
- Runs daily updates with parallel processing for optimal performance

### Architecture Components
1. **FMP API Client**: Rate-limited API client (300 requests/minute)
2. **ETL Pipelines**: Company, Price, Financial, TTM, Ratios, Market Metrics
3. **Snowflake Database**: Three-layer architecture with VARIANT storage
4. **Pipeline Orchestrator**: Parallel execution framework

### Key Performance Metrics
- Single symbol processing: ~30 seconds (after optimizations)
- Parallel processing: Company, Price, Financial ETLs run concurrently
- Connection reuse: Single Snowflake connection shared across operations

---

## Common Issues & Solutions

### Issue 1: Pipeline Timeout Errors
**Symptoms**: 
- `TimeoutError: Request timed out after 600.0 seconds`
- Pipeline hangs during execution

**Root Causes**:
1. Missing analytics layer updates
2. Network connectivity issues
3. Snowflake warehouse suspended

**Solutions**:
```bash
# Check if analytics updates are being called
grep "update_fact_table" scripts/run_daily_pipeline.py

# Test Snowflake connection
python -c "from src.db.snowflake_connector import SnowflakeConnector; from src.utils.config import Config; c = Config.load(); s = SnowflakeConnector(c.snowflake); s.connect(); print('Connected'); s.disconnect()"

# Resume Snowflake warehouse if suspended
# In Snowflake: ALTER WAREHOUSE EQUITY_DATA_WAREHOUSE RESUME;
```

### Issue 2: Quarterly Data Showing as Annual (FY)
**Symptoms**:
- Financial data has period='FY' instead of 'quarterly'
- Missing recent quarterly data

**Root Cause**: 
FMP API requires `period='quarter'` parameter for quarterly data (not 'quarterly')

**Solution**:
Check the FMP client code:
```python
# src/api/fmp_client.py - should have:
elif period.lower() == 'quarterly':
    params['period'] = 'quarter'  # NOT 'quarterly'
```

### Issue 3: Dividend Yield Always NULL
**Symptoms**:
- FACT_MARKET_METRICS.dividend_yield is NULL for all records

**Root Cause**: 
Dividends are stored as negative values in cash flow statements

**Solution**:
Check market metrics ETL:
```python
# Should check for ttm_dividends < 0 (not > 0)
if close_price > 0 and ttm_dividends < 0 and shares_outstanding > 0:
    dividends_per_share = abs(ttm_dividends) / shares_outstanding
    metric_record['dividend_yield'] = round((dividends_per_share / close_price) * 100, 2)
```

### Issue 4: ImportError for snowflake.connector.pooling
**Symptoms**:
- `ImportError: cannot import name 'pooling' from 'snowflake.connector'`

**Root Cause**: 
Older snowflake-connector-python versions don't have pooling module

**Solution**:
Use simple connection reuse instead:
```python
# Don't import pooling
# Instead, implement connection reuse in SnowflakeConnector
self.use_pooling = use_pooling
# In disconnect(): keep connection alive if use_pooling=True
```

### Issue 5: Empty Fact Tables After Pipeline Run
**Symptoms**:
- Pipeline reports success but fact tables are empty
- Analytics layer not updated

**Root Cause**:
Missing calls to `update_fact_table()` methods in pipeline

**Solution**:
Ensure analytics updates are called:
```python
# In run_daily_pipeline.py
if not args.skip_analytics and records_loaded > 0:
    logger.info("Updating FACT_FINANCIALS...")
    etl.update_fact_table(symbols)
```

---

## Pipeline Troubleshooting

### Company ETL Issues

**Check API Response**:
```bash
# Test single company profile
python -c "from src.api.fmp_client import FMPClient; from src.utils.config import Config; c = Config.load(); f = FMPClient(c.fmp); print(f.get_company_profile(['AAPL']))"
```

**Verify Data in Snowflake**:
```sql
-- Check raw data
SELECT COUNT(*) FROM RAW_DATA.RAW_COMPANY_PROFILE WHERE symbol = 'AAPL';

-- Check staging
SELECT * FROM STAGING.STG_COMPANY_PROFILE WHERE symbol = 'AAPL';

-- Check dimension
SELECT * FROM ANALYTICS.DIM_COMPANY WHERE symbol = 'AAPL' AND is_current = TRUE;
```

### Price ETL Issues

**Common Problems**:
1. Missing historical data
2. Duplicate price records
3. Date range issues

**Debug Commands**:
```bash
# Run price ETL for specific date range
python scripts/run_price_etl.py --symbols AAPL --from-date 2024-01-01 --to-date 2024-01-31

# Check for duplicates
```
```sql
SELECT symbol, date, COUNT(*) 
FROM STAGING.STG_HISTORICAL_PRICES 
GROUP BY symbol, date 
HAVING COUNT(*) > 1;
```

### Financial Statement ETL Issues

**Field Mapping Problems**:
Common fields that may be NULL due to API changes:
- `operatingExpenses` → `operating_expenses`
- `weightedAverageShsOut` → `shares_outstanding`
- `commonDividendsPaid` → `dividends_paid`

**Debug Process**:
1. Check raw JSON in RAW_INCOME_STATEMENT
2. Verify field names match FMP response
3. Update field mappings in transform logic

**Verify Quarter Loading**:
```sql
-- Should see 'quarterly' not 'FY' for recent quarters
SELECT symbol, fiscal_date, period, filing_date 
FROM STAGING.STG_INCOME_STATEMENT 
WHERE symbol = 'AAPL' 
ORDER BY fiscal_date DESC;
```

### TTM Calculation Issues

**Common Problems**:
1. Not finding 4 quarters of data
2. Incorrect date lookback logic
3. Duplicate TTM records

**Debug Queries**:
```sql
-- Check available quarters
SELECT symbol, fiscal_date, period, accepted_date
FROM ANALYTICS.FACT_FINANCIALS
WHERE symbol = 'AAPL' AND period = 'quarterly'
ORDER BY fiscal_date DESC;

-- Verify TTM calculations
SELECT * FROM ANALYTICS.FACT_FINANCIALS_TTM
WHERE symbol = 'AAPL'
ORDER BY calculation_date DESC;

-- Check quarters used
SELECT calculation_date, quarters_used
FROM ANALYTICS.FACT_FINANCIALS_TTM
WHERE symbol = 'AAPL' AND calculation_date = '2025-05-02';
```

### Market Metrics Issues

**P/E Ratio Calculation**:
- Uses eps_diluted from FACT_FINANCIALS (quarterly)
- Uses ttm_eps_diluted from FACT_FINANCIALS_TTM

**Debug Missing Metrics**:
```sql
-- Check joins are working
SELECT 
    dp.symbol,
    dp.date,
    dp.close_price,
    f.eps_diluted as quarterly_eps,
    ttm.ttm_eps_diluted,
    ttm.ttm_revenue
FROM ANALYTICS.FACT_DAILY_PRICES dp
LEFT JOIN ANALYTICS.FACT_FINANCIALS f ON dp.symbol = f.symbol 
    AND dp.date >= f.accepted_date
LEFT JOIN ANALYTICS.FACT_FINANCIALS_TTM ttm ON dp.symbol = ttm.symbol
    AND dp.date >= ttm.accepted_date
WHERE dp.symbol = 'AAPL' AND dp.date = '2025-01-15'
LIMIT 1;
```

---

## Performance Troubleshooting

### Slow Pipeline Execution

**Check Connection Pooling**:
```python
# Verify pooling is enabled
config = Config.load()
print(f"Connection pool size: {config.app.connection_pool_size}")
print(f"Parallel processing: {config.app.enable_parallel_processing}")
```

**Monitor Parallel Execution**:
The pipeline should show:
```
PHASE 1: Running independent ETLs in parallel
Parallel ETLs: ['company', 'price', 'financial']
```

If not running in parallel:
1. Check ThreadPoolExecutor implementation
2. Verify independent ETLs are grouped correctly
3. Check for connection contention

### Memory Issues

**Symptoms**:
- Out of memory errors
- Process killed

**Solutions**:
1. Reduce batch size in config
2. Process symbols in smaller batches
3. Use --symbols flag instead of --sp500

```bash
# Process in batches
python scripts/run_daily_pipeline.py --symbols AAPL MSFT GOOGL
python scripts/run_daily_pipeline.py --symbols META AMZN NVDA
```

---

## Data Recovery Procedures

### Recover from Partial Load Failure

1. **Identify Failed Symbols**:
```sql
-- Find symbols with incomplete data
SELECT DISTINCT s.symbol
FROM STAGING.STG_COMPANY_PROFILE s
WHERE NOT EXISTS (
    SELECT 1 FROM ANALYTICS.FACT_DAILY_PRICES f
    WHERE f.symbol = s.symbol
    AND f.date >= CURRENT_DATE - 30
);
```

2. **Re-run for Failed Symbols**:
```bash
# Re-run specific pipeline for failed symbols
python scripts/run_daily_pipeline.py --symbols FAILED_SYMBOL1 FAILED_SYMBOL2
```

### Recover from Duplicate Data

1. **Identify Duplicates**:
```sql
-- Find duplicate financial records
SELECT symbol, fiscal_date, period, COUNT(*)
FROM ANALYTICS.FACT_FINANCIALS
GROUP BY symbol, fiscal_date, period
HAVING COUNT(*) > 1;
```

2. **Clean Duplicates**:
```sql
-- Delete duplicates keeping latest load
DELETE FROM ANALYTICS.FACT_FINANCIALS
WHERE (symbol, fiscal_date, period, _loaded_at) NOT IN (
    SELECT symbol, fiscal_date, period, MAX(_loaded_at)
    FROM ANALYTICS.FACT_FINANCIALS
    GROUP BY symbol, fiscal_date, period
);
```

### Full Table Reload

**When Needed**:
- Corrupted data
- Major schema changes
- Field mapping updates

**Process**:
```bash
# 1. Backup existing data (in Snowflake)
CREATE TABLE ANALYTICS.FACT_FINANCIALS_BACKUP AS 
SELECT * FROM ANALYTICS.FACT_FINANCIALS;

# 2. Recreate tables
python scripts/recreate_financial_tables.py

# 3. Reload data
python scripts/run_daily_pipeline.py --symbols AAPL MSFT GOOGL
```

---

## Maintenance Procedures

### Daily Maintenance

1. **Check Pipeline Status**:
```sql
-- Recent job history
SELECT job_name, status, start_time, end_time, records_loaded
FROM MONITORING.ETL_JOB_HISTORY
WHERE start_time >= CURRENT_DATE
ORDER BY start_time DESC;
```

2. **Monitor Data Freshness**:
```sql
-- Check latest data dates
SELECT 
    'PRICES' as data_type,
    MAX(date) as latest_date,
    DATEDIFF('hour', MAX(date), CURRENT_TIMESTAMP) as hours_old
FROM ANALYTICS.FACT_DAILY_PRICES
UNION ALL
SELECT 
    'FINANCIALS' as data_type,
    MAX(fiscal_date) as latest_date,
    DATEDIFF('day', MAX(fiscal_date), CURRENT_DATE) as days_old
FROM ANALYTICS.FACT_FINANCIALS;
```

### Weekly Maintenance

1. **Check for Missing Symbols**:
```sql
-- Companies without recent price data
SELECT c.symbol
FROM ANALYTICS.DIM_COMPANY c
WHERE c.is_current = TRUE
AND NOT EXISTS (
    SELECT 1 FROM ANALYTICS.FACT_DAILY_PRICES p
    WHERE p.symbol = c.symbol
    AND p.date >= CURRENT_DATE - 7
);
```

2. **Review Error Logs**:
```sql
SELECT job_name, error_message, COUNT(*) as error_count
FROM MONITORING.ETL_JOB_ERRORS
WHERE error_time >= CURRENT_DATE - 7
GROUP BY job_name, error_message
ORDER BY error_count DESC;
```

### Monthly Maintenance

1. **Archive Old Logs**:
```sql
-- Move old monitoring data to archive
INSERT INTO MONITORING.ETL_JOB_HISTORY_ARCHIVE
SELECT * FROM MONITORING.ETL_JOB_HISTORY
WHERE start_time < DATEADD('month', -3, CURRENT_DATE);

DELETE FROM MONITORING.ETL_JOB_HISTORY
WHERE start_time < DATEADD('month', -3, CURRENT_DATE);
```

2. **Update S&P 500 Constituents**:
```bash
# Get latest S&P 500 list
python scripts/update_sp500_constituents.py
```

---

## Emergency Contacts

### Escalation Path
1. **Level 1**: Check this runbook
2. **Level 2**: Review error logs and monitoring tables
3. **Level 3**: Contact team lead
4. **Level 4**: Contact vendor support

### Key Contacts
- **FMP API Support**: support@financialmodelingprep.com
- **Snowflake Support**: (via web portal)
- **Internal Team**: (Add your team contacts here)

### API Keys and Credentials
- Store in `.env` file (never commit)
- Backup in secure password manager
- Rotate quarterly

---

## Quick Reference

### Most Common Commands

```bash
# Run daily update for specific symbols
python scripts/run_daily_pipeline.py --symbols AAPL MSFT GOOGL

# Run with skip options
python scripts/run_daily_pipeline.py --skip-financial --symbols AAPL

# Dry run
python scripts/run_daily_pipeline.py --dry-run --symbols AAPL

# Check data
python scripts/check_snowflake_data.py
```

### Key File Locations
- **Config**: `src/utils/config.py`
- **FMP Client**: `src/api/fmp_client.py`
- **ETL Pipelines**: `src/etl/*_etl.py`
- **Main Orchestrator**: `scripts/run_daily_pipeline.py`
- **SQL Scripts**: `sql/*.sql`

### Performance Settings
```python
# In .env file
CONNECTION_POOL_SIZE=5
PIPELINE_TIMEOUT=600
ENABLE_PARALLEL_PROCESSING=true
BATCH_SIZE=1000
```

### Critical Gotchas
1. **Quarterly Data**: Use `period='quarter'` not 'quarterly'
2. **Dividends**: Stored as negative in cash flows
3. **TTM Logic**: Looks back ~15 months for 4 quarters
4. **Connection Pooling**: Simple reuse, not formal pooling
5. **Parallel ETLs**: Only Company, Price, Financial run in parallel

---

## Appendix: Useful SQL Queries

### Data Quality Checks
```sql
-- Missing financial data
SELECT * FROM STAGING.V_MISSING_FINANCIAL_DATA;

-- Data freshness
SELECT * FROM STAGING.V_STAGING_DATA_FRESHNESS;

-- Symbol coverage
SELECT 
    COUNT(DISTINCT symbol) as total_symbols,
    COUNT(DISTINCT CASE WHEN has_prices THEN symbol END) as symbols_with_prices,
    COUNT(DISTINCT CASE WHEN has_financials THEN symbol END) as symbols_with_financials
FROM (
    SELECT 
        c.symbol,
        EXISTS(SELECT 1 FROM ANALYTICS.FACT_DAILY_PRICES p WHERE p.symbol = c.symbol) as has_prices,
        EXISTS(SELECT 1 FROM ANALYTICS.FACT_FINANCIALS f WHERE f.symbol = c.symbol) as has_financials
    FROM ANALYTICS.DIM_COMPANY c
    WHERE c.is_current = TRUE
) t;
```

### Performance Analysis
```sql
-- ETL performance by pipeline
SELECT 
    job_name,
    AVG(DATEDIFF('second', start_time, end_time)) as avg_duration_seconds,
    MAX(DATEDIFF('second', start_time, end_time)) as max_duration_seconds,
    AVG(records_loaded) as avg_records
FROM MONITORING.ETL_JOB_HISTORY
WHERE status = 'SUCCESS'
GROUP BY job_name
ORDER BY avg_duration_seconds DESC;
```

---

*End of Runbook - Version 1.0*