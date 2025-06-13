# Claude Code Guidance for Financial Data Service

This document provides guidance for future Claude Code instances working on this financial data service project.

## Project Overview
This is a financial data pipeline that extracts market data from Financial Modeling Prep (FMP) API and loads it into Snowflake using a three-layer architecture (Raw → Staging → Analytics).

## Common Commands

### Daily Pipeline Operations
```bash
# Run full pipeline for specific symbols
python scripts/run_daily_pipeline.py --symbols AAPL MSFT GOOGL

# Run for all S&P 500 companies
python scripts/run_daily_pipeline.py --sp500

# Dry run to test without database changes
python scripts/run_daily_pipeline.py --dry-run --symbols AAPL

# Skip specific pipelines
python scripts/run_daily_pipeline.py --skip-financial --symbols AAPL

# Run with specific date range for prices
python scripts/run_daily_pipeline.py --from-date 2024-01-01 --to-date 2024-12-31 --symbols AAPL
```

### Individual ETL Scripts
```bash
# Run company ETL only
python scripts/run_company_etl.py --symbols AAPL MSFT

# Run price ETL with date range
python scripts/run_price_etl.py --symbols AAPL --days-back 30

# Run financial statement ETL
python scripts/run_financial_etl.py --symbols AAPL --period quarterly --limit 8

# Run TTM calculation ETL
python scripts/run_ttm_calculation_etl.py --symbols AAPL MSFT

# Run financial ratio ETL
python scripts/run_financial_ratio_etl.py --symbols AAPL

# Run market metrics ETL
python scripts/run_market_metrics_etl.py --symbols AAPL --start-date 2024-01-01
```

### Testing
```bash
# Run all tests
python -m pytest tests/ -v

# Run specific test file
python -m pytest tests/test_company_etl.py -v

# Run with coverage
python -m pytest tests/ --cov=src --cov-report=html
```

### Database Operations
```bash
# Check data in Snowflake
python scripts/check_snowflake_data.py

# Recreate financial tables (BE CAREFUL - drops tables!)
python scripts/recreate_financial_tables.py
```

## Architecture Overview

### Three-Layer Architecture
1. **RAW Layer** (RAW_DATA schema)
   - Stores raw JSON from APIs as VARIANT columns
   - Immutable historical record
   - Tables: RAW_COMPANY_PROFILE, RAW_HISTORICAL_PRICES, RAW_INCOME_STATEMENT, etc.

2. **STAGING Layer** (STAGING schema)
   - Structured tables with typed columns
   - Uses MERGE statements to prevent duplicates
   - Tables: STG_COMPANY_PROFILE, STG_HISTORICAL_PRICES, STG_INCOME_STATEMENT, etc.

3. **ANALYTICS Layer** (ANALYTICS schema)
   - Star schema for analysis
   - Dimensions: DIM_COMPANY, DIM_DATE
   - Facts: FACT_DAILY_PRICES, FACT_FINANCIALS, FACT_FINANCIAL_RATIOS, FACT_MARKET_METRICS, FACT_FINANCIALS_TTM

### ETL Framework
All ETL pipelines inherit from `BaseETL` and follow this pattern:
1. **Extract**: Get data from FMP API
2. **Transform**: Convert to raw and staging formats
3. **Load**: Insert into Snowflake tables

Key classes:
- `CompanyETL`: Company profiles
- `HistoricalPriceETL`: Daily stock prices
- `FinancialStatementETL`: Income statements, balance sheets, cash flows
- `FinancialRatioETL`: Financial ratios (ROE, ROA, margins, etc.)
- `MarketMetricsETL`: Market-based metrics (P/E, P/B, EV/EBITDA, etc.)
- `TTMCalculationETL`: Pre-calculated trailing twelve month metrics

### Pipeline Orchestration
The `PipelineOrchestrator` in `scripts/run_daily_pipeline.py` runs all ETLs in dependency order:
1. Company profiles (needed for DIM_COMPANY)
2. Historical prices
3. Financial statements
4. TTM calculations (after financial data)
5. Financial ratios
6. Market metrics

## Key Technical Decisions

### VARIANT Column Handling
- Raw layer uses VARIANT columns to store JSON
- Custom `DateTimeEncoder` handles datetime serialization
- Bulk insert detects VARIANT columns and applies PARSE_JSON

### Duplicate Prevention
- Staging tables use MERGE statements
- Unique keys: (symbol, date) for prices, (symbol, fiscal_date, period) for financials
- Ensures idempotent pipeline runs

### Filing Date Capture
- Critical for preventing look-ahead bias
- Captures both `filing_date` and `accepted_date` from FMP API
- Enables proper point-in-time analysis

### Consistent ETL Interface
- All ETL classes take a `Config` object
- Runtime parameters (symbols, dates) passed to extract methods
- Enables flexible orchestration

## Common Issues and Solutions

### Issue: NULL columns in fact tables
**Solution**: Check FMP API field mappings in `src/models/fmp_models.py`. Common mismatches:
- `operatingExpenses` vs `operating_expenses`
- `weightedAverageShsOut` vs `shares_outstanding`
- `commonDividendsPaid` vs `dividends_paid`

### Issue: Duplicate data in staging
**Solution**: Use MERGE statements with proper unique keys. See `SnowflakeConnector.merge()` method.

### Issue: Certificate errors with Snowflake
**Solution**: Using single-row inserts for VARIANT columns. Future optimization could use staging tables.

### Issue: Rate limiting from FMP API
**Solution**: FMPClient has built-in rate limiting (300 calls/minute). Batch endpoints available for company profiles.

## Environment Variables
Required in `.env` file:
```
SNOWFLAKE_ACCOUNT=
SNOWFLAKE_USER=
SNOWFLAKE_PASSWORD=
SNOWFLAKE_WAREHOUSE=
SNOWFLAKE_DATABASE=EQUITY_DATA
SNOWFLAKE_SCHEMA=PUBLIC
SNOWFLAKE_ROLE=EQUITY_DATA_LOADER

FMP_API_KEY=
FMP_BASE_URL=https://financialmodelingprep.com/api/v3

LOG_LEVEL=INFO
BATCH_SIZE=1000
ENABLE_MONITORING=true
```

## Recent Updates (2025-06-13)

### Critical Performance Fix - Bulk Insert Rewrite ✅ COMPLETED
Fixed major performance issue where bulk inserts were timing out:

1. **Root Cause**: VARIANT column detection causing single-row inserts for ALL tables
2. **Solution**: Complete rewrite using pandas write_pandas() with executemany fallback
3. **Result**: 5+ minute timeouts → ~8 seconds for 1,256 records

### FACT_MARKET_METRICS Fix ✅ COMPLETED
Fixed market metrics loading issue:

1. **Problem**: Only 21 records instead of 6,280 (complex CTE query timeouts)
2. **Solution**: Created batch loading script (load_market_metrics_batch.py)
3. **Result**: Successfully loaded all 6,280 market metrics records

### New Utilities and Scripts
- **load_market_metrics_batch.py**: Process market metrics in date batches to avoid timeouts
- **load_top_stocks_by_market_cap.py**: Load S&P 500 companies by market cap
- **truncate_all_tables.py**: Delete all data from all tables for testing

## Recent Updates (2025-06-12)

### Performance Optimizations ✅ COMPLETED
Implemented Phase 1 and Phase 2 performance optimizations achieving 77% improvement:

1. **Phase 1: Connection Reuse (48% improvement)**:
   - Modified SnowflakeConnector to support connection reuse with `use_pooling` flag
   - Added timing to BaseETL for visibility into extract/transform/load phases
   - Single connection shared across all ETL operations

2. **Phase 2: Parallel Processing (Additional 56% improvement)**:
   - Modified run_daily_pipeline.py to run independent ETLs concurrently
   - Used ThreadPoolExecutor to parallelize Company, Price, and Financial ETLs
   - Sequential execution only for dependent ETLs (TTM, Ratios, Metrics)

3. **Results**:
   - Baseline: 131 seconds per symbol
   - After Phase 1: 68 seconds (48% faster)
   - After Phase 2: 29.6 seconds (77% faster than baseline)
   - Price ETL is now the bottleneck at 27.8s (94% of total time)

## Recent Updates (2025-06-09)

### Story 5.3: TTM Financial Calculations ✅ COMPLETED
Successfully implemented pre-calculated TTM (Trailing Twelve Month) financial metrics:

1. **FACT_FINANCIALS_TTM Table**:
   - Stores pre-calculated TTM metrics for efficient querying
   - Flow metrics (summed): revenue, net income, EPS, cash flows, etc.
   - Stock metrics (point-in-time): shares outstanding, assets, equity, etc.
   - Tracks calculation_date, accepted_date, and quarters used

2. **TTMCalculationETL Implementation**:
   - Finds all dates where 4 quarters of data are available
   - Respects point-in-time logic using accepted_date
   - Prevents duplicate calculations with unique constraint
   - Integrated into daily pipeline after financial data ETL

3. **Verification**:
   - Loaded 10 TTM records (5 AAPL, 5 MSFT)
   - AAPL TTM Revenue (2025-05-02): $400.37B ✓ (verified against external source)
   - All integrity tests pass: exactly 4 quarters used, no duplicates

## Important Notes

### TTM Calculation Logic
- Uses the 4 most recent quarters available on each calculation date
- Looks back ~15 months to find quarters (handles reporting delays)
- Flow metrics: SUM of 4 quarters (revenue, net income, cash flows)
- Stock metrics: Most recent quarter's value (shares outstanding, assets)

### Point-in-Time Analysis
- All calculations respect accepted_date to prevent look-ahead bias
- Market metrics should join on price date <= TTM calculation date
- Ensures historical backtesting accuracy

## Future Enhancements
1. ~~Implement FACT_FINANCIAL_RATIOS calculations~~ ✅ COMPLETED
2. ~~Create FACT_MARKET_METRICS for daily market-based ratios~~ ✅ COMPLETED
3. ~~Implement FACT_FINANCIALS_TTM for pre-calculated metrics~~ ✅ COMPLETED
4. Refactor FACT_MARKET_METRICS to use pre-calculated TTM values (Story 5.4)
5. Add revenue_per_share to FACT_FINANCIAL_RATIOS
6. Add more sophisticated error recovery
7. Implement data quality monitoring dashboard
8. Add support for real-time data feeds