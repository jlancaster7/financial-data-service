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
python scripts/run_financial_etl.py --symbols AAPL --period annual --limit 5
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
   - Facts: FACT_DAILY_PRICES, FACT_FINANCIALS, FACT_FINANCIAL_RATIOS (pending)

### ETL Framework
All ETL pipelines inherit from `BaseETL` and follow this pattern:
1. **Extract**: Get data from FMP API
2. **Transform**: Convert to raw and staging formats
3. **Load**: Insert into Snowflake tables

Key classes:
- `CompanyETL`: Company profiles
- `HistoricalPriceETL`: Daily stock prices
- `FinancialStatementETL`: Income statements, balance sheets, cash flows

### Pipeline Orchestration
The `PipelineOrchestrator` in `scripts/run_daily_pipeline.py` runs all ETLs in dependency order:
1. Company profiles (needed for DIM_COMPANY)
2. Historical prices
3. Financial statements

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

## Future Enhancements
1. Implement FACT_FINANCIAL_RATIOS calculations
2. Create FACT_MARKET_METRICS for daily market-based ratios
3. Add more sophisticated error recovery
4. Implement data quality monitoring dashboard
5. Add support for real-time data feeds