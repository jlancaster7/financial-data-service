# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a financial data pipeline that extracts equity data from the Financial Modeling Prep (FMP) API and loads it into Snowflake using a three-layer architecture (RAW → STAGING → ANALYTICS).

## Common Commands

### Development Setup
```bash
# Create and activate virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Set up environment
cp .env.example .env
# Edit .env with Snowflake and FMP API credentials
```

### Testing
```bash
# Run all tests
pytest

# Run specific test file
pytest tests/test_fmp_client.py

# Run with coverage
pytest --cov=src

# Run specific test
pytest tests/test_fmp_client.py::TestFMPClient::test_get_company_profile_success
```

### Code Quality
```bash
# Format code
black .

# Lint code
flake8

# Type checking
mypy src/
```

### Running ETL Pipelines
```bash
# Company profile ETL
python scripts/run_company_etl.py --symbols AAPL MSFT GOOGL

# Historical price ETL
python scripts/run_price_etl.py --symbols AAPL --days-back 30

# Financial statement ETL
python scripts/run_financial_etl.py --symbols AAPL MSFT --period annual --limit 5

# Run for all S&P 500 (omit --symbols)
python scripts/run_company_etl.py
```

### Snowflake Setup
```bash
# Run SQL scripts in order
sql/01_database_setup.sql
sql/02_schema_setup.sql
sql/03_table_definitions.sql
sql/04_populate_date_dimension.sql
sql/05_etl_monitoring_tables.sql
```

## Architecture

### Three-Layer Data Architecture

1. **RAW Layer** (`RAW_DATA` schema)
   - Stores raw JSON responses from FMP API as VARIANT columns
   - Preserves original API responses for audit trail
   - Tables: `RAW_COMPANY_PROFILE`, `RAW_HISTORICAL_PRICES`, `RAW_INCOME_STATEMENT`, etc.

2. **STAGING Layer** (`STAGING` schema)
   - Structured tables with parsed and validated data
   - Uses MERGE statements to prevent duplicates
   - Tables: `STG_COMPANY_PROFILE`, `STG_HISTORICAL_PRICES`, `STG_INCOME_STATEMENT`, etc.

3. **ANALYTICS Layer** (`ANALYTICS` schema)
   - Star schema with dimension and fact tables
   - `DIM_COMPANY` - SCD Type 2 for company changes
   - `DIM_DATE` - Pre-populated date dimension (2020-2030)
   - `FACT_DAILY_PRICES`, `FACT_FINANCIALS`, `FACT_FINANCIAL_RATIOS`

### ETL Framework

The project uses an abstract ETL framework (`src/etl/base_etl.py`) with:
- Extract, Transform, Load methods with retry logic
- Batch processing capabilities
- Monitoring hooks for observability
- Data quality validation integration
- Standardized error handling and result tracking

Each ETL pipeline extends this base class and implements domain-specific logic.

### Key Technical Decisions

1. **VARIANT Column Handling**: Snowflake VARIANT columns require special handling. The bulk_insert method detects VARIANT columns and applies PARSE_JSON automatically.

2. **Duplicate Prevention**: All staging tables use MERGE statements with appropriate merge keys (e.g., symbol + date) to ensure idempotent ETL runs.

3. **Filing Date Capture**: Financial statements capture both `filing_date` and `accepted_date` to prevent look-ahead bias in quantitative analysis.

4. **FMP API Integration**: 
   - Uses `/stable/` endpoints with query parameters
   - Rate limiting (300 calls/minute)
   - See `docs/FMP_FIELD_MAPPINGS.md` for detailed field mappings

### Important Files

- `src/db/snowflake_connector.py` - Snowflake connection handling with context managers
- `src/api/fmp_client.py` - FMP API client with rate limiting
- `src/etl/base_etl.py` - Abstract ETL framework
- `src/models/fmp_models.py` - Data models with field mappings
- `src/transformations/fmp_transformer.py` - Data transformation logic
- `sql/03_table_definitions.sql` - Complete database schema

### Monitoring

ETL job monitoring is available when `enable_monitoring=true` in config:
- Job history tracked in `ETL_JOB_HISTORY`
- Errors logged to `ETL_JOB_ERRORS`
- Data quality issues in `ETL_DATA_QUALITY_ISSUES`
- Performance metrics in `ETL_JOB_METRICS`