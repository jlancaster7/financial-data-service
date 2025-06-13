# Financial Data Service

A high-performance equity data pipeline that fetches financial data from FMP API and loads it into Snowflake. Optimized with parallel processing and connection reuse for 77% faster execution.

## Project Structure

```
financial-data-service/
├── sql/                    # Snowflake SQL scripts
├── src/
│   ├── api/               # API client modules
│   ├── db/                # Database connection and operations
│   ├── etl/               # ETL pipeline modules
│   ├── models/            # Data models
│   └── utils/             # Utility functions
├── tests/                 # Test files
├── config/                # Configuration files
├── scripts/               # Utility scripts
├── requirements.txt       # Python dependencies
├── .env.example          # Environment variables template
└── README.md             # This file
```

## Setup

1. **Install Dependencies**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   pip install -r requirements.txt
   ```

2. **Configure Environment**
   ```bash
   cp .env.example .env
   # Edit .env with your Snowflake and FMP API credentials
   ```

3. **Set up Snowflake**
   Run the SQL scripts in order:
   ```
   sql/01_database_setup.sql
   sql/02_schema_setup.sql
   sql/03_table_definitions.sql
   sql/04_populate_date_dimension.sql
   ```

## Usage

### Daily Pipeline Orchestrator

The main way to run the ETL pipeline is through the orchestrator script:

```bash
# Run all pipelines for specific symbols
python scripts/run_daily_pipeline.py --symbols AAPL MSFT GOOGL

# Run for all S&P 500 companies
python scripts/run_daily_pipeline.py --sp500

# Dry run mode (no database changes)
python scripts/run_daily_pipeline.py --dry-run --symbols AAPL

# Skip specific pipelines
python scripts/run_daily_pipeline.py --skip-financial --symbols AAPL

# Custom date range for historical prices
python scripts/run_daily_pipeline.py --from-date 2024-01-01 --to-date 2024-12-31 --symbols AAPL

# See all options
python scripts/run_daily_pipeline.py --help
```

### Individual ETL Scripts

You can also run individual ETL pipelines:

```bash
# Company profiles
python scripts/run_company_etl.py --symbols AAPL MSFT

# Historical prices  
python scripts/run_price_etl.py --symbols AAPL --days-back 30

# Financial statements
python scripts/run_financial_etl.py --symbols AAPL --period annual --limit 5
```

## Performance

The pipeline has been optimized for speed with two key improvements:

1. **Connection Reuse**: Single Snowflake connection shared across ETL operations
2. **Parallel Processing**: Independent ETLs (Company, Price, Financial) run concurrently

Performance results for single symbol processing:
- Baseline: 131 seconds
- With optimizations: 29.6 seconds (77% improvement)

## Development

- Run tests: `pytest`
- Format code: `black .`
- Lint: `flake8`
- Type check: `mypy src/`