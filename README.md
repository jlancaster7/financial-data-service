# Financial Data Service

A simplified equity data pipeline that fetches financial data from FMP API and loads it into Snowflake.

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

```python
# Run the ETL pipeline
python -m src.main --symbols AAPL,MSFT,GOOGL --start-date 2023-01-01
```

## Development

- Run tests: `pytest`
- Format code: `black .`
- Lint: `flake8`
- Type check: `mypy src/`