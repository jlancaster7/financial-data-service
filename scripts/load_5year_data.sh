#!/bin/bash
# Load 5 years of data for top 5 tech stocks
# This script properly loads all historical data

echo "Loading 5 years of data for MSFT, NVDA, AAPL, AMZN, GOOGL"
echo "============================================================"

# For 5 years of quarterly data, we need at least 20 quarters
# Adding buffer for safety
QUARTERLY_LIMIT=25

# Run the pipeline with correct parameters
python scripts/run_daily_pipeline.py \
    --symbols MSFT NVDA AAPL AMZN GOOGL \
    --from-date 2020-06-13 \
    --to-date 2025-06-12 \
    --period quarterly \
    --limit $QUARTERLY_LIMIT

echo ""
echo "Pipeline complete. Checking results..."
echo ""

# Check what was loaded
python -c "
from src.utils.config import Config
from src.db.snowflake_connector import SnowflakeConnector

config = Config.load()
conn = SnowflakeConnector(config.snowflake)
conn.connect()

print('Data loaded:')
print('=' * 60)

# Check fact tables
tables = [
    ('ANALYTICS.FACT_DAILY_PRICES', 'Daily Prices'),
    ('ANALYTICS.FACT_FINANCIALS', 'Financial Statements'),
    ('ANALYTICS.FACT_FINANCIAL_RATIOS', 'Financial Ratios'),
    ('ANALYTICS.FACT_FINANCIALS_TTM', 'TTM Calculations'),
    ('ANALYTICS.FACT_MARKET_METRICS', 'Market Metrics')
]

for table, name in tables:
    result = conn.fetch_one(f'SELECT COUNT(*) as count FROM {table}')
    print(f'{name}: {result[\"COUNT\"]:,} records')

# Check financial data date range
result = conn.fetch_one('''
    SELECT 
        MIN(d.date) as min_date,
        MAX(d.date) as max_date,
        COUNT(DISTINCT d.date) as unique_dates
    FROM ANALYTICS.FACT_FINANCIALS f
    JOIN ANALYTICS.DIM_DATE d ON f.fiscal_date_key = d.date_key
''')
print(f'\\nFinancial data date range: {result[\"MIN_DATE\"]} to {result[\"MAX_DATE\"]}')
print(f'Unique fiscal dates: {result[\"UNIQUE_DATES\"]}')

conn.disconnect()
"