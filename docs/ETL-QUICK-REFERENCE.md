# ETL Quick Reference Guide

## 🚀 New ETL Checklist

- [ ] Create API client class (inherit from `BaseAPIClient`)
- [ ] Create transformer class 
- [ ] Create ETL class (inherit from `BaseETL`)
- [ ] Design database tables (RAW, STAGING, ANALYTICS)
- [ ] Add to pipeline orchestrator
- [ ] Write unit tests
- [ ] Document API rate limits

## 📁 File Structure Template

```
src/
├── api/
│   └── your_api_client.py          # API client with rate limiting
├── etl/
│   └── your_data_etl.py            # Main ETL pipeline
├── transformations/
│   └── your_transformer.py         # Data transformation logic
└── models/
    └── your_models.py              # Pydantic models (optional)

tests/
└── test_your_etl.py                # Unit tests

sql/
└── XX_your_tables.sql              # Table creation scripts
```

## 🏗️ Core Components

### 1. API Client Template
```python
from src.api.base_api_client import BaseAPIClient

class YourAPIClient(BaseAPIClient):
    def __init__(self, config):
        super().__init__(
            base_url=config.base_url,
            api_key=config.api_key,
            rate_limit_calls=300,    # Adjust based on API
            rate_limit_period=60
        )
    
    def get_data(self, symbol: str) -> List[Dict]:
        endpoint = f"endpoint/{symbol}"
        return self._make_request(endpoint)
```

### 2. ETL Class Template
```python
from src.etl.base_etl import BaseETL

class YourETL(BaseETL):
    def __init__(self, config: Config):
        super().__init__(
            job_name="your_etl",
            snowflake_connector=SnowflakeConnector(config.snowflake),
            fmp_client=YourAPIClient(config.your_api),
            batch_size=config.app.batch_size
        )
    
    def extract(self, symbols: List[str]) -> List[Dict]:
        # Extract from API
        pass
    
    def transform(self, raw_data: List[Dict]) -> Dict[str, List[Dict]]:
        # Return {'raw': [...], 'staging': [...]}
        pass
    
    def load(self, transformed_data: Dict[str, List[Dict]]) -> int:
        # Load to Snowflake
        pass
```

### 3. Table Design Template
```sql
-- RAW layer (stores original JSON)
CREATE TABLE RAW_DATA.RAW_YOUR_DATA (
    id INTEGER IDENTITY,
    symbol VARCHAR(10),
    extract_timestamp TIMESTAMP_NTZ,
    raw_data VARIANT,
    PRIMARY KEY (id)
);

-- STAGING layer (structured data)
CREATE TABLE STAGING.STG_YOUR_DATA (
    symbol VARCHAR(10),
    date DATE,
    -- your fields here
    loaded_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (symbol, date)  -- Define unique keys
);

-- ANALYTICS layer (business metrics)
CREATE TABLE ANALYTICS.FACT_YOUR_METRICS (
    metric_key INTEGER IDENTITY,
    company_key INTEGER REFERENCES DIM_COMPANY,
    date_key INTEGER REFERENCES DIM_DATE,
    -- aggregated metrics here
    PRIMARY KEY (metric_key)
);
```

## 🔧 Common Operations

### Bulk Insert (Optimized)
```python
# Automatic optimization for VARIANT columns
self.snowflake.bulk_insert('RAW_DATA.RAW_TABLE', data)
```

### Merge (Prevent Duplicates)
```python
self.snowflake.merge(
    table='STAGING.STG_TABLE',
    data=data,
    merge_keys=['symbol', 'date'],        # Unique identifiers
    update_columns=['price', 'volume']     # Columns to update
)
```

### Execute with Row Count
```python
affected = self.snowflake.execute_with_rowcount(
    "UPDATE table SET x = %s WHERE y = %s",
    (value1, value2)
)
logger.info(f"Updated {affected} rows")
```

### Point-in-Time Query Pattern
```python
query = """
SELECT *
FROM fact_table f
WHERE f.accepted_date <= %s  -- Point-in-time
ORDER BY f.accepted_date DESC
LIMIT 1
"""
```

## 📊 Performance Tips

### 1. Connection Reuse
```python
# Good - reuse connection
with self.snowflake as conn:
    conn.bulk_insert(...)
    conn.merge(...)
    conn.execute(...)

# Bad - multiple connections
self.snowflake.bulk_insert(...)
self.snowflake.merge(...)
```

### 2. Batch Processing
```python
BATCH_SIZE = 10000
for i in range(0, len(data), BATCH_SIZE):
    batch = data[i:i + BATCH_SIZE]
    self.snowflake.bulk_insert('table', batch)
```

### 3. Parallel Symbol Processing
```python
from concurrent.futures import ThreadPoolExecutor

with ThreadPoolExecutor(max_workers=5) as executor:
    futures = [executor.submit(self.process_symbol, sym) for sym in symbols]
    results = [f.result() for f in futures]
```

## 🐛 Common Issues & Solutions

| Issue | Solution |
|-------|----------|
| VARIANT timeout | Use bulk_insert (auto-optimized) |
| Duplicate data | Use MERGE with proper keys |
| Certificate errors | Fallback to executemany (automatic) |
| Memory with large data | Process in batches |
| API rate limits | Use BaseAPIClient |
| Time zone issues | Always use UTC |

## 🧪 Testing Patterns

### Unit Test Structure
```python
def test_transform():
    etl = YourETL(mock_config)
    raw_data = [{'test': 'data'}]
    result = etl.transform(raw_data)
    assert 'staging' in result
    assert len(result['staging']) > 0

def test_extract_error_handling():
    etl = YourETL(mock_config)
    etl.api_client.get_data = Mock(side_effect=Exception("API Error"))
    result = etl.extract(['FAIL'])
    assert len(etl.result.errors) > 0
```

### Integration Test Pattern
```python
@pytest.mark.integration
def test_full_pipeline():
    config = get_test_config()
    etl = YourETL(config)
    result = etl.run(['TEST'], test_mode=True)
    assert result['status'] == 'success'
```

## 📝 Logging Best Practices

```python
# Start of operation
logger.info(f"Extracting data for {len(symbols)} symbols")

# Progress updates
logger.debug(f"Processing {symbol}: {len(data)} records")

# Success
logger.info(f"✓ Loaded {count} records to {table}")

# Warnings
logger.warning(f"No data found for {symbol}")

# Errors (but continue)
logger.error(f"Failed to process {symbol}: {e}")
self.result.errors.append(str(e))
```

## 🔄 ETL Result Tracking

```python
# BaseETL provides automatic tracking
self.result.records_extracted = len(raw_data)
self.result.records_transformed = len(transformed['staging'])
self.result.records_loaded = count
self.result.errors.append("Error message")

# Access in orchestrator
if etl.result.status == ETLStatus.SUCCESS:
    logger.info("Pipeline succeeded")
```

## 🚦 Exit Codes

- `0` - Complete success
- `1` - Partial success (some ETLs failed)
- `2` - Complete failure

## 📚 Reference Examples

Look at these ETLs for patterns:
- `CompanyETL` - Simple profile data
- `HistoricalPriceETL` - High-volume time series
- `FinancialStatementETL` - Complex nested data
- `MarketMetricsETL` - Calculated metrics with CTEs
- `TTMCalculationETL` - Complex business logic

## 🛠️ Utility Scripts

```bash
# Load specific data
python scripts/run_daily_pipeline.py --symbols AAPL MSFT

# Truncate for testing
python scripts/truncate_all_tables.py

# Load market metrics in batches
python scripts/load_market_metrics_batch.py --symbols AAPL --from-date 2020-01-01

# Check what's in the database
python scripts/check_snowflake_data.py
```

---

Remember: When in doubt, look at existing implementations. The patterns handle most edge cases!