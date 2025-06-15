# ETL Cookbook: Building New Data Pipelines

## Table of Contents
1. [Introduction](#introduction)
2. [Architecture Overview](#architecture-overview)
3. [Core Patterns](#core-patterns)
4. [Step-by-Step: Adding a New Data Source](#step-by-step-adding-a-new-data-source)
5. [Example: Building an Options Data Pipeline](#example-building-an-options-data-pipeline)
6. [Best Practices](#best-practices)
7. [Common Pitfalls](#common-pitfalls)
8. [Testing Your Pipeline](#testing-your-pipeline)
9. [Performance Optimization](#performance-optimization)

## Introduction

This cookbook provides a comprehensive guide for adding new data sources to the Financial Data Service. Whether you're adding options data, economic indicators, cryptocurrency prices, or any other financial dataset, this guide will help you build a robust, performant ETL pipeline that follows our established patterns.

### What You'll Learn
- How to structure your ETL pipeline using our three-layer architecture
- How to leverage the BaseETL framework for consistency
- How to handle different data types and API patterns
- How to ensure data quality and prevent duplicates
- How to optimize for performance

## Architecture Overview

### Three-Layer Data Architecture

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   RAW_DATA  │ --> │  STAGING    │ --> │  ANALYTICS  │
│  (VARIANT)  │     │(Structured) │     │(Star Schema)│
└─────────────┘     └─────────────┘     └─────────────┘
```

1. **RAW Layer** (`RAW_DATA` schema)
   - Stores original API responses as VARIANT (JSON)
   - Immutable historical record
   - Enables data lineage and debugging

2. **STAGING Layer** (`STAGING` schema)
   - Structured tables with proper data types
   - Data validation and cleansing
   - MERGE operations prevent duplicates

3. **ANALYTICS Layer** (`ANALYTICS` schema)
   - Star schema design (facts and dimensions)
   - Pre-calculated metrics for performance
   - Business-friendly column names

### Key Components

```
src/
├── api/              # API clients for data sources
├── db/               # Database connectors
├── etl/              # ETL pipeline classes
├── models/           # Data models and schemas
├── transformations/  # Data transformation logic
└── utils/           # Utilities and configuration
```

## Core Patterns

### 1. BaseETL Framework

Every ETL pipeline inherits from `BaseETL`:

```python
from src.etl.base_etl import BaseETL

class YourNewETL(BaseETL):
    def __init__(self, config: Config):
        super().__init__(
            job_name="your_etl_name",
            snowflake_connector=snowflake_connector,
            fmp_client=api_client,  # Your API client
            batch_size=config.app.batch_size,
            enable_monitoring=config.app.enable_monitoring
        )
    
    def extract(self, **kwargs) -> List[Dict[str, Any]]:
        """Extract data from API"""
        pass
    
    def transform(self, raw_data: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
        """Transform to raw and staging formats"""
        pass
    
    def load(self, transformed_data: Dict[str, List[Dict[str, Any]]]) -> int:
        """Load to Snowflake"""
        pass
```

### 2. API Client Pattern

Create a dedicated API client with rate limiting:

```python
from src.api.base_api_client import BaseAPIClient

class YourAPIClient(BaseAPIClient):
    def __init__(self, config: YourAPIConfig):
        super().__init__(
            base_url=config.base_url,
            api_key=config.api_key,
            rate_limit_calls=config.rate_limit_calls,
            rate_limit_period=config.rate_limit_period
        )
    
    def get_your_data(self, symbol: str) -> List[Dict[str, Any]]:
        """Fetch data with automatic rate limiting"""
        endpoint = f"your-endpoint/{symbol}"
        return self._make_request(endpoint)
```

### 3. Transformation Pattern

Use a dedicated transformer class:

```python
class YourTransformer(BaseTransformer):
    def transform_to_raw(self, data: List[Dict]) -> List[Dict]:
        """Transform to RAW layer format"""
        return [{
            'symbol': record['symbol'],
            'extract_timestamp': datetime.now(timezone.utc),
            'raw_data': record  # Store complete response
        } for record in data]
    
    def transform_to_staging(self, data: List[Dict]) -> List[Dict]:
        """Transform to STAGING layer format"""
        return [{
            'symbol': record['symbol'],
            'date': self.parse_date(record['date']),
            'value': float(record['value']),
            # ... structured fields
        } for record in data]
```

### 4. Data Loading Pattern

Use bulk operations with proper error handling:

```python
def load(self, transformed_data: Dict[str, List[Dict[str, Any]]]) -> int:
    total_loaded = 0
    
    # Load RAW layer
    if transformed_data.get('raw'):
        self.snowflake.bulk_insert('RAW_DATA.RAW_YOUR_TABLE', transformed_data['raw'])
        total_loaded += len(transformed_data['raw'])
    
    # Load STAGING layer with MERGE to prevent duplicates
    if transformed_data.get('staging'):
        self.snowflake.merge(
            table='STAGING.STG_YOUR_TABLE',
            data=transformed_data['staging'],
            merge_keys=['symbol', 'date'],  # Your unique keys
            update_columns=['value', 'updated_timestamp']
        )
        total_loaded += len(transformed_data['staging'])
    
    return total_loaded
```

## Step-by-Step: Adding a New Data Source

### Step 1: Plan Your Data Model

1. **Identify the data source**
   - API documentation
   - Rate limits
   - Authentication method
   - Data update frequency

2. **Design your tables**
   ```sql
   -- RAW layer
   CREATE TABLE RAW_DATA.RAW_YOUR_DATA (
       id INTEGER IDENTITY,
       symbol VARCHAR(10),
       extract_timestamp TIMESTAMP_NTZ,
       raw_data VARIANT,
       PRIMARY KEY (id)
   );
   
   -- STAGING layer
   CREATE TABLE STAGING.STG_YOUR_DATA (
       symbol VARCHAR(10),
       date DATE,
       value FLOAT,
       -- ... other fields
       loaded_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
       PRIMARY KEY (symbol, date)
   );
   
   -- ANALYTICS layer
   CREATE TABLE ANALYTICS.FACT_YOUR_METRICS (
       metric_key INTEGER IDENTITY,
       company_key INTEGER REFERENCES DIM_COMPANY,
       date_key INTEGER REFERENCES DIM_DATE,
       metric_value FLOAT,
       -- ... other metrics
       PRIMARY KEY (metric_key)
   );
   ```

### Step 2: Create the API Client

1. **Create config class** in `src/utils/config.py`:
   ```python
   @dataclass
   class YourAPIConfig:
       api_key: str
       base_url: str = "https://api.yourprovider.com/v1"
       rate_limit_calls: int = 100
       rate_limit_period: int = 60
   ```

2. **Implement API client** in `src/api/your_api_client.py`:
   ```python
   class YourAPIClient(BaseAPIClient):
       def get_options_chain(self, symbol: str) -> List[Dict[str, Any]]:
           endpoint = f"options/{symbol}"
           return self._make_request(endpoint)
   ```

### Step 3: Create the Transformer

Create `src/transformations/your_transformer.py`:

```python
class YourDataTransformer:
    def transform_your_data(self, symbol: str, raw_data: List[Dict]) -> Dict[str, List[Dict]]:
        """Transform API response to our schema"""
        
        raw_records = []
        staging_records = []
        
        for record in raw_data:
            # RAW layer - preserve everything
            raw_records.append({
                'symbol': symbol,
                'extract_timestamp': datetime.now(timezone.utc),
                'raw_data': record
            })
            
            # STAGING layer - structured data
            staging_records.append({
                'symbol': symbol,
                'date': self.parse_date(record.get('date')),
                'strike_price': float(record.get('strike', 0)),
                'option_type': record.get('type', 'CALL'),
                'bid': float(record.get('bid', 0)),
                'ask': float(record.get('ask', 0)),
                'volume': int(record.get('volume', 0)),
                'open_interest': int(record.get('openInterest', 0)),
                'implied_volatility': float(record.get('impliedVolatility', 0))
            })
        
        return {
            'raw': raw_records,
            'staging': staging_records
        }
```

### Step 4: Implement the ETL Pipeline

Create `src/etl/your_data_etl.py`:

```python
class YourDataETL(BaseETL):
    def __init__(self, config: Config):
        # Initialize API client
        api_client = YourAPIClient(config.your_api)
        
        # Initialize Snowflake connector
        snowflake_connector = SnowflakeConnector(config.snowflake)
        
        # Initialize base class
        super().__init__(
            job_name="your_data_etl",
            snowflake_connector=snowflake_connector,
            fmp_client=api_client,
            batch_size=config.app.batch_size,
            enable_monitoring=config.app.enable_monitoring
        )
        
        self.transformer = YourDataTransformer()
    
    def extract(self, symbols: List[str], **kwargs) -> List[Dict[str, Any]]:
        """Extract data from API"""
        all_data = []
        
        for symbol in symbols:
            try:
                data = self.fmp_client.get_options_chain(symbol)
                all_data.extend(data)
                logger.info(f"Extracted {len(data)} records for {symbol}")
            except Exception as e:
                logger.error(f"Failed to extract data for {symbol}: {e}")
                self.result.errors.append(f"Extract failed for {symbol}: {str(e)}")
        
        return all_data
    
    def transform(self, raw_data: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
        """Transform raw data"""
        # Group by symbol for efficient transformation
        symbol_data = {}
        for record in raw_data:
            symbol = record.get('symbol')
            if symbol not in symbol_data:
                symbol_data[symbol] = []
            symbol_data[symbol].append(record)
        
        all_transformed = {'raw': [], 'staging': []}
        
        for symbol, records in symbol_data.items():
            transformed = self.transformer.transform_your_data(symbol, records)
            all_transformed['raw'].extend(transformed['raw'])
            all_transformed['staging'].extend(transformed['staging'])
        
        return all_transformed
    
    def load(self, transformed_data: Dict[str, List[Dict[str, Any]]]) -> int:
        """Load to Snowflake"""
        total_loaded = 0
        
        with self.snowflake as conn:
            # Load RAW layer
            if transformed_data.get('raw'):
                conn.bulk_insert('RAW_DATA.RAW_YOUR_DATA', transformed_data['raw'])
                total_loaded += len(transformed_data['raw'])
                logger.info(f"Loaded {len(transformed_data['raw'])} records to RAW layer")
            
            # Load STAGING layer with MERGE
            if transformed_data.get('staging'):
                conn.merge(
                    table='STAGING.STG_YOUR_DATA',
                    data=transformed_data['staging'],
                    merge_keys=['symbol', 'date', 'strike_price', 'option_type'],
                    update_columns=['bid', 'ask', 'volume', 'open_interest', 'implied_volatility']
                )
                total_loaded += len(transformed_data['staging'])
                logger.info(f"Merged {len(transformed_data['staging'])} records to STAGING layer")
        
        return total_loaded
    
    def update_analytics_layer(self, symbols: List[str]):
        """Update fact tables with calculated metrics"""
        with self.snowflake as conn:
            query = """
            MERGE INTO ANALYTICS.FACT_YOUR_METRICS AS target
            USING (
                SELECT 
                    c.company_key,
                    d.date_key,
                    s.symbol,
                    s.date,
                    -- Calculate your metrics
                    AVG(s.implied_volatility) as avg_implied_vol,
                    SUM(s.volume) as total_volume,
                    SUM(s.open_interest) as total_open_interest
                FROM STAGING.STG_YOUR_DATA s
                JOIN ANALYTICS.DIM_COMPANY c ON s.symbol = c.symbol
                JOIN ANALYTICS.DIM_DATE d ON s.date = d.date
                WHERE s.symbol = ANY(%s)
                GROUP BY c.company_key, d.date_key, s.symbol, s.date
            ) AS source
            ON target.company_key = source.company_key 
               AND target.date_key = source.date_key
            WHEN MATCHED THEN UPDATE SET
                avg_implied_volatility = source.avg_implied_vol,
                total_volume = source.total_volume,
                total_open_interest = source.total_open_interest
            WHEN NOT MATCHED THEN INSERT (
                company_key, date_key,
                avg_implied_volatility, total_volume, total_open_interest
            ) VALUES (
                source.company_key, source.date_key,
                source.avg_implied_vol, source.total_volume, source.total_open_interest
            )
            """
            
            conn.execute(query, (symbols,))
            logger.info(f"Updated analytics layer for {len(symbols)} symbols")
```

### Step 5: Add to Pipeline Orchestrator

Update `scripts/run_daily_pipeline.py`:

```python
def run_your_data_etl(self, symbols: List[str], args) -> bool:
    """Run your new ETL"""
    logger.info("Starting Your Data ETL...")
    
    try:
        etl = YourDataETL(self.config)
        etl.snowflake = self.snowflake  # Share connection
        
        # Extract data
        raw_data = etl.extract(symbols=symbols)
        
        # Transform data
        transformed = etl.transform(raw_data)
        
        # Load data
        records_loaded = etl.load(transformed)
        
        # Update analytics if requested
        if not args.skip_analytics:
            etl.update_analytics_layer(symbols)
        
        logger.info(f"✓ Your Data ETL completed: {records_loaded} records loaded")
        return True
        
    except Exception as e:
        logger.error(f"Your Data ETL failed: {e}")
        return False
```

## Example: Building an Options Data Pipeline

Let's walk through a complete example of adding options data to our system.

### 1. Create Tables

```sql
-- RAW layer
CREATE TABLE RAW_DATA.RAW_OPTIONS_CHAIN (
    id INTEGER IDENTITY,
    symbol VARCHAR(10),
    extract_timestamp TIMESTAMP_NTZ,
    raw_data VARIANT,
    PRIMARY KEY (id)
);

-- STAGING layer
CREATE TABLE STAGING.STG_OPTIONS (
    symbol VARCHAR(10),
    expiration_date DATE,
    strike_price FLOAT,
    option_type VARCHAR(4), -- CALL/PUT
    contract_symbol VARCHAR(30),
    last_price FLOAT,
    bid FLOAT,
    ask FLOAT,
    volume INTEGER,
    open_interest INTEGER,
    implied_volatility FLOAT,
    in_the_money BOOLEAN,
    last_trade_date TIMESTAMP_NTZ,
    loaded_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (contract_symbol)
);

-- ANALYTICS layer
CREATE TABLE ANALYTICS.FACT_OPTIONS_METRICS (
    options_metrics_key INTEGER IDENTITY,
    company_key INTEGER REFERENCES DIM_COMPANY,
    date_key INTEGER REFERENCES DIM_DATE,
    expiration_date_key INTEGER REFERENCES DIM_DATE,
    
    -- Aggregated metrics
    total_call_volume INTEGER,
    total_put_volume INTEGER,
    call_put_ratio FLOAT,
    avg_call_iv FLOAT,
    avg_put_iv FLOAT,
    total_open_interest INTEGER,
    
    -- Calculated fields
    put_call_skew FLOAT,
    iv_rank FLOAT,
    
    created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (options_metrics_key)
);
```

### 2. Implement the Pipeline

See the complete implementation in Step 4 above, adapted for options data.

### 3. Add Custom Calculations

```python
def calculate_options_metrics(self, staging_data: List[Dict]) -> List[Dict]:
    """Calculate options-specific metrics"""
    
    # Group by symbol and date
    grouped = {}
    for record in staging_data:
        key = (record['symbol'], record['date'])
        if key not in grouped:
            grouped[key] = {'calls': [], 'puts': []}
        
        if record['option_type'] == 'CALL':
            grouped[key]['calls'].append(record)
        else:
            grouped[key]['puts'].append(record)
    
    metrics = []
    for (symbol, date), options in grouped.items():
        calls = options['calls']
        puts = options['puts']
        
        # Calculate metrics
        call_volume = sum(c['volume'] for c in calls)
        put_volume = sum(p['volume'] for p in puts)
        
        metrics.append({
            'symbol': symbol,
            'date': date,
            'total_call_volume': call_volume,
            'total_put_volume': put_volume,
            'call_put_ratio': call_volume / put_volume if put_volume > 0 else None,
            'avg_call_iv': np.mean([c['implied_volatility'] for c in calls]) if calls else None,
            'avg_put_iv': np.mean([p['implied_volatility'] for p in puts]) if puts else None,
            'put_call_skew': self.calculate_skew(calls, puts)
        })
    
    return metrics
```

## Best Practices

### 1. Data Quality

Always validate data during transformation:

```python
def validate_record(self, record: Dict) -> Tuple[bool, List[str]]:
    """Validate a single record"""
    issues = []
    
    # Required fields
    if not record.get('symbol'):
        issues.append("Missing symbol")
    
    # Data type validation
    try:
        float(record.get('price', 0))
    except (TypeError, ValueError):
        issues.append(f"Invalid price: {record.get('price')}")
    
    # Business logic validation
    if record.get('volume', 0) < 0:
        issues.append("Negative volume")
    
    return len(issues) == 0, issues
```

### 2. Error Handling

Use the result tracking from BaseETL:

```python
def extract(self, symbols: List[str]) -> List[Dict]:
    all_data = []
    
    for symbol in symbols:
        try:
            data = self.api_client.get_data(symbol)
            all_data.extend(data)
            self.result.records_extracted += len(data)
        except Exception as e:
            logger.error(f"Failed to extract {symbol}: {e}")
            self.result.errors.append(f"{symbol}: {str(e)}")
            # Continue with other symbols
    
    return all_data
```

### 3. Performance Optimization

Use batch operations whenever possible:

```python
# Good - batch insert
self.snowflake.bulk_insert('table', records)

# Bad - individual inserts
for record in records:
    self.snowflake.execute("INSERT INTO table VALUES (%s)", record)
```

### 4. Incremental Loading

Only process new data:

```python
def get_last_load_date(self, symbol: str) -> Optional[date]:
    """Get the last date we loaded for this symbol"""
    query = """
    SELECT MAX(date) as last_date 
    FROM STAGING.STG_YOUR_DATA 
    WHERE symbol = %s
    """
    result = self.snowflake.fetch_one(query, (symbol,))
    return result['last_date'] if result else None

def extract(self, symbols: List[str]) -> List[Dict]:
    all_data = []
    
    for symbol in symbols:
        # Only get new data
        last_date = self.get_last_load_date(symbol)
        start_date = last_date + timedelta(days=1) if last_date else date(2020, 1, 1)
        
        data = self.api_client.get_data(symbol, start_date=start_date)
        all_data.extend(data)
```

### 5. Connection Management

Reuse connections for better performance:

```python
class YourDataETL(BaseETL):
    def run(self, symbols: List[str]) -> Dict[str, Any]:
        """Run complete ETL with connection reuse"""
        
        # Use context manager for automatic cleanup
        with self.snowflake as conn:
            # Extract
            raw_data = self.extract(symbols)
            
            # Transform
            transformed = self.transform(raw_data)
            
            # Load (connection is already open)
            records_loaded = self.load(transformed)
            
            # Update analytics (same connection)
            self.update_analytics_layer(symbols)
        
        return {'status': 'success', 'records': records_loaded}
```

## Common Pitfalls

### 1. VARIANT Column Performance

**Problem**: Bulk inserts timeout with VARIANT columns.

**Solution**: Use our optimized SnowflakeConnector with pandas:
```python
# This is handled automatically by our connector
self.snowflake.bulk_insert('RAW_DATA.YOUR_TABLE', data)
```

### 2. Duplicate Data

**Problem**: Running ETL twice creates duplicates.

**Solution**: Always use MERGE for staging tables:
```python
self.snowflake.merge(
    table='STAGING.STG_YOUR_TABLE',
    data=data,
    merge_keys=['symbol', 'date'],  # Your unique keys
    update_columns=['price', 'volume']  # Columns to update
)
```

### 3. Time Zone Issues

**Problem**: Mixing time zones causes data misalignment.

**Solution**: Always use UTC:
```python
from datetime import datetime, timezone

# Good
timestamp = datetime.now(timezone.utc)

# Bad
timestamp = datetime.now()
```

### 4. Memory Issues with Large Datasets

**Problem**: Loading millions of records causes memory errors.

**Solution**: Process in batches:
```python
def load_in_batches(self, data: List[Dict], batch_size: int = 10000):
    """Load data in batches to avoid memory issues"""
    
    for i in range(0, len(data), batch_size):
        batch = data[i:i + batch_size]
        self.snowflake.bulk_insert('table', batch)
        logger.info(f"Loaded batch {i//batch_size + 1}")
```

### 5. API Rate Limits

**Problem**: Hitting rate limits causes failures.

**Solution**: Use BaseAPIClient with built-in rate limiting:
```python
class YourAPIClient(BaseAPIClient):
    def __init__(self, config):
        super().__init__(
            rate_limit_calls=100,  # 100 calls
            rate_limit_period=60   # per 60 seconds
        )
```

## Testing Your Pipeline

### 1. Unit Tests

Test each component in isolation:

```python
def test_transformer():
    """Test data transformation logic"""
    transformer = YourDataTransformer()
    
    # Mock input
    raw_data = [{'symbol': 'TEST', 'value': '100.5'}]
    
    # Transform
    result = transformer.transform_your_data('TEST', raw_data)
    
    # Verify
    assert len(result['staging']) == 1
    assert result['staging'][0]['value'] == 100.5
```

### 2. Integration Tests

Test the full pipeline with test data:

```python
def test_full_pipeline():
    """Test complete ETL flow"""
    config = Config()  # Test config
    etl = YourDataETL(config)
    
    # Run with test symbol
    result = etl.run(['TEST'])
    
    # Verify success
    assert result['status'] == 'success'
    assert result['records'] > 0
```

### 3. Data Validation Tests

Verify data quality:

```python
def test_data_in_snowflake():
    """Verify data was loaded correctly"""
    
    query = """
    SELECT COUNT(*) as count 
    FROM STAGING.STG_YOUR_DATA 
    WHERE symbol = 'TEST'
    """
    
    result = snowflake.fetch_one(query)
    assert result['count'] > 0
```

## Performance Optimization

### 1. Query Optimization

Use CTEs for complex queries:

```python
query = """
WITH daily_metrics AS (
    -- First, aggregate at daily level
    SELECT 
        symbol,
        date,
        SUM(volume) as daily_volume,
        AVG(implied_volatility) as avg_iv
    FROM STAGING.STG_OPTIONS
    GROUP BY symbol, date
),
ranked_metrics AS (
    -- Then calculate rankings
    SELECT 
        *,
        PERCENT_RANK() OVER (PARTITION BY symbol ORDER BY avg_iv) as iv_rank
    FROM daily_metrics
)
SELECT * FROM ranked_metrics WHERE symbol = %s
"""
```

### 2. Parallel Processing

Process multiple symbols concurrently:

```python
from concurrent.futures import ThreadPoolExecutor

def process_symbols_parallel(self, symbols: List[str]):
    """Process multiple symbols in parallel"""
    
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = []
        
        for symbol in symbols:
            future = executor.submit(self.process_single_symbol, symbol)
            futures.append(future)
        
        # Collect results
        results = []
        for future in futures:
            try:
                result = future.result()
                results.append(result)
            except Exception as e:
                logger.error(f"Failed to process symbol: {e}")
    
    return results
```

### 3. Batch API Calls

If your API supports batch endpoints:

```python
def extract_batch(self, symbols: List[str]) -> List[Dict]:
    """Extract multiple symbols in one API call"""
    
    # Check if batch endpoint is available
    if hasattr(self.api_client, 'get_batch_data'):
        return self.api_client.get_batch_data(symbols)
    else:
        # Fall back to individual calls
        return self.extract_individual(symbols)
```

### 4. Caching for Expensive Calculations

Cache calculated values:

```python
from functools import lru_cache

@lru_cache(maxsize=1000)
def calculate_complex_metric(self, symbol: str, date: date) -> float:
    """Cache expensive calculations"""
    # This result will be cached
    return self._perform_expensive_calculation(symbol, date)
```

## Conclusion

This cookbook provides the foundation for extending the Financial Data Service with new data sources. The key principles are:

1. **Follow the three-layer architecture** (RAW → STAGING → ANALYTICS)
2. **Use the BaseETL framework** for consistency
3. **Implement proper error handling** and monitoring
4. **Optimize for performance** from the start
5. **Write tests** for your pipeline

Remember: The patterns we've established handle most edge cases and performance issues. When in doubt, look at existing ETL implementations like `HistoricalPriceETL` or `FinancialStatementETL` for reference.

Happy coding! 🚀