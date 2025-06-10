# Financial Data Service Performance Optimization Plan

## Overview

This document outlines a detailed implementation plan to optimize the financial data service ETL pipeline performance. The goal is to reduce processing time for the S&P 500 from 18+ hours to under 3 hours through systematic improvements.

## Current State Analysis

### Performance Baseline (Single Symbol - AAPL)
- Company ETL: 7 seconds
- Price ETL (30 days): 34 seconds  
- Financial ETL (Annual): 43 seconds
- Financial ETL (Quarterly): 19 seconds
- TTM Calculation: 18 seconds
- Financial Ratios: 2 seconds
- Market Metrics: 8 seconds
- **Total: 131 seconds per symbol**

### Scaling Profile
- Linear scaling: O(n) where n = number of symbols
- No parallelization utilized
- Database connection overhead compounds with scale

## Implementation Phases

## Phase 1: Foundation & Quick Wins
**Timeline**: 1-2 days  
**Expected Improvement**: 15-20%  
**Risk**: Low

### 1.1 Fix Analytics Layer Updates ✅ (COMPLETED)
- Added missing `update_fact_table()` calls in `run_daily_pipeline.py`
- Ensures FACT_FINANCIALS and FACT_DAILY_PRICES are properly populated

### 1.2 Configuration Updates
```python
# config/pipeline_config.py
PIPELINE_CONFIG = {
    'timeout_seconds': 600,  # Increase from 120 to 600
    'batch_size': 50,        # For API calls
    'db_pool_size': 5,       # Connection pool size
    'max_workers': 4,        # Parallel execution threads
    'cache_ttl': {
        'company_profiles': 86400,    # 24 hours
        'financial_statements': 21600, # 6 hours
        'price_data': 900             # 15 minutes
    }
}
```

### 1.3 Performance Logging
```python
# src/monitoring/performance_logger.py
import time
from functools import wraps
from loguru import logger

def log_execution_time(operation_name: str):
    """Decorator to log execution time of functions"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            start_time = time.time()
            try:
                result = func(*args, **kwargs)
                duration = time.time() - start_time
                logger.info(f"{operation_name} completed in {duration:.2f}s")
                return result
            except Exception as e:
                duration = time.time() - start_time
                logger.error(f"{operation_name} failed after {duration:.2f}s: {e}")
                raise
        return wrapper
    return decorator
```

### 1.4 Basic Connection Reuse
```python
# src/etl/pipeline_context.py
class PipelineContext:
    """Shared context for pipeline execution"""
    def __init__(self, config: Config):
        self.config = config
        self._connection = None
        self._fmp_client = None
    
    @property
    def connection(self):
        if not self._connection:
            self._connection = SnowflakeConnector(self.config.snowflake)
        return self._connection
    
    @property
    def fmp_client(self):
        if not self._fmp_client:
            self._fmp_client = FMPClient(self.config.fmp)
        return self._fmp_client
    
    def cleanup(self):
        if self._connection:
            self._connection.close()
```

## Phase 2: Core Performance Improvements
**Timeline**: 1-2 weeks  
**Expected Improvement**: 60-70% total  
**Risk**: Medium

### 2.1 Connection Pool Implementation
```python
# src/db/connection_pool.py
from snowflake.connector import pooling
from typing import Optional
import threading

class SnowflakeConnectionPool:
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls, config: SnowflakeConfig, pool_size: int = 5):
        if not cls._instance:
            with cls._lock:
                if not cls._instance:
                    cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self, config: SnowflakeConfig, pool_size: int = 5):
        if not hasattr(self, 'initialized'):
            self.pool = pooling.create_pool(
                pool_name='financial_etl_pool',
                pool_size=pool_size,
                pool_validation_timeout=30,
                account=config.account,
                user=config.user,
                password=config.password,
                warehouse=config.warehouse,
                database=config.database,
                schema=config.schema,
                role=config.role,
                session_parameters={
                    'QUERY_TAG': 'financial_data_etl',
                    'AUTOCOMMIT': True
                }
            )
            self.initialized = True
    
    def get_connection(self):
        """Get a connection from the pool"""
        return self.pool.get_connection()
    
    def return_connection(self, connection):
        """Return a connection to the pool"""
        if connection and not connection.is_closed():
            connection.close()  # Pool handles the actual return
```

### 2.2 Parallel ETL Orchestrator
```python
# src/etl/parallel_orchestrator.py
import concurrent.futures
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
import time

@dataclass
class ETLTask:
    name: str
    etl_class: type
    dependencies: List[str] = None
    params: Dict[str, Any] = None

class ParallelOrchestrator:
    def __init__(self, config: Config, max_workers: int = 4):
        self.config = config
        self.max_workers = max_workers
        self.connection_pool = SnowflakeConnectionPool(config.snowflake)
        self.results = {}
    
    def run_parallel_pipeline(self, symbols: List[str], args) -> Dict[str, Any]:
        """
        Execute ETL pipeline with parallel processing where possible
        """
        start_time = time.time()
        
        # Define ETL tasks and dependencies
        tasks = [
            ETLTask('company', CompanyETL, dependencies=[]),
            ETLTask('price', HistoricalPriceETL, dependencies=[], 
                   params={'from_date': args.from_date, 'to_date': args.to_date}),
            ETLTask('financial_annual', FinancialStatementETL, dependencies=[],
                   params={'period': 'annual', 'limit': args.limit}),
            ETLTask('financial_quarterly', FinancialStatementETL, dependencies=[],
                   params={'period': 'quarterly', 'limit': args.limit * 2}),
            ETLTask('ttm', TTMCalculationETL, 
                   dependencies=['financial_quarterly']),
            ETLTask('ratios', FinancialRatioETL, 
                   dependencies=['financial_annual', 'financial_quarterly']),
            ETLTask('metrics', MarketMetricsETL,
                   dependencies=['price', 'ttm'])
        ]
        
        # Group tasks by dependency level
        task_groups = self._group_tasks_by_dependency(tasks)
        
        # Execute each group in parallel
        for group_level, group_tasks in sorted(task_groups.items()):
            logger.info(f"Executing dependency level {group_level}: "
                       f"{[t.name for t in group_tasks]}")
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                futures = {}
                
                for task in group_tasks:
                    future = executor.submit(
                        self._run_etl_task,
                        task,
                        symbols,
                        args
                    )
                    futures[future] = task.name
                
                # Wait for all tasks in group to complete
                for future in concurrent.futures.as_completed(futures):
                    task_name = futures[future]
                    try:
                        result = future.result()
                        self.results[task_name] = result
                        logger.info(f"✓ {task_name} completed successfully")
                    except Exception as e:
                        logger.error(f"✗ {task_name} failed: {e}")
                        self.results[task_name] = {
                            'status': 'failed',
                            'error': str(e)
                        }
        
        total_time = time.time() - start_time
        logger.info(f"Pipeline completed in {total_time:.2f}s")
        
        return self.results
    
    def _run_etl_task(self, task: ETLTask, symbols: List[str], args) -> Dict[str, Any]:
        """Execute a single ETL task"""
        logger.info(f"Starting {task.name} ETL")
        
        # Get connection from pool
        connection = self.connection_pool.get_connection()
        
        try:
            # Initialize ETL with pooled connection
            etl = task.etl_class(self.config)
            etl.snowflake = connection
            
            # Run ETL based on task type
            if task.name == 'company':
                result = etl.run(symbols=symbols)
            elif task.name.startswith('financial'):
                result = etl.run(
                    symbols=symbols,
                    period=task.params['period'],
                    limit=task.params['limit'],
                    update_analytics=not args.skip_analytics
                )
            elif task.name == 'price':
                result = etl.run(
                    symbols=symbols,
                    from_date=task.params['from_date'],
                    to_date=task.params['to_date'],
                    update_analytics=not args.skip_analytics
                )
            else:
                result = etl.run(symbols=symbols)
            
            return {
                'status': result.status.value,
                'records_loaded': result.records_loaded,
                'duration': (result.end_time - result.start_time).total_seconds()
            }
            
        finally:
            # Return connection to pool
            self.connection_pool.return_connection(connection)
    
    def _group_tasks_by_dependency(self, tasks: List[ETLTask]) -> Dict[int, List[ETLTask]]:
        """Group tasks by dependency level for parallel execution"""
        task_levels = {}
        task_map = {t.name: t for t in tasks}
        
        def get_level(task_name: str, visited=None) -> int:
            if visited is None:
                visited = set()
            
            if task_name in visited:
                raise ValueError(f"Circular dependency detected: {task_name}")
            
            visited.add(task_name)
            task = task_map[task_name]
            
            if not task.dependencies:
                return 0
            
            max_dep_level = 0
            for dep in task.dependencies:
                if dep in task_map:
                    dep_level = get_level(dep, visited.copy())
                    max_dep_level = max(max_dep_level, dep_level)
            
            return max_dep_level + 1
        
        # Calculate levels
        groups = {}
        for task in tasks:
            level = get_level(task.name)
            if level not in groups:
                groups[level] = []
            groups[level].append(task)
        
        return groups
```

### 2.3 Batch API Client
```python
# src/api/fmp_batch_client.py
from typing import List, Dict, Any
import asyncio
import aiohttp
from loguru import logger

class FMPBatchClient(FMPClient):
    """Enhanced FMP client with batch operations"""
    
    def __init__(self, config: FMPConfig):
        super().__init__(config)
        self.batch_endpoints = {
            'company_profile': '/profile',
            'income_statement': '/income-statement',
            'balance_sheet': '/balance-sheet',
            'cash_flow': '/cash-flow-statement'
        }
    
    def get_batch_company_profiles(self, symbols: List[str]) -> Dict[str, Dict]:
        """Get company profiles for multiple symbols in one call"""
        batch_size = 100  # FMP allows up to 100 symbols
        all_profiles = {}
        
        for i in range(0, len(symbols), batch_size):
            batch = symbols[i:i + batch_size]
            symbol_str = ','.join(batch)
            
            endpoint = f"/profile/{symbol_str}"
            data = self._make_request(endpoint)
            
            if data:
                for profile in data:
                    all_profiles[profile['symbol']] = profile
        
        logger.info(f"Retrieved {len(all_profiles)} company profiles")
        return all_profiles
    
    async def get_batch_financial_statements_async(
        self, 
        symbols: List[str],
        statement_type: str,
        period: str = 'annual',
        limit: int = 5
    ) -> Dict[str, List[Dict]]:
        """Asynchronously get financial statements for multiple symbols"""
        
        async def fetch_statement(session, symbol):
            endpoint = f"{self.batch_endpoints[statement_type]}/{symbol}"
            params = {'period': period, 'limit': limit, 'apikey': self.api_key}
            
            try:
                async with session.get(
                    f"{self.base_url}{endpoint}",
                    params=params
                ) as response:
                    if response.status == 200:
                        data = await response.json()
                        return symbol, data
                    else:
                        logger.warning(f"Failed to fetch {statement_type} "
                                     f"for {symbol}: {response.status}")
                        return symbol, None
            except Exception as e:
                logger.error(f"Error fetching {statement_type} for {symbol}: {e}")
                return symbol, None
        
        # Create async session and fetch all
        async with aiohttp.ClientSession() as session:
            tasks = [fetch_statement(session, symbol) for symbol in symbols]
            results = await asyncio.gather(*tasks)
        
        # Convert to dict
        return {symbol: data for symbol, data in results if data}
    
    def get_batch_financial_statements(
        self,
        symbols: List[str],
        statement_type: str,
        period: str = 'annual',
        limit: int = 5
    ) -> Dict[str, List[Dict]]:
        """Synchronous wrapper for async batch fetch"""
        return asyncio.run(
            self.get_batch_financial_statements_async(
                symbols, statement_type, period, limit
            )
        )
```

## Phase 3: Advanced Optimizations
**Timeline**: 2-3 weeks  
**Expected Improvement**: 80-86% total  
**Risk**: Medium-High

### 3.1 Bulk VARIANT Loading
```python
# src/db/bulk_variant_loader.py
import uuid
import json
import tempfile
import os
from typing import List, Dict, Any

class BulkVariantLoader:
    """Efficient bulk loading for VARIANT columns"""
    
    def __init__(self, connection_pool: SnowflakeConnectionPool):
        self.pool = connection_pool
    
    def bulk_load_variant(self, 
                         table: str, 
                         data: List[Dict[str, Any]],
                         batch_size: int = 10000) -> int:
        """
        Load VARIANT data using COPY command for better performance
        """
        if not data:
            return 0
        
        total_loaded = 0
        
        # Process in batches
        for i in range(0, len(data), batch_size):
            batch = data[i:i + batch_size]
            loaded = self._load_batch(table, batch)
            total_loaded += loaded
        
        logger.info(f"Bulk loaded {total_loaded} records to {table}")
        return total_loaded
    
    def _load_batch(self, table: str, batch: List[Dict]) -> int:
        """Load a single batch using COPY INTO"""
        connection = self.pool.get_connection()
        
        try:
            # Create temporary file
            with tempfile.NamedTemporaryFile(
                mode='w',
                suffix='.json',
                delete=False
            ) as tmp_file:
                # Write NDJSON (newline-delimited JSON)
                for record in batch:
                    json.dump(record, tmp_file, cls=DateTimeEncoder)
                    tmp_file.write('\n')
                
                tmp_path = tmp_file.name
            
            # Create temporary stage
            stage_name = f"TEMP_STAGE_{uuid.uuid4().hex[:8]}"
            connection.cursor().execute(
                f"CREATE TEMPORARY STAGE {stage_name} "
                f"FILE_FORMAT = (TYPE = JSON)"
            )
            
            # Upload file to stage
            connection.cursor().execute(
                f"PUT file://{tmp_path} @{stage_name} AUTO_COMPRESS=TRUE"
            )
            
            # Copy into table
            result = connection.cursor().execute(f"""
                COPY INTO {table}
                FROM @{stage_name}
                FILE_FORMAT = (TYPE = JSON)
                MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
                ON_ERROR = CONTINUE
                RETURN_FAILED_ONLY = FALSE
            """).fetchone()
            
            rows_loaded = result[0] if result else 0
            
            # Cleanup
            connection.cursor().execute(f"DROP STAGE {stage_name}")
            os.unlink(tmp_path)
            
            return rows_loaded
            
        finally:
            self.pool.return_connection(connection)
```

### 3.2 Intelligent Caching Layer
```python
# src/cache/etl_cache.py
import redis
import json
import hashlib
from datetime import datetime, timedelta
from typing import Any, Optional, Callable
from functools import wraps

class ETLCache:
    """Redis-based caching for ETL operations"""
    
    def __init__(self, redis_url: str = 'redis://localhost:6379/0'):
        self.redis = redis.from_url(redis_url, decode_responses=True)
        self.default_ttl = {
            'company_profile': timedelta(days=1),
            'financial_statement': timedelta(hours=6),
            'price_data': timedelta(minutes=15),
            'calculated_metrics': timedelta(hours=1)
        }
    
    def cache_key(self, prefix: str, **kwargs) -> str:
        """Generate cache key from prefix and parameters"""
        # Sort kwargs for consistent keys
        sorted_params = sorted(kwargs.items())
        param_str = json.dumps(sorted_params, sort_keys=True)
        param_hash = hashlib.md5(param_str.encode()).hexdigest()[:8]
        return f"{prefix}:{param_hash}"
    
    def get(self, key: str) -> Optional[Any]:
        """Get value from cache"""
        value = self.redis.get(key)
        if value:
            return json.loads(value)
        return None
    
    def set(self, key: str, value: Any, ttl: Optional[timedelta] = None):
        """Set value in cache with TTL"""
        if ttl is None:
            ttl = timedelta(hours=1)
        
        self.redis.setex(
            key,
            int(ttl.total_seconds()),
            json.dumps(value, cls=DateTimeEncoder)
        )
    
    def cached(self, cache_type: str, key_params: List[str]):
        """Decorator for caching function results"""
        def decorator(func: Callable):
            @wraps(func)
            def wrapper(*args, **kwargs):
                # Build cache key from specified parameters
                cache_params = {k: kwargs.get(k) for k in key_params if k in kwargs}
                cache_key = self.cache_key(cache_type, **cache_params)
                
                # Check cache
                cached_value = self.get(cache_key)
                if cached_value is not None:
                    logger.debug(f"Cache hit for {cache_key}")
                    return cached_value
                
                # Call function
                result = func(*args, **kwargs)
                
                # Cache result
                ttl = self.default_ttl.get(cache_type, timedelta(hours=1))
                self.set(cache_key, result, ttl)
                logger.debug(f"Cached result for {cache_key}")
                
                return result
            return wrapper
        return decorator
    
    def invalidate_pattern(self, pattern: str):
        """Invalidate all keys matching pattern"""
        cursor = 0
        while True:
            cursor, keys = self.redis.scan(cursor, match=pattern, count=100)
            if keys:
                self.redis.delete(*keys)
            if cursor == 0:
                break

# Usage example:
class CachedFMPClient(FMPClient):
    def __init__(self, config: FMPConfig):
        super().__init__(config)
        self.cache = ETLCache()
    
    @ETLCache.cached('company_profile', ['symbol'])
    def get_company_profile(self, symbol: str) -> Dict:
        """Get company profile with caching"""
        return super().get_company_profile(symbol)
```

### 3.3 Query Optimization Scripts
```sql
-- sql/performance_optimizations.sql

-- 1. Add indexes for common query patterns
CREATE OR REPLACE INDEX idx_fact_financials_lookup 
ON ANALYTICS.FACT_FINANCIALS(company_key, fiscal_date_key, period_type)
CLUSTER BY (fiscal_date_key);

CREATE OR REPLACE INDEX idx_fact_prices_lookup
ON ANALYTICS.FACT_DAILY_PRICES(company_key, date_key)
CLUSTER BY (date_key);

CREATE OR REPLACE INDEX idx_fact_ttm_lookup
ON ANALYTICS.FACT_FINANCIALS_TTM(company_key, fiscal_date_key)
CLUSTER BY (fiscal_date_key);

-- 2. Create materialized views for common queries
CREATE OR REPLACE MATERIALIZED VIEW ANALYTICS.MV_LATEST_FINANCIALS AS
SELECT 
    f.*,
    ROW_NUMBER() OVER (
        PARTITION BY f.company_key, f.period_type 
        ORDER BY f.fiscal_date_key DESC
    ) as rn
FROM ANALYTICS.FACT_FINANCIALS f
WHERE f.fiscal_date_key >= (
    SELECT date_key 
    FROM ANALYTICS.DIM_DATE 
    WHERE date = DATEADD(year, -2, CURRENT_DATE())
);

-- 3. Optimize merge operations with temporary tables
CREATE OR REPLACE PROCEDURE ANALYTICS.SP_OPTIMIZED_MERGE(
    target_table STRING,
    source_data VARIANT,
    merge_keys ARRAY
)
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    -- Create temporary table for staging
    LET temp_table := target_table || '_TEMP_' || TO_VARCHAR(CURRENT_TIMESTAMP(), 'YYYYMMDDHH24MISS');
    
    CREATE TEMPORARY TABLE IDENTIFIER(:temp_table) AS
    SELECT * FROM TABLE(FLATTEN(input => :source_data));
    
    -- Perform merge
    LET merge_sql := 'MERGE INTO ' || target_table || ' t USING ' || temp_table || ' s';
    -- ... build merge statement dynamically ...
    
    EXECUTE IMMEDIATE :merge_sql;
    
    RETURN 'Merge completed successfully';
END;
$$;

-- 4. Enable automatic clustering
ALTER TABLE ANALYTICS.FACT_DAILY_PRICES 
RESUME RECLUSTER;

ALTER TABLE ANALYTICS.FACT_FINANCIALS 
RESUME RECLUSTER;
```

## Phase 4: Architecture Evolution
**Timeline**: 1 month  
**Expected Improvement**: 90%+ total  
**Risk**: High

### 4.1 Workflow Orchestration with Airflow
```python
# dags/financial_data_pipeline.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email': ['data-alerts@company.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'financial_data_pipeline',
    default_args=default_args,
    description='Daily financial data ETL pipeline',
    schedule_interval='0 6 * * *',  # 6 AM daily
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['financial', 'etl', 'production']
)

# Symbol selection task
def get_symbols(**context):
    """Get list of symbols to process"""
    # Could pull from database or API
    symbols = get_sp500_symbols()
    context['task_instance'].xcom_push(key='symbols', value=symbols)
    return len(symbols)

symbol_task = PythonOperator(
    task_id='get_symbols',
    python_callable=get_symbols,
    dag=dag
)

# Parallel ETL tasks
with TaskGroup('extract_transform_load', dag=dag) as etl_group:
    
    # Company profiles
    company_etl = PythonOperator(
        task_id='company_profiles',
        python_callable=run_company_etl,
        op_kwargs={'use_cache': True},
        pool='etl_pool',
        dag=dag
    )
    
    # Historical prices
    price_etl = PythonOperator(
        task_id='historical_prices',
        python_callable=run_price_etl,
        op_kwargs={'days_back': 30},
        pool='etl_pool',
        dag=dag
    )
    
    # Financial statements
    with TaskGroup('financial_statements') as financial_group:
        annual_etl = PythonOperator(
            task_id='annual_statements',
            python_callable=run_financial_etl,
            op_kwargs={'period': 'annual', 'limit': 5},
            pool='etl_pool',
            dag=dag
        )
        
        quarterly_etl = PythonOperator(
            task_id='quarterly_statements',
            python_callable=run_financial_etl,
            op_kwargs={'period': 'quarterly', 'limit': 8},
            pool='etl_pool',
            dag=dag
        )

# Dependent calculations
ttm_calc = PythonOperator(
    task_id='ttm_calculations',
    python_callable=run_ttm_calculations,
    dag=dag
)

ratio_calc = PythonOperator(
    task_id='financial_ratios',
    python_callable=run_ratio_calculations,
    dag=dag
)

metrics_calc = PythonOperator(
    task_id='market_metrics',
    python_callable=run_market_metrics,
    dag=dag
)

# Data quality checks
quality_check = BashOperator(
    task_id='data_quality_check',
    bash_command='python /opt/airflow/scripts/data_quality_check.py',
    dag=dag
)

# Define dependencies
symbol_task >> etl_group
etl_group >> ttm_calc >> [ratio_calc, metrics_calc] >> quality_check
```

### 4.2 Real-time Streaming Architecture
```python
# src/streaming/price_stream_processor.py
from kafka import KafkaConsumer, KafkaProducer
from typing import Dict, Any
import json
import time

class PriceStreamProcessor:
    """Process real-time price updates"""
    
    def __init__(self, config: Config):
        self.config = config
        self.consumer = KafkaConsumer(
            'market-prices',
            bootstrap_servers=config.kafka.brokers,
            auto_offset_reset='latest',
            enable_auto_commit=True,
            group_id='price-processor-group',
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )
        
        self.producer = KafkaProducer(
            bootstrap_servers=config.kafka.brokers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        
        self.batch = []
        self.last_flush = time.time()
        self.batch_size = 1000
        self.flush_interval = 30  # seconds
    
    def process_stream(self):
        """Main processing loop"""
        logger.info("Starting price stream processor")
        
        for message in self.consumer:
            try:
                price_data = message.value
                
                # Enrich with additional data
                enriched = self.enrich_price_data(price_data)
                
                # Add to batch
                self.batch.append(enriched)
                
                # Check if we should flush
                if self.should_flush():
                    self.flush_batch()
                    
            except Exception as e:
                logger.error(f"Error processing message: {e}")
                self.producer.send('dead-letter-queue', message.value)
    
    def should_flush(self) -> bool:
        """Check if batch should be flushed"""
        return (
            len(self.batch) >= self.batch_size or
            time.time() - self.last_flush > self.flush_interval
        )
    
    def flush_batch(self):
        """Write batch to Snowflake"""
        if not self.batch:
            return
        
        try:
            # Use bulk loader for efficiency
            with BulkVariantLoader(self.config) as loader:
                loaded = loader.bulk_load_variant(
                    'STAGING.STG_REALTIME_PRICES',
                    self.batch
                )
            
            logger.info(f"Flushed {loaded} price records to Snowflake")
            
            # Send to metrics calculation topic
            self.producer.send(
                'calculate-metrics',
                {'symbols': list(set(p['symbol'] for p in self.batch))}
            )
            
            # Clear batch
            self.batch = []
            self.last_flush = time.time()
            
        except Exception as e:
            logger.error(f"Failed to flush batch: {e}")
            # Could implement retry logic or dead letter queue
```

## Implementation Timeline

### Week 1-2: Foundation
- [ ] Set up performance monitoring
- [ ] Implement basic connection pooling
- [ ] Add batch API endpoints
- [ ] Deploy Phase 1 optimizations

### Week 3-4: Core Performance
- [ ] Implement parallel orchestrator
- [ ] Deploy connection pool across all ETLs
- [ ] Add async API operations
- [ ] Test with subset of S&P 500

### Week 5-6: Advanced Features
- [ ] Deploy Redis cache
- [ ] Implement bulk VARIANT loader
- [ ] Add query optimizations
- [ ] Full S&P 500 testing

### Week 7-8: Architecture
- [ ] Set up Airflow
- [ ] Migrate pipelines to DAGs
- [ ] Add monitoring dashboards
- [ ] Performance validation

## Success Metrics

### Performance KPIs
- **Pipeline Duration**: < 3 hours for S&P 500
- **API Efficiency**: < 3 calls per symbol
- **Database Connections**: < 10 concurrent
- **Memory Usage**: < 8GB peak
- **Error Rate**: < 0.1%

### Monitoring Dashboard
```python
# src/monitoring/performance_dashboard.py
class PerformanceDashboard:
    """Track ETL performance metrics"""
    
    def __init__(self, config: Config):
        self.config = config
        self.metrics_db = MetricsDatabase(config)
    
    def record_pipeline_run(self, pipeline_id: str, metrics: Dict[str, Any]):
        """Record pipeline execution metrics"""
        self.metrics_db.insert({
            'pipeline_id': pipeline_id,
            'timestamp': datetime.utcnow(),
            'duration_seconds': metrics['duration'],
            'symbols_processed': metrics['symbols_count'],
            'records_loaded': metrics['records_loaded'],
            'api_calls': metrics['api_calls'],
            'db_connections_peak': metrics['db_connections'],
            'memory_usage_mb': metrics['memory_mb'],
            'errors': metrics['errors']
        })
    
    def generate_report(self, days: int = 7) -> Dict[str, Any]:
        """Generate performance report"""
        data = self.metrics_db.get_recent(days)
        
        return {
            'avg_duration': sum(d['duration_seconds'] for d in data) / len(data),
            'total_symbols': sum(d['symbols_processed'] for d in data),
            'success_rate': sum(1 for d in data if not d['errors']) / len(data),
            'throughput': sum(d['records_loaded'] for d in data) / 
                         sum(d['duration_seconds'] for d in data)
        }
```

## Risk Mitigation

### Technical Risks
1. **Connection Pool Exhaustion**
   - Monitor pool usage
   - Implement circuit breakers
   - Auto-scale pool size

2. **Memory Overflow**
   - Batch size limits
   - Streaming processing for large datasets
   - Memory monitoring alerts

3. **API Rate Limits**
   - Implement backoff strategies
   - Request queuing
   - Multiple API keys rotation

### Operational Risks
1. **Data Consistency**
   - Transaction management
   - Rollback procedures
   - Data validation checks

2. **Performance Degradation**
   - Continuous monitoring
   - Automated alerts
   - Rollback plan for each phase

## Conclusion

This optimization plan provides a structured approach to improving ETL pipeline performance from 18+ hours to under 3 hours for S&P 500 processing. The phased implementation allows for:

1. **Incremental improvements** with measurable results
2. **Risk mitigation** through gradual changes
3. **Flexibility** to adjust based on real-world performance
4. **Scalability** for future growth

Each phase builds upon the previous, creating a robust, performant data pipeline capable of handling increasing data volumes and real-time requirements.