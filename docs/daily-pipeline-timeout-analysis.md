# Daily Pipeline Timeout Analysis

## Issue Summary
The `run_daily_pipeline.py` script was timing out after 2 minutes when processing multiple symbols.

## Root Causes Identified

### 1. Missing Analytics Layer Updates
**Critical Issue**: The daily pipeline is not updating the analytics layer (fact tables) properly.

- Individual ETL scripts (e.g., `run_financial_etl.py`) call the `run()` method which includes analytics updates
- Daily pipeline calls `extract()`, `transform()`, and `load()` separately but never calls:
  - `update_fact_table()` for financial data → FACT_FINANCIALS
  - `update_fact_table()` for price data → FACT_DAILY_PRICES
  - Analytics layer updates for DIM_COMPANY

This means staging tables get populated but fact tables remain empty, causing downstream ETLs to have no data to process.

### 2. Sequential Processing Takes Too Long
Timing analysis for single symbol (AAPL):
- Company ETL: ~7 seconds
- Price ETL: ~34 seconds
- Financial ETL Annual: ~43 seconds
- Financial ETL Quarterly: ~19 seconds
- TTM Calculation: ~18 seconds
- Ratio ETL: ~2 seconds
- Market Metrics: ~8 seconds

**Total: ~131 seconds for one symbol**

With 3 symbols, this could easily exceed 2-3 minutes.

### 3. Snowflake Connection Overhead
Each ETL creates multiple connections:
- Initial connection for extract
- New connection for load
- Another connection for analytics update
- Connection for monitoring

This adds significant overhead.

## Recommendations

### Immediate Fixes

1. **Fix Analytics Updates in Daily Pipeline**
   ```python
   # After load() in each ETL method:
   if not args.skip_analytics:
       etl.update_fact_table(symbols)  # For financial ETL
       # Or call the full run() method instead
   ```

2. **Increase Timeout**
   - Default timeout is 120 seconds (2 minutes)
   - Should be at least 300 seconds (5 minutes) for multi-symbol runs

### Performance Optimizations

1. **Batch Processing**
   - Process multiple symbols in single API calls where possible
   - Use batch endpoints for company profiles

2. **Connection Pooling**
   - Reuse Snowflake connections across ETL steps
   - Pass connection to ETL classes instead of creating new ones

3. **Parallel Processing**
   - Run independent ETLs in parallel (company, price, financial)
   - Only serialize dependent ETLs (TTM needs financial, ratios need financial, market metrics needs all)

4. **Skip Unchanged Data**
   - Check if company profiles have changed before reloading
   - Only load new price data (not historical)
   - Skip financial data that hasn't been updated

5. **Optimize Queries**
   - The MERGE operations are slow
   - Consider bulk operations for better performance

## Code Comparison

### Individual Script (Working)
```python
# run_financial_etl.py
result = etl.run(
    symbols=batch_symbols,
    period=args.period,
    limit=args.limit,
    update_analytics=not args.skip_analytics  # ← Updates FACT_FINANCIALS
)
```

### Daily Pipeline (Broken)
```python
# run_daily_pipeline.py
statement_data = etl.extract(symbols=symbols, period=args.period, limit=args.limit)
transformed_data = etl.transform(statement_data)
records_loaded = etl.load(transformed_data)
# Missing: etl.update_fact_table(symbols) or analytics update!
```

## Impact
Without analytics updates:
- FACT_FINANCIALS remains empty
- FACT_DAILY_PRICES remains empty
- TTM calculations have no data
- Ratio calculations have no data
- Market metrics have no data

This explains why the pipeline appears to "hang" - it's processing but finding no data in the fact tables.