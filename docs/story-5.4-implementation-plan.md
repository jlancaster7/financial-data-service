# Story 5.4: Refactor Market Metrics to Use Pre-calculated Values - Implementation Plan

**Story Points:** 3  
**Status:** 📋 TODO  
**Dependencies:** Story 5.3 (TTM Calculations) ✅ COMPLETED

## Overview
Refactor the market metrics ETL to use pre-calculated TTM values from FACT_FINANCIALS_TTM and add revenue_per_share to financial ratios. This will simplify the code, improve performance, and ensure we use official reported values.

## Goals
1. Add `revenue_per_share` to FACT_FINANCIAL_RATIOS table and ETL
2. Refactor market metrics to use official EPS values from FACT_FINANCIALS
3. Simplify the complex CTE logic by joining with FACT_FINANCIALS_TTM
4. Ensure proper point-in-time logic throughout
5. Improve query performance and maintainability

## Implementation Plan

### Phase 1: Update FACT_FINANCIAL_RATIOS Table and ETL

#### 1.1 Update Table Definition
**File:** `sql/03_table_definitions.sql`

Add `revenue_per_share` to FACT_FINANCIAL_RATIOS:
```sql
-- Per Share Metrics
book_value_per_share NUMBER(10,4),
revenue_per_share NUMBER(10,4),  -- NEW COLUMN
```

#### 1.2 Recreate Tables
```bash
# Drop and recreate financial tables
python scripts/recreate_financial_tables.py

# Reload financial data
python scripts/run_financial_etl.py --symbols AAPL MSFT --period quarterly --limit 8
python scripts/run_financial_etl.py --symbols AAPL MSFT --period annual --limit 5

# Reload TTM calculations
python scripts/run_ttm_calculation_etl.py --symbols AAPL MSFT
```

#### 1.3 Update FinancialRatioETL
**File:** `src/etl/financial_ratio_etl.py`

Add revenue_per_share calculation:
```python
# In the SQL query, add:
CASE 
    WHEN shares_outstanding > 0 
    THEN revenue / shares_outstanding 
    ELSE NULL 
END as revenue_per_share
```

### Phase 2: Refactor Market Metrics ETL

#### 2.1 Simplify Extract Query
**File:** `src/etl/market_metrics_etl.py`

**Current structure (complex):**
- Multiple CTEs for quarterly data aggregation
- On-the-fly TTM calculations
- Complex point-in-time logic
- ~200+ lines of SQL

**New structure (simple):**
```sql
WITH daily_prices AS (
    -- Get daily price and market cap data
    SELECT 
        p.company_key,
        p.date_key,
        p.close_price,
        p.close_price * f.latest_shares_outstanding as market_cap
    FROM ANALYTICS.FACT_DAILY_PRICES p
    -- Join with most recent quarterly data for shares
),
latest_quarterly AS (
    -- Get most recent quarterly financial data as of each price date
    SELECT DISTINCT
        p.company_key,
        p.date_key,
        FIRST_VALUE(f.financial_key) OVER (
            PARTITION BY p.company_key, p.date_key 
            ORDER BY f.accepted_date DESC
        ) as financial_key
    FROM daily_prices p
    JOIN ANALYTICS.FACT_FINANCIALS f
        ON p.company_key = f.company_key
        AND f.accepted_date <= [price_date]
),
latest_ttm AS (
    -- Get most recent TTM calculation as of each price date
    SELECT DISTINCT
        p.company_key,
        p.date_key,
        FIRST_VALUE(t.ttm_key) OVER (
            PARTITION BY p.company_key, p.date_key 
            ORDER BY t.calculation_date DESC
        ) as ttm_key
    FROM daily_prices p
    JOIN ANALYTICS.FACT_FINANCIALS_TTM t
        ON p.company_key = t.company_key
        AND t.calculation_date <= [price_date]
)
SELECT 
    -- Join everything together
    -- Use official eps_diluted from FACT_FINANCIALS
    -- Use ttm_eps_diluted from FACT_FINANCIALS_TTM
    -- Use revenue_per_share from FACT_FINANCIAL_RATIOS
```

#### 2.2 Key Changes
1. **Remove**: Complex 4-quarter lookback CTEs
2. **Remove**: Manual TTM calculations
3. **Add**: Direct join with FACT_FINANCIALS_TTM
4. **Add**: Join with FACT_FINANCIAL_RATIOS for revenue_per_share
5. **Use**: Official eps_diluted values instead of calculating

#### 2.3 Metric Calculations
```sql
-- P/E Ratio (Quarterly)
CASE 
    WHEN f.eps_diluted > 0 
    THEN p.close_price / f.eps_diluted 
    ELSE NULL 
END as pe_ratio

-- P/E Ratio (TTM)
CASE 
    WHEN t.ttm_eps_diluted > 0 
    THEN p.close_price / t.ttm_eps_diluted 
    ELSE NULL 
END as pe_ratio_ttm

-- P/S Ratio (Quarterly)
CASE 
    WHEN fr.revenue_per_share > 0 
    THEN p.close_price / fr.revenue_per_share 
    ELSE NULL 
END as ps_ratio

-- P/S Ratio (TTM)
CASE 
    WHEN t.ttm_revenue > 0 AND t.latest_shares_outstanding > 0
    THEN p.close_price / (t.ttm_revenue / t.latest_shares_outstanding)
    ELSE NULL 
END as ps_ratio_ttm
```

### Phase 3: Testing & Validation

#### 3.1 Create Comparison Test
**File:** `test_market_metrics_refactor.py` (temporary)

```python
# Run both old and new versions
# Compare results for key metrics
# Ensure values match within acceptable tolerance
```

#### 3.2 Performance Testing
- Measure execution time before/after
- Target: >30% improvement
- Document query execution plans

#### 3.3 Data Validation
- Verify P/E ratios match when using official EPS
- Verify TTM calculations match pre-calculated values
- Check edge cases (negative earnings, zero shares, etc.)

### Phase 4: Cleanup and Documentation

#### 4.1 Update Documentation
- Update CLAUDE.md with new approach
- Document the simplified architecture
- Add examples of the new query structure

#### 4.2 Remove Old Code
- Remove complex CTE logic
- Remove manual TTM calculations
- Clean up unnecessary helper functions

## Implementation Checklist

- [ ] Phase 1: Update FACT_FINANCIAL_RATIOS
  - [ ] Update table definition in SQL file
  - [ ] Run recreate_financial_tables.py
  - [ ] Reload financial data
  - [ ] Update FinancialRatioETL to calculate revenue_per_share
  - [ ] Test ratio calculations
  
- [ ] Phase 2: Refactor Market Metrics ETL
  - [ ] Create new simplified query structure
  - [ ] Update extract method
  - [ ] Remove complex CTEs
  - [ ] Add joins to TTM and ratio tables
  - [ ] Update metric calculations to use official values
  
- [ ] Phase 3: Testing & Validation
  - [ ] Create comparison test script
  - [ ] Run side-by-side comparison
  - [ ] Verify results match
  - [ ] Measure performance improvement
  - [ ] Test edge cases
  
- [ ] Phase 4: Cleanup
  - [ ] Update documentation
  - [ ] Remove old code
  - [ ] Update integration tests
  - [ ] Final verification

## Success Criteria
- ✅ Market metrics query reduced by >50% in complexity
- ✅ All P/E ratios use official EPS values
- ✅ TTM metrics match pre-calculated values exactly
- ✅ Query performance improved by >30%
- ✅ All existing tests continue to pass
- ✅ Point-in-time logic preserved
- ✅ Revenue per share available in financial ratios

## Risks & Mitigations
1. **Risk**: Missing TTM data for some dates
   - **Mitigation**: Use LEFT JOIN and handle NULLs appropriately
   
2. **Risk**: Performance regression in some cases
   - **Mitigation**: Test thoroughly, keep old code as backup initially
   
3. **Risk**: Data discrepancies during transition
   - **Mitigation**: Run comprehensive comparison tests

## Notes
- Keep the old market metrics code commented out initially
- Run both versions in parallel for a few days to ensure consistency
- Document any discrepancies found and their resolutions
- Consider adding a ttm_key foreign key to FACT_MARKET_METRICS for traceability