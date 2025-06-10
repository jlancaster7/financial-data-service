# Dividend Yield Calculation Fix Documentation

## Issue Summary
The dividend yield calculation in the Market Metrics ETL was always returning NULL values, despite having dividend data in the FACT_FINANCIALS_TTM table.

## Root Cause
The code was checking for positive dividend values (`ttm_dividends > 0`), but dividends are stored as **negative values** in the cash flow statement because they represent cash outflows from the company's perspective.

## The Fix

### Before (Incorrect)
```python
# Line 331 in market_metrics_etl.py
if close_price > 0 and ttm_dividends > 0 and shares_outstanding > 0:
    dividends_per_share = ttm_dividends / shares_outstanding
    metric_record['dividend_yield'] = round((dividends_per_share / close_price) * 100, 2)
```

### After (Fixed)
```python
# Line 331 in market_metrics_etl.py  
if close_price > 0 and ttm_dividends < 0 and shares_outstanding > 0:  # dividends are negative cash flows
    dividends_per_share = abs(ttm_dividends) / shares_outstanding
    metric_record['dividend_yield'] = round((dividends_per_share / close_price) * 100, 2)
```

## Key Changes

1. **Condition Change**: Changed from `ttm_dividends > 0` to `ttm_dividends < 0`
2. **Value Handling**: Added `abs()` function to convert negative dividend value to positive for the calculation
3. **Documentation**: Added inline comment explaining that dividends are negative cash flows

## Why Dividends Are Negative

In financial accounting and the FMP API:
- Cash flow statements follow the convention where:
  - **Positive values** = Cash inflows (money coming in)
  - **Negative values** = Cash outflows (money going out)
- Dividends paid to shareholders are cash outflows, hence stored as negative values
- This is consistent with other cash outflows like capital expenditures

## Impact on Other Calculations

The same fix was also applied to the **payout ratio** calculation:

```python
# Line 339 in market_metrics_etl.py
if ttm_net_income > 0 and ttm_dividends < 0:  # dividends are negative cash flow
    metric_record['payout_ratio'] = round((abs(ttm_dividends) / ttm_net_income) * 100, 2)
```

## Testing the Fix

To verify the fix works correctly:

1. Check raw data in FACT_FINANCIALS_TTM:
```sql
SELECT 
    symbol,
    ttm_dividends_paid,
    ttm_net_income
FROM ANALYTICS.FACT_FINANCIALS_TTM ttm
JOIN ANALYTICS.DIM_COMPANY c ON ttm.company_key = c.company_key
WHERE ttm_dividends_paid IS NOT NULL
  AND ttm_dividends_paid < 0  -- Dividends are negative
LIMIT 10;
```

2. Verify dividend yield calculations:
```sql
SELECT 
    c.symbol,
    m.close_price,
    m.dividend_yield,
    m.payout_ratio,
    m.date_key
FROM ANALYTICS.FACT_MARKET_METRICS m
JOIN ANALYTICS.DIM_COMPANY c ON m.company_key = c.company_key
WHERE m.dividend_yield IS NOT NULL
ORDER BY m.date_key DESC
LIMIT 20;
```

## Lessons Learned

1. **Always verify data conventions**: Check how values are stored (positive vs negative)
2. **Review source data**: Look at actual values in the database before making assumptions
3. **Document conventions**: Add comments explaining non-obvious data conventions
4. **Test with real data**: Ensure calculations produce expected results with actual data

## Related Files

- `/src/etl/market_metrics_etl.py` - Lines 331-334 (dividend yield) and 339-342 (payout ratio)
- `/src/etl/financial_statement_etl.py` - Source of dividend data from cash flow statements
- `/src/etl/ttm_calculation_etl.py` - Calculates TTM dividend values

## Future Considerations

1. **Data Validation**: Add checks to ensure dividend values follow expected convention
2. **Unit Tests**: Create tests that verify dividend calculations with negative input values
3. **Documentation**: Update data dictionary to clearly state that dividends are stored as negative values