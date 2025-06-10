# Data Quality Rules Documentation

## Overview
This document catalogs all data quality rules applied during the ETL process from FMP API to Snowflake. Rules are enforced in the Python transformation layer before data enters the staging tables.

## Data Quality Framework

### Rule Categories
1. **Completeness**: Required fields must be present
2. **Validity**: Data must meet format and range requirements
3. **Consistency**: Related fields must be logically consistent
4. **Accuracy**: Data must be reasonable and within expected bounds
5. **Uniqueness**: No duplicate records based on business keys

## Company Profile Quality Rules

### Required Fields (Completeness)
- ✅ **symbol**: Must be present and non-empty
- ✅ **company_name**: Must be present and non-empty

### Validation Rules
| Field | Rule | Action if Failed |
|-------|------|------------------|
| symbol | Length ≤ 10 characters | Skip record |
| symbol | Alphanumeric only | Skip record |
| market_cap | If present, must be ≥ 0 | Set to NULL |
| employees | If present, must be ≥ 0 | Set to NULL |
| website | Must be valid URL format | Set to NULL |

### Implementation
```python
# From DataQualityValidator.validate_company_profile()
if not profile.symbol or not profile.company_name:
    issues.append(DataQualityIssue(
        severity="ERROR",
        field="symbol/company_name",
        message="Missing required field",
        record_identifier=profile.symbol
    ))
```

## Historical Price Quality Rules

### Required Fields
- ✅ **symbol**: Must be present
- ✅ **price_date**: Must be valid date
- ✅ **close_price**: Must be present

### Validation Rules
| Field | Rule | Action if Failed |
|-------|------|------------------|
| price_date | Cannot be future date | Skip record |
| price_date | Format: YYYY-MM-DD | Skip record |
| open_price | 0 < price < 1,000,000 | Skip record |
| high_price | 0 < price < 1,000,000 | Skip record |
| low_price | 0 < price < 1,000,000 | Skip record |
| close_price | 0 < price < 1,000,000 | Skip record |
| volume | Must be ≥ 0 | Set to 0 |
| change_percent | -100 ≤ percent ≤ 1000 | Set to NULL |

### Consistency Rules
- ✅ **high_price** ≥ low_price
- ✅ **high_price** ≥ open_price
- ✅ **high_price** ≥ close_price
- ✅ **low_price** ≤ open_price
- ✅ **low_price** ≤ close_price

### Implementation
```python
# Price range validation
if not (0 < price.close_price < 1_000_000):
    issues.append(DataQualityIssue(
        severity="ERROR",
        field="close_price",
        message=f"Price {price.close_price} outside valid range",
        record_identifier=f"{price.symbol}:{price.price_date}"
    ))

# High/Low consistency
if price.high_price < price.low_price:
    issues.append(DataQualityIssue(
        severity="ERROR",
        field="high_price/low_price",
        message="High price less than low price",
        record_identifier=f"{price.symbol}:{price.price_date}"
    ))
```

## Financial Statement Quality Rules

### Required Fields (All Statements)
- ✅ **symbol**: Must be present
- ✅ **fiscal_date**: Must be valid date
- ✅ **period**: Must be in ['FY', 'Q1', 'Q2', 'Q3', 'Q4']

### Income Statement Rules
| Field | Rule | Action if Failed |
|-------|------|------------------|
| filing_date | Cannot be > 180 days after fiscal_date | Warning |
| filing_date | Cannot be before fiscal_date | Warning |
| accepted_date | Cannot be before filing_date | Warning |
| revenue | If present, must be ≥ 0 | Warning |
| gross_profit | Can be negative (valid) | None |
| net_income | Can be negative (valid) | None |
| eps | Can be negative (valid) | None |
| shares_outstanding | If present, must be > 0 | Warning |

### Balance Sheet Rules
| Field | Rule | Action if Failed |
|-------|------|------------------|
| total_assets | If present, must be > 0 | Warning |
| total_liabilities | If present, must be ≥ 0 | Warning |
| total_equity | Can be negative (valid) | None |
| current_ratio | current_assets/current_liabilities > 0 | Info |

### Cash Flow Rules
| Field | Rule | Action if Failed |
|-------|------|------------------|
| operating_cash_flow | Can be negative (valid) | None |
| free_cash_flow | Can be negative (valid) | None |
| dividends_paid | Should be ≤ 0 (outflow) | Info |
| capital_expenditures | Should be ≤ 0 (outflow) | Info |

### Cross-Statement Consistency (Future Enhancement)
These rules are planned but not yet implemented:
- ❌ gross_profit = revenue - cost_of_revenue
- ❌ total_assets = total_liabilities + total_equity
- ❌ free_cash_flow = operating_cash_flow + capital_expenditures
- ❌ Quarter-over-quarter revenue change < 100%

## Uniqueness Rules

### Staging Layer
Enforced via MERGE statements with composite keys:

| Table | Unique Key | Handling |
|-------|------------|----------|
| STG_COMPANY_PROFILE | symbol | UPDATE on conflict |
| STG_HISTORICAL_PRICES | (symbol, price_date) | UPDATE on conflict |
| STG_INCOME_STATEMENT | (symbol, fiscal_date, period) | UPDATE on conflict |
| STG_BALANCE_SHEET | (symbol, fiscal_date, period) | UPDATE on conflict |
| STG_CASH_FLOW | (symbol, fiscal_date, period) | UPDATE on conflict |

## Data Quality Monitoring

### Severity Levels
1. **ERROR**: Record will be skipped
2. **WARNING**: Record will be loaded but flagged
3. **INFO**: Informational only, no action required

### Quality Metrics Tracked
```python
transformation_stats = {
    'records_processed': 1000,
    'records_failed': 5,
    'validation_errors': 3,
    'validation_warnings': 12,
    'last_run': '2024-01-15 10:30:00'
}
```

### Quality Issue Tracking
```python
@dataclass
class DataQualityIssue:
    severity: str  # ERROR, WARNING, INFO
    field: str
    message: str
    record_identifier: str
    value: Optional[Any] = None
    expected_value: Optional[Any] = None
```

## Future Enhancements

### Planned Additional Rules
1. **Statistical Outlier Detection**
   - Revenue changes > 3 standard deviations
   - P/E ratios outside historical range
   - Volume spikes > 10x average

2. **Time Series Validation**
   - No missing quarters in sequence
   - Fiscal year = sum of 4 quarters
   - Consistent fiscal calendar

3. **Cross-Company Validation**
   - Market cap vs peers in same industry
   - Margins within industry norms
   - Growth rates reasonable for company size

4. **External Reference Validation**
   - Stock splits properly adjusted
   - Dividend dates match announcements
   - Earnings dates align with company calendar

## SQL-Based Quality Monitoring

Create views to monitor data quality in staging:

```sql
-- Data freshness monitoring
CREATE VIEW V_STAGING_DATA_FRESHNESS AS
SELECT 
    'STG_COMPANY_PROFILE' as table_name,
    COUNT(*) as record_count,
    MAX(loaded_timestamp) as last_load_time,
    DATEDIFF('hour', MAX(loaded_timestamp), CURRENT_TIMESTAMP()) as hours_since_load
FROM STG_COMPANY_PROFILE
UNION ALL
SELECT 
    'STG_HISTORICAL_PRICES' as table_name,
    COUNT(*) as record_count,
    MAX(loaded_timestamp) as last_load_time,
    DATEDIFF('hour', MAX(loaded_timestamp), CURRENT_TIMESTAMP()) as hours_since_load
FROM STG_HISTORICAL_PRICES;

-- Missing data detection
CREATE VIEW V_MISSING_FINANCIAL_DATA AS
SELECT 
    i.symbol,
    i.fiscal_date,
    i.period,
    CASE WHEN b.symbol IS NULL THEN 'Missing Balance Sheet' END as balance_issue,
    CASE WHEN c.symbol IS NULL THEN 'Missing Cash Flow' END as cashflow_issue
FROM STG_INCOME_STATEMENT i
LEFT JOIN STG_BALANCE_SHEET b 
    ON i.symbol = b.symbol 
    AND i.fiscal_date = b.fiscal_date 
    AND i.period = b.period
LEFT JOIN STG_CASH_FLOW c 
    ON i.symbol = c.symbol 
    AND i.fiscal_date = c.fiscal_date 
    AND i.period = c.period
WHERE b.symbol IS NULL OR c.symbol IS NULL;
```

## Usage in ETL Pipeline

Data quality validation is automatically applied in the transformation phase:

```python
# In BaseETL.transform()
validator = DataQualityValidator()
issues = validator.validate_financial_statement(statement)

if any(issue.severity == "ERROR" for issue in issues):
    logger.error(f"Skipping record due to validation errors: {issues}")
    continue
```

This ensures only high-quality data enters our staging and analytics layers.