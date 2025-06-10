# Transformation Mapping Guide

## Overview
This document describes how data is transformed from the FMP API through our three-layer architecture (RAW → STAGING → ANALYTICS).

## Architecture Flow
```
FMP API → Python ETL → RAW Layer (VARIANT) → Python Transform → STAGING Layer → SQL Transform → ANALYTICS Layer
```

## Company Profile Transformations

### FMP API → RAW Layer
Raw data stored as VARIANT in `RAW_COMPANY_PROFILE` table:
```json
{
  "symbol": "AAPL",
  "companyName": "Apple Inc.",
  "marketCap": 3000000000000,
  "sector": "Technology",
  "industry": "Consumer Electronics",
  ...
}
```

### RAW → STAGING Layer
Transformed to structured columns in `STG_COMPANY_PROFILE`:

| FMP Field | Staging Column | Transformation | Data Type |
|-----------|----------------|----------------|-----------|
| symbol | symbol | Direct mapping | VARCHAR(10) |
| companyName | company_name | Direct mapping | VARCHAR(255) |
| sector | sector | Direct mapping | VARCHAR(100) |
| industry | industry | Direct mapping | VARCHAR(100) |
| exchange | exchange | Direct mapping | VARCHAR(50) |
| marketCap | market_cap | Direct mapping | NUMBER(20,2) |
| description | description | Direct mapping | TEXT |
| website | website | Direct mapping | VARCHAR(255) |
| ceo | ceo | Direct mapping | VARCHAR(255) |
| employees | employees | String to integer | NUMBER |
| city | headquarters_city | Direct mapping | VARCHAR(100) |
| state | headquarters_state | Direct mapping | VARCHAR(50) |
| country | headquarters_country | Direct mapping | VARCHAR(50) |

### Key Transformations:
- **Null Handling**: Empty strings converted to NULL
- **Market Cap**: Preserved as-is (already numeric from API)
- **Employee Count**: Converted from string to integer

## Historical Price Transformations

### FMP API → RAW Layer
Raw data stored as VARIANT in `RAW_HISTORICAL_PRICES`:
```json
{
  "date": "2024-01-15",
  "open": 150.25,
  "high": 152.30,
  "low": 149.80,
  "close": 151.75,
  "adjClose": 151.75,
  "volume": 50000000,
  "changePercent": 1.5
}
```

### RAW → STAGING Layer
Transformed to `STG_HISTORICAL_PRICES`:

| FMP Field | Staging Column | Transformation | Data Type |
|-----------|----------------|----------------|-----------|
| date | price_date | String to date | DATE |
| open | open_price | Direct mapping | NUMBER(10,2) |
| high | high_price | Direct mapping | NUMBER(10,2) |
| low | low_price | Direct mapping | NUMBER(10,2) |
| close | close_price | Direct mapping | NUMBER(10,2) |
| adjClose | adj_close | Direct mapping | NUMBER(10,2) |
| volume | volume | Direct mapping | NUMBER(20) |
| changePercent | change_percent | Direct mapping | NUMBER(10,4) |

### Key Transformations:
- **Date Conversion**: "YYYY-MM-DD" string to DATE type
- **Price Validation**: Ensures prices > 0
- **Volume**: Converted to integer if needed

## Income Statement Transformations

### FMP API → RAW Layer
Raw data stored as VARIANT in `RAW_INCOME_STATEMENT`:
```json
{
  "date": "2023-12-31",
  "symbol": "AAPL",
  "period": "FY",
  "revenue": 383285000000,
  "costOfRevenue": 214137000000,
  "grossProfit": 169148000000,
  "filingDate": "2024-02-02",
  "acceptedDate": "2024-02-02 16:05:31"
}
```

### RAW → STAGING Layer
Transformed to `STG_INCOME_STATEMENT`:

| FMP Field | Staging Column | Transformation | Data Type |
|-----------|----------------|----------------|-----------|
| symbol | symbol | Direct mapping | VARCHAR(10) |
| date | fiscal_date | String to date | DATE |
| period | period | Direct mapping | VARCHAR(10) |
| filingDate | filing_date | String to date | DATE |
| acceptedDate | accepted_date | String to timestamp | TIMESTAMP_NTZ |
| revenue | revenue | Direct mapping | NUMBER(20,2) |
| costOfRevenue | cost_of_revenue | Direct mapping | NUMBER(20,2) |
| grossProfit | gross_profit | Direct mapping | NUMBER(20,2) |
| operatingExpenses | operating_expenses | Direct mapping | NUMBER(20,2) |
| operatingIncome | operating_income | Direct mapping | NUMBER(20,2) |
| netIncome | net_income | Direct mapping | NUMBER(20,2) |
| eps | eps | Direct mapping | NUMBER(10,4) |
| epsDiluted | eps_diluted | Direct mapping | NUMBER(10,4) |
| weightedAverageShsOut | shares_outstanding | Direct mapping | NUMBER(20) |
| weightedAverageShsOutDil | shares_outstanding_diluted | Direct mapping | NUMBER(20) |

### Key Transformations:
- **Date Parsing**: "YYYY-MM-DD" → DATE
- **Timestamp Parsing**: "YYYY-MM-DD HH:MM:SS" → TIMESTAMP_NTZ
- **Null Handling**: Missing financial values become NULL
- **Period Standardization**: Preserves FY, Q1, Q2, Q3, Q4

## Balance Sheet Transformations

### FMP API → STAGING Layer

| FMP Field | Staging Column | Transformation | Data Type |
|-----------|----------------|----------------|-----------|
| totalAssets | total_assets | Direct mapping | NUMBER(20,2) |
| totalCurrentAssets | current_assets | Direct mapping | NUMBER(20,2) |
| totalLiabilities | total_liabilities | Direct mapping | NUMBER(20,2) |
| totalCurrentLiabilities | current_liabilities | Direct mapping | NUMBER(20,2) |
| totalEquity or totalStockholdersEquity | total_equity | Fallback logic | NUMBER(20,2) |
| cashAndCashEquivalents | cash_and_equivalents | Direct mapping | NUMBER(20,2) |
| totalDebt | total_debt | Direct mapping | NUMBER(20,2) |
| netDebt | net_debt | Direct mapping | NUMBER(20,2) |

### Key Transformations:
- **Equity Fallback**: Uses `totalEquity` or `totalStockholdersEquity`
- **All amounts**: Preserved as-is (API provides numeric values)

## Cash Flow Statement Transformations

### FMP API → STAGING Layer

| FMP Field | Staging Column | Transformation | Data Type |
|-----------|----------------|----------------|-----------|
| operatingCashFlow | operating_cash_flow | Direct mapping | NUMBER(20,2) |
| cashFlowFromInvestment | investing_cash_flow | Direct mapping | NUMBER(20,2) |
| cashFlowFromFinancing | financing_cash_flow | Direct mapping | NUMBER(20,2) |
| freeCashFlow | free_cash_flow | Direct mapping | NUMBER(20,2) |
| capitalExpenditure | capital_expenditures | Direct mapping | NUMBER(20,2) |
| dividendsPaid | dividends_paid | Direct mapping | NUMBER(20,2) |

### Key Transformations:
- **Sign Convention**: Preserves API signs (negative for outflows)
- **Null Handling**: Missing values become NULL

## Common Transformation Patterns

### 1. Date/Time Handling
```python
# Date string to date
fiscal_date = datetime.strptime(data.get('date'), '%Y-%m-%d').date()

# Datetime string to timestamp
accepted_date = datetime.strptime(data.get('acceptedDate'), '%Y-%m-%d %H:%M:%S')
```

### 2. Null Value Handling
- Empty strings → NULL
- Missing keys → NULL
- Zero preservation (0 is valid, not NULL)

### 3. Field Naming Convention
- camelCase (API) → snake_case (Database)
- Descriptive names: `weightedAverageShsOut` → `shares_outstanding`

### 4. Data Type Standardization
- All monetary values: NUMBER(20,2)
- Share counts: NUMBER(20)
- Ratios/percentages: NUMBER(10,4)
- Identifiers: VARCHAR with appropriate length

## Staging → Analytics Transformations

### DIM_COMPANY
Sources from `STG_COMPANY_PROFILE`:
- Adds surrogate key (company_key)
- Concatenates location: `city || ', ' || state || ', ' || country`
- Categorizes market_cap_category based on size

### FACT_FINANCIALS
Merges data from staging tables:
- Joins income statement, balance sheet, and cash flow by symbol/date/period
- Adds foreign keys (company_key, date_key)
- Preserves all financial metrics

### FACT_DAILY_PRICES
Sources from `STG_HISTORICAL_PRICES`:
- Adds surrogate key (price_key)
- Calculates change_amount: `close_price - open_price`
- Links to DIM_COMPANY and DIM_DATE

## Error Handling

### Data Quality Checks Applied:
1. **Required Fields**: Symbol, dates must exist
2. **Valid Ranges**: Prices > 0, dates not in future
3. **Data Types**: Numeric fields must parse correctly
4. **Referential Integrity**: Symbol must exist in DIM_COMPANY

### Transformation Failures:
- Logged to application logs
- Tracked in transformation_stats
- Failed records skipped, not loaded to staging
- Partial loads allowed (processes valid records)