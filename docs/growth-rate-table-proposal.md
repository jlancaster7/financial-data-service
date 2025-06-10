# Growth Rate Table Proposal

## Overview
Growth rates are essential for financial analysis, including PEG ratio calculations, trend analysis, and forecasting. This proposal outlines a comprehensive growth rate table that would serve multiple use cases.

## Growth Rates Needed for PEG Ratio

**PEG Ratio = P/E Ratio ÷ Earnings Growth Rate**

For PEG ratio specifically, we need:
- **Earnings Growth Rate**: Typically year-over-year (YoY) EPS growth
- Common variations:
  - 1-year historical EPS growth
  - 3-year average EPS growth
  - 5-year average EPS growth
  - Forward EPS growth (requires analyst estimates)

Example calculation:
```
If P/E = 25 and EPS growth = 20%
PEG = 25 / 20 = 1.25
```

## Proposed Table: FACT_GROWTH_RATES

### Table Structure
```sql
CREATE TABLE IF NOT EXISTS ANALYTICS.FACT_GROWTH_RATES (
    growth_rate_key NUMBER AUTOINCREMENT PRIMARY KEY,
    company_key NUMBER NOT NULL,
    calculation_date DATE NOT NULL,
    period_type VARCHAR(10) NOT NULL,  -- 'QUARTERLY', 'ANNUAL', 'TTM'
    
    -- Period identifiers
    current_period_date DATE NOT NULL,
    prior_period_date DATE NOT NULL,
    periods_back NUMBER NOT NULL,  -- 1 for YoY, 4 for QoQ same quarter
    
    -- Revenue growth rates
    revenue_growth_rate NUMBER(10,4),
    revenue_current NUMBER(20,2),
    revenue_prior NUMBER(20,2),
    
    -- Earnings growth rates
    net_income_growth_rate NUMBER(10,4),
    net_income_current NUMBER(20,2),
    net_income_prior NUMBER(20,2),
    
    eps_growth_rate NUMBER(10,4),
    eps_current NUMBER(10,4),
    eps_prior NUMBER(10,4),
    
    eps_diluted_growth_rate NUMBER(10,4),
    eps_diluted_current NUMBER(10,4),
    eps_diluted_prior NUMBER(10,4),
    
    -- Operating metrics growth
    operating_income_growth_rate NUMBER(10,4),
    operating_income_current NUMBER(20,2),
    operating_income_prior NUMBER(20,2),
    
    gross_profit_growth_rate NUMBER(10,4),
    gross_profit_current NUMBER(20,2),
    gross_profit_prior NUMBER(20,2),
    
    -- Cash flow growth
    operating_cash_flow_growth_rate NUMBER(10,4),
    operating_cash_flow_current NUMBER(20,2),
    operating_cash_flow_prior NUMBER(20,2),
    
    free_cash_flow_growth_rate NUMBER(10,4),
    free_cash_flow_current NUMBER(20,2),
    free_cash_flow_prior NUMBER(20,2),
    
    -- Balance sheet growth
    total_assets_growth_rate NUMBER(10,4),
    total_assets_current NUMBER(20,2),
    total_assets_prior NUMBER(20,2),
    
    total_equity_growth_rate NUMBER(10,4),
    total_equity_current NUMBER(20,2),
    total_equity_prior NUMBER(20,2),
    
    -- Per share metrics growth
    book_value_per_share_growth_rate NUMBER(10,4),
    book_value_per_share_current NUMBER(10,4),
    book_value_per_share_prior NUMBER(10,4),
    
    revenue_per_share_growth_rate NUMBER(10,4),
    revenue_per_share_current NUMBER(10,4),
    revenue_per_share_prior NUMBER(10,4),
    
    -- Growth quality metrics
    growth_consistency_score NUMBER(10,4),  -- Std dev of growth rates
    growth_acceleration NUMBER(10,4),  -- Change in growth rate
    
    -- Metadata
    created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    
    FOREIGN KEY (company_key) REFERENCES DIM_COMPANY(company_key),
    UNIQUE KEY (company_key, calculation_date, period_type, periods_back)
);
```

## Growth Rate Types to Calculate

### 1. Year-over-Year (YoY) Growth
- Compare same period from previous year
- Q2 2024 vs Q2 2023
- FY 2024 vs FY 2023
- TTM ending Q2 2024 vs TTM ending Q2 2023

### 2. Quarter-over-Quarter (QoQ) Growth
- Sequential quarter comparison
- Q2 2024 vs Q1 2024
- Useful for detecting momentum changes

### 3. Compound Annual Growth Rate (CAGR)
- Multi-year average growth
- 3-year CAGR: ((Current/3YearsAgo)^(1/3) - 1)
- 5-year CAGR: ((Current/5YearsAgo)^(1/5) - 1)

### 4. Moving Average Growth
- Smooths out volatility
- 4-quarter moving average growth
- Reduces impact of one-time items

## Implementation Approach

### ETL Pipeline: GrowthRateETL
```python
class GrowthRateETL(BaseETL):
    """Calculate growth rates from financial data"""
    
    def extract(self):
        # Get financial data with current and prior periods
        # Join FACT_FINANCIALS with itself on prior periods
        
    def transform(self):
        # Calculate growth rates
        # Handle edge cases (division by zero, negative to positive)
        # Calculate consistency scores
        
    def load(self):
        # Insert into FACT_GROWTH_RATES
        # Use MERGE to handle updates
```

### Key Calculations
```sql
-- YoY Quarterly Growth
WITH current_quarter AS (
    SELECT * FROM FACT_FINANCIALS 
    WHERE period_type IN ('Q1','Q2','Q3','Q4')
),
prior_year_quarter AS (
    SELECT * FROM FACT_FINANCIALS 
    WHERE period_type IN ('Q1','Q2','Q3','Q4')
)
SELECT 
    c.company_key,
    c.fiscal_date_key as current_period,
    p.fiscal_date_key as prior_period,
    CASE 
        WHEN p.revenue > 0 
        THEN ((c.revenue - p.revenue) / p.revenue) * 100
        ELSE NULL 
    END as revenue_growth_rate
FROM current_quarter c
JOIN prior_year_quarter p
    ON c.company_key = p.company_key
    AND c.period_type = p.period_type  -- Same quarter
    AND DATEADD('year', -1, c.fiscal_date) = p.fiscal_date

-- YoY TTM Growth
WITH current_ttm AS (
    SELECT * FROM FACT_FINANCIALS_TTM
),
prior_ttm AS (
    SELECT * FROM FACT_FINANCIALS_TTM
)
SELECT 
    c.company_key,
    c.calculation_date,
    CASE 
        WHEN p.ttm_revenue > 0 
        THEN ((c.ttm_revenue - p.ttm_revenue) / p.ttm_revenue) * 100
        ELSE NULL 
    END as ttm_revenue_growth_rate
FROM current_ttm c
JOIN prior_ttm p
    ON c.company_key = p.company_key
    AND DATEADD('year', -1, c.calculation_date) = p.calculation_date
```

## Use Cases Beyond PEG Ratio

### 1. Screening & Filtering
- Find companies with >20% revenue growth
- Identify decelerating growth (growth rate declining)
- Screen for consistent growers (low volatility in growth)

### 2. Valuation Models
- DCF models need growth assumptions
- Relative valuation using growth-adjusted multiples
- Growth at Reasonable Price (GARP) analysis

### 3. Performance Analytics
- Management effectiveness (asset growth vs revenue growth)
- Operating leverage (operating income growing faster than revenue)
- Market share analysis (revenue growth vs industry growth)

### 4. Risk Analysis
- Growth sustainability metrics
- Mean reversion analysis
- Growth quality scoring

### 5. Dashboard Metrics
- Growth trend charts
- Peer comparison tables
- Growth decomposition analysis

## Sample Queries Using Growth Rates

### Calculate PEG Ratio
```sql
SELECT 
    m.company_key,
    m.date_key,
    m.pe_ratio_ttm,
    g.eps_diluted_growth_rate as eps_growth_yoy,
    CASE 
        WHEN g.eps_diluted_growth_rate > 0 
        THEN m.pe_ratio_ttm / g.eps_diluted_growth_rate
        ELSE NULL 
    END as peg_ratio
FROM FACT_MARKET_METRICS m
JOIN FACT_GROWTH_RATES g
    ON m.company_key = g.company_key
    AND m.date_key = g.calculation_date_key
WHERE g.period_type = 'TTM'
AND g.periods_back = 1;  -- YoY
```

### Find High-Quality Growth
```sql
SELECT 
    c.symbol,
    g.revenue_growth_rate,
    g.gross_profit_growth_rate,
    g.operating_income_growth_rate,
    g.growth_consistency_score
FROM FACT_GROWTH_RATES g
JOIN DIM_COMPANY c ON g.company_key = c.company_key
WHERE g.period_type = 'TTM'
AND g.periods_back = 1
AND g.revenue_growth_rate > 15
AND g.operating_income_growth_rate > g.revenue_growth_rate  -- Operating leverage
AND g.growth_consistency_score > 0.8  -- Consistent growth
ORDER BY g.revenue_growth_rate DESC;
```

## Benefits of Dedicated Growth Table

1. **Performance**: Pre-calculated growth rates avoid complex joins
2. **Consistency**: Single source of truth for growth calculations
3. **Flexibility**: Multiple growth periods and types in one place
4. **Analysis**: Enables advanced growth analytics and scoring
5. **Historical**: Preserves point-in-time growth rates

## Implementation Priority

1. **Phase 1**: Basic YoY growth for key metrics (revenue, EPS, net income)
2. **Phase 2**: QoQ and TTM growth rates
3. **Phase 3**: Multi-year CAGR calculations
4. **Phase 4**: Growth quality metrics and scoring

This would enable PEG ratio calculation and provide a foundation for comprehensive growth analysis across the platform.