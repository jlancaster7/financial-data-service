# Story 6.1: Create ETL Monitoring Dashboard - Implementation Plan

## Overview
Create a comprehensive monitoring dashboard to track ETL pipeline health, performance, and data quality metrics using Snowflake's native visualization capabilities or a simple web-based solution.

## What is a Monitoring Dashboard?
A monitoring dashboard is a visual interface that displays key metrics about our ETL pipelines in real-time, allowing operators to:
- Quickly identify failed jobs or performance issues
- Track data freshness and completeness
- Monitor trends over time
- Drill down into specific issues

## Option 1: Snowflake Native Dashboard (Recommended)

### Pros:
- No additional infrastructure needed
- Direct connection to data
- Built-in sharing capabilities
- SQL-based, easy to maintain

### Cons:
- Limited visualization options
- Requires Snowflake access for viewers

### Implementation:
1. Create views for dashboard metrics
2. Build Snowsight dashboards
3. Set up refresh schedules

## Option 2: Python + Streamlit Dashboard

### Pros:
- Rich, interactive visualizations
- Can be hosted anywhere
- More customization options
- Can integrate multiple data sources

### Cons:
- Requires hosting infrastructure
- Additional dependency (Streamlit)
- Needs authentication setup

### Implementation:
1. Create Streamlit app
2. Connect to Snowflake
3. Build interactive charts
4. Deploy to Streamlit Cloud or internal server

## Proposed Dashboard Components

### 1. Pipeline Health Overview
**Purpose:** At-a-glance view of system health

**Metrics:**
- Current Status: Number of pipelines running/completed/failed (last 24h)
- Success Rate: Percentage of successful runs (7-day trend)
- Active Alerts: Count of current issues requiring attention

**Visualization:**
- Traffic light indicators (green/yellow/red)
- Donut chart for success/failure breakdown
- Alert banner for critical issues

### 2. ETL Job History
**Purpose:** Track individual job executions

**Metrics:**
- Job timeline (Gantt chart showing when jobs ran)
- Duration trends by job type
- Record counts processed
- Failure reasons

**Visualization:**
- Timeline/Gantt chart
- Bar charts for duration
- Table with sortable columns

**SQL View Example:**
```sql
CREATE OR REPLACE VIEW V_ETL_JOB_SUMMARY AS
SELECT 
    job_name,
    DATE(start_time) as run_date,
    COUNT(*) as run_count,
    SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) as success_count,
    AVG(duration_seconds) as avg_duration_seconds,
    MAX(duration_seconds) as max_duration_seconds,
    SUM(records_loaded) as total_records_loaded,
    MAX(end_time) as last_run_time
FROM ETL_JOB_HISTORY
WHERE start_time >= DATEADD('day', -30, CURRENT_DATE())
GROUP BY job_name, DATE(start_time)
ORDER BY run_date DESC, job_name;
```

### 3. Data Freshness Monitor
**Purpose:** Ensure data is up-to-date

**Metrics:**
- Last update time by data type (company, prices, financials)
- Days since last update
- Expected vs actual update frequency
- Missing data alerts

**Visualization:**
- Table with color coding (green = fresh, yellow = stale, red = very stale)
- Line chart showing update frequency over time

**SQL View Example:**
```sql
CREATE OR REPLACE VIEW V_DATA_FRESHNESS_MONITOR AS
WITH freshness_data AS (
    SELECT 
        'Company Profiles' as data_type,
        MAX(loaded_timestamp) as last_update,
        DATEDIFF('hour', MAX(loaded_timestamp), CURRENT_TIMESTAMP()) as hours_since_update,
        COUNT(DISTINCT symbol) as symbol_count
    FROM STAGING.STG_COMPANY_PROFILE
    UNION ALL
    SELECT 
        'Daily Prices' as data_type,
        MAX(loaded_timestamp) as last_update,
        DATEDIFF('hour', MAX(loaded_timestamp), CURRENT_TIMESTAMP()) as hours_since_update,
        COUNT(DISTINCT symbol) as symbol_count
    FROM STAGING.STG_HISTORICAL_PRICES
    UNION ALL
    SELECT 
        'Financial Statements' as data_type,
        MAX(loaded_timestamp) as last_update,
        DATEDIFF('hour', MAX(loaded_timestamp), CURRENT_TIMESTAMP()) as hours_since_update,
        COUNT(DISTINCT symbol) as symbol_count
    FROM STAGING.STG_INCOME_STATEMENT
)
SELECT 
    data_type,
    last_update,
    hours_since_update,
    symbol_count,
    CASE 
        WHEN data_type = 'Daily Prices' AND hours_since_update > 48 THEN 'CRITICAL'
        WHEN data_type = 'Daily Prices' AND hours_since_update > 24 THEN 'WARNING'
        WHEN data_type IN ('Company Profiles', 'Financial Statements') AND hours_since_update > 168 THEN 'WARNING'
        ELSE 'OK'
    END as freshness_status
FROM freshness_data;
```

### 4. Performance Metrics
**Purpose:** Track system performance and identify bottlenecks

**Metrics:**
- Average job duration by type
- Records processed per minute
- API call counts and rate limits
- Database query performance

**Visualization:**
- Line charts for trends
- Heat map for job performance by time of day
- Top 10 slowest operations table

### 5. Data Quality Scorecard
**Purpose:** Monitor data quality metrics

**Metrics:**
- Completeness: % of expected fields populated
- Validity: % of records passing quality checks
- Consistency: Cross-table validation results
- Coverage: Companies/dates with data

**Visualization:**
- Scorecard with percentage indicators
- Trend lines for quality metrics
- Drill-down to specific issues

**SQL View Example:**
```sql
CREATE OR REPLACE VIEW V_DATA_QUALITY_SCORECARD AS
WITH quality_metrics AS (
    -- Completeness check
    SELECT 
        'Completeness' as metric_type,
        'Financial Data' as category,
        (SELECT COUNT(*) FROM STAGING.STG_INCOME_STATEMENT WHERE revenue IS NOT NULL) * 100.0 / 
        NULLIF((SELECT COUNT(*) FROM STAGING.STG_INCOME_STATEMENT), 0) as score
    UNION ALL
    -- Consistency check
    SELECT 
        'Consistency' as metric_type,
        'Financial Statements' as category,
        (SELECT COUNT(*) 
         FROM STAGING.STG_INCOME_STATEMENT i
         JOIN STAGING.STG_BALANCE_SHEET b 
            ON i.symbol = b.symbol 
            AND i.fiscal_date = b.fiscal_date 
            AND i.period = b.period) * 100.0 /
        NULLIF((SELECT COUNT(*) FROM STAGING.STG_INCOME_STATEMENT), 0) as score
)
SELECT 
    metric_type,
    category,
    ROUND(score, 2) as score_percentage,
    CASE 
        WHEN score >= 95 THEN 'EXCELLENT'
        WHEN score >= 90 THEN 'GOOD'
        WHEN score >= 80 THEN 'FAIR'
        ELSE 'POOR'
    END as quality_rating
FROM quality_metrics;
```

### 6. Error Analysis
**Purpose:** Identify and track error patterns

**Metrics:**
- Error counts by type
- Error frequency trends
- Most common error messages
- Failed record details

**Visualization:**
- Pie chart of error types
- Time series of error counts
- Searchable error log table

## Implementation Plan

### Phase 1: Create Monitoring Views (Day 1)
1. Create all SQL views for metrics
2. Test views with sample queries
3. Optimize performance with materialized views if needed

### Phase 2: Build Dashboard (Day 2-3)
**Option A - Snowflake Approach:**
1. Create Snowsight worksheets for each component
2. Build dashboard layout
3. Add filters for date ranges and job types
4. Set up auto-refresh

**Option B - Streamlit Approach:**
1. Create `dashboard/app.py`
2. Build layout with tabs for each section
3. Implement interactive filters
4. Add data export capabilities

### Phase 3: Operationalize (Day 4)
1. Create refresh procedures
2. Document dashboard usage
3. Set up access controls
4. Create user guide

## Sample Dashboard Layout

```
┌─────────────────────────────────────────────────────────────────┐
│                    ETL Pipeline Monitoring Dashboard             │
├─────────────────────────────────────────────────────────────────┤
│  Health Overview  │  Last 24 Hours  │  Last 7 Days  │  Custom   │
├─────────────────────────────────────────────────────────────────┤
│ ┌─────────────┐ ┌─────────────┐ ┌─────────────┐ ┌────────────┐│
│ │ Success Rate│ │Running Jobs │ │ Failed Jobs │ │Data Fresh  ││
│ │    94.5%    │ │      2      │ │      3      │ │  ✓ Fresh   ││
│ │  ▲ +2.1%    │ │             │ │  ⚠ Check   │ │            ││
│ └─────────────┘ └─────────────┘ └─────────────┘ └────────────┘│
├─────────────────────────────────────────────────────────────────┤
│                         Job Timeline                             │
│  [====Company ETL====]  [==Price ETL==] [ERROR]                │
│         [=====Financial ETL=====]    [==TTM Calc==]            │
├─────────────────────────────────────────────────────────────────┤
│  Performance Trends          │  Data Quality Scorecard          │
│  [Line chart of duration]    │  Completeness:  █████████ 95%   │
│                              │  Consistency:   ████████  89%   │
│                              │  Validity:      █████████ 92%   │
└─────────────────────────────────────────────────────────────────┘
```

## Success Criteria
1. Dashboard loads in < 3 seconds
2. All metrics update automatically
3. Users can drill down to details
4. Mobile-responsive (if web-based)
5. Alerts are visually prominent

## Future Enhancements
1. Real-time updates via webhooks
2. Predictive failure analysis
3. Cost tracking integration
4. API performance monitoring
5. Custom alert thresholds