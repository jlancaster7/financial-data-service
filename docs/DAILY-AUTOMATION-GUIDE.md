# Daily Pipeline Automation Guide

## Overview

This guide explains how to set up automated daily updates for the financial data pipeline, including scheduling options, command arguments, and deployment considerations.

## What the Daily Pipeline Does

The `run_daily_pipeline.py` script orchestrates all ETL processes in the correct dependency order:

### Execution Phases
1. **Phase 1**: Company ETL (updates DIM_COMPANY dimension table)
2. **Phase 2**: Price & Financial ETLs (run in parallel)
3. **Phase 3**: Derived calculations (TTM, Ratios, Market Metrics)

### With Daily Arguments: `--sp500 --days-back 5 --period quarterly`

1. **Company ETL**
   - Fetches current S&P 500 constituents
   - Updates company profiles for any changes
   - Adds new companies, marks removed ones as historical
   - Uses MERGE to prevent duplicates

2. **Price ETL** (`--days-back 5`)
   - Loads prices from (today - 5 days) to today
   - Covers weekends and market holidays
   - Example: Monday run gets Wed-Mon data
   - Backfills any missing prices in the window
   - Updates existing prices if corrections exist

3. **Financial ETL** (`--period quarterly`)
   - Fetches all available quarterly reports
   - Captures new earnings releases same day
   - Updates existing quarters if restated
   - Maintains point-in-time accuracy with filing dates

4. **TTM Calculation ETL**
   - Identifies new quarterly data enabling TTM calculations
   - Creates trailing twelve month aggregates
   - Only processes dates not already calculated

5. **Financial Ratios ETL**
   - Calculates ratios for any financials without them
   - Includes profitability, efficiency, leverage metrics

6. **Market Metrics ETL**
   - Combines daily prices with latest financial data
   - Calculates P/E, P/B, EV/EBITDA, etc.
   - Respects point-in-time logic

## Recommended Schedule

### Daily Update (Weekdays after market close)
```bash
# Run at 6:30 PM Eastern (10:30 PM UTC / 9:30 PM UTC daylight)
python scripts/run_daily_pipeline.py --sp500 --days-back 5 --period quarterly
```

**Why these arguments:**
- `--sp500`: Updates all S&P 500 companies
- `--days-back 5`: Rolling window covers weekends/holidays
- `--period quarterly`: Captures earnings reports quickly

### Weekly Full Refresh (Sundays)
```bash
# Run at 2 AM Eastern Sunday
python scripts/run_daily_pipeline.py --sp500 --days-back 30 --period quarterly
```

**Purpose:**
- Catches any data corrections
- Ensures no gaps from missed daily runs
- Updates any late SEC filings

### Alternative Schedules

**For Testing (Small subset)**
```bash
python scripts/run_daily_pipeline.py \
  --symbols AAPL MSFT NVDA AMZN GOOGL \
  --days-back 5 \
  --period quarterly
```

**For Price-Only Updates (Faster)**
```bash
python scripts/run_daily_pipeline.py \
  --sp500 \
  --days-back 1 \
  --skip-financial \
  --skip-ttm \
  --skip-ratio
```

**For Earnings Season (More frequent)**
```bash
# Run every 6 hours during earnings season
python scripts/run_daily_pipeline.py \
  --sp500 \
  --skip-price \
  --period quarterly
```

## Deployment Options

### 1. Local Development (Cron)

**Setup:**
```bash
# Edit paths in cron file
vi cron/financial-data-pipeline.cron

# Install crontab
crontab cron/financial-data-pipeline.cron

# Verify
crontab -l
```

**Pros:**
- Easy testing
- Direct log access
- No cloud costs

**Cons:**
- Requires always-on computer
- No built-in monitoring
- Manual failure recovery

### 2. AWS EC2 + Cron

**Setup:**
```bash
# On EC2 instance
git clone <repo>
cd financial-data-service
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Configure environment
cp .env.example .env
vi .env  # Add credentials

# Install cron
crontab cron/financial-data-pipeline.cron
```

**Pros:**
- Reliable uptime
- Full control
- Can handle large workloads

**Cons:**
- Monthly EC2 costs (~$20-50)
- Requires maintenance
- Need to monitor instance health

### 3. AWS Lambda + EventBridge

**Architecture:**
- EventBridge triggers Lambda on schedule
- Lambda runs containerized Python environment
- Logs to CloudWatch

**Pros:**
- Serverless (no maintenance)
- Pay per execution
- Built-in monitoring

**Cons:**
- 15-minute timeout limit
- Cold starts
- More complex setup

### 4. GitHub Actions (Recommended)

**Setup:** Create `.github/workflows/daily-pipeline.yml`

**Pros:**
- Free tier (2,000 mins/month)
- Version controlled
- Built-in secret management
- Email notifications on failure
- No infrastructure

**Cons:**
- 6-hour job timeout
- Requires GitHub
- Public logs (for public repos)

### 5. AWS Batch

**Best for:**
- Large scale (all stocks, not just S&P 500)
- Complex scheduling needs
- Auto-scaling requirements

## Monitoring and Alerts

### Log Files
```bash
# Daily logs stored with timestamps
logs/daily_pipeline_20240615.log
logs/weekly_refresh_20240617.log
```

### Key Metrics to Monitor
1. **Pipeline Duration**: Should be < 30 min for S&P 500
2. **Records Loaded**: Should be > 0 for each ETL
3. **Error Count**: Any errors need investigation
4. **API Rate Limits**: Monitor FMP API usage

### Health Checks
```sql
-- Check latest data dates
SELECT 
    MAX(date) as latest_price_date,
    COUNT(DISTINCT company_key) as companies_with_prices
FROM ANALYTICS.FACT_DAILY_PRICES
WHERE date >= CURRENT_DATE - 7;

-- Check for gaps
WITH daily_companies AS (
    SELECT date, COUNT(DISTINCT company_key) as company_count
    FROM ANALYTICS.FACT_DAILY_PRICES
    WHERE date >= CURRENT_DATE - 30
    GROUP BY date
)
SELECT * FROM daily_companies
WHERE company_count < 400  -- Expect ~500 for S&P 500
ORDER BY date DESC;
```

## Troubleshooting

### Common Issues

1. **Pipeline Timeout**
   - Reduce symbols: Use top 100 instead of full S&P 500
   - Run financial ETL separately with smaller batches
   - Check Snowflake warehouse size

2. **Missing Data**
   - Check FMP API key validity
   - Verify network connectivity
   - Look for API rate limit errors

3. **Duplicate Data**
   - Pipeline uses MERGE statements (idempotent)
   - Safe to re-run after failures

4. **Stale Data**
   - Check cron job is running: `grep CRON /var/log/syslog`
   - Verify time zone settings
   - Check for holidays affecting schedule

### Manual Recovery
```bash
# Backfill missing date range
python scripts/run_daily_pipeline.py \
  --sp500 \
  --from-date 2024-06-01 \
  --to-date 2024-06-10

# Force reload specific symbols
python scripts/run_daily_pipeline.py \
  --symbols AAPL MSFT \
  --days-back 30 \
  --period quarterly
```

## Cost Considerations

### API Costs (FMP)
- Free tier: 250 requests/day
- Paid tiers: $14-299/month
- S&P 500 daily update: ~1,500 requests

### Compute Costs
- GitHub Actions: Free (2,000 mins/month)
- AWS Lambda: ~$5-10/month
- AWS EC2 (t3.medium): ~$30/month
- Local: Electricity only

### Storage Costs (Snowflake)
- Minimal for structured data
- ~$23/TB/month
- S&P 500 complete history: < 1 GB

## Best Practices

1. **Start Small**: Test with 5-10 symbols before S&P 500
2. **Monitor Early**: Set up logging before automation
3. **Use Staging**: Test schedules in dev before production
4. **Document Changes**: Update this guide with lessons learned
5. **Plan for Growth**: Design for 5,000+ symbols eventually

## Next Steps

1. Test pipeline manually with desired arguments
2. Choose deployment platform based on needs
3. Set up monitoring and alerts
4. Document any customizations
5. Plan disaster recovery procedures

---

*Last Updated: 2025-06-14*