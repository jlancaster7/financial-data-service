# Simplified Snowflake Data Pipeline

## Overview
Keep your sophisticated Snowflake schema but with a much simpler pipeline to populate it.

## Project Structure (Minimal)
```
snowflake-pipeline/
├── config.py                # Configuration
├── snowflake_connector.py   # Snowflake helper functions
├── fmp_loader.py           # FMP data fetching
├── daily_pipeline.py       # Main pipeline script
├── calculate_metrics.py    # Metric calculations
├── requirements.txt        # Dependencies
└── logs/                   # Log files
```

## Setup Snowflake (One-time)

Run your existing schema creation scripts as-is. They're perfect.

## Configuration

```python
# config.py
import os
from dotenv import load_dotenv

load_dotenv()

# Snowflake connection
SNOWFLAKE_CONFIG = {
    'account': os.getenv('SNOWFLAKE_ACCOUNT'),
    'user': os.getenv('SNOWFLAKE_USER'),
    'password': os.getenv('SNOWFLAKE_PASSWORD'),
    'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH'),
    'database': 'QUANT_PLATFORM',
    'schema': 'RAW',
    'role': os.getenv('SNOWFLAKE_ROLE', 'SYSADMIN')
}

# FMP API
FMP_API_KEY = os.getenv('FMP_API_KEY')

# Symbols to track (start small, expand later)
SYMBOLS = [
    'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'META',
    'NVDA', 'TSLA', 'JPM', 'JNJ', 'V'
]

# Simple settings
BATCH_SIZE = 100
LOG_LEVEL = 'INFO'
```

## Snowflake Connector Wrapper

```python
# snowflake_connector.py
import snowflake.connector
import pandas as pd
from contextlib import contextmanager
import logging

logger = logging.getLogger(__name__)

@contextmanager
def get_snowflake_connection(config):
    """Get Snowflake connection"""
    conn = None
    try:
        conn = snowflake.connector.connect(**config)
        yield conn
    finally:
        if conn:
            conn.close()

def write_to_snowflake(df, table_name, config, if_exists='append'):
    """Write pandas DataFrame to Snowflake"""
    with get_snowflake_connection(config) as conn:
        # Use Snowflake's write_pandas function for efficiency
        from snowflake.connector.pandas_tools import write_pandas
        
        success, nchunks, nrows, _ = write_pandas(
            conn,
            df,
            table_name,
            database=config['database'],
            schema=config['schema'],
            auto_create_table=False,
            overwrite=(if_exists == 'replace')
        )
        
        logger.info(f"Wrote {nrows} rows to {table_name}")
        return success

def execute_query(query, config):
    """Execute a query and return results as DataFrame"""
    with get_snowflake_connection(config) as conn:
        return pd.read_sql(query, conn)
```

## FMP Data Loader

```python
# fmp_loader.py
import requests
import pandas as pd
import time
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

class FMPLoader:
    def __init__(self, api_key):
        self.api_key = api_key
        self.base_url = "https://financialmodelingprep.com/api/v3"
        self.request_count = 0
        self.minute_start = time.time()
    
    def _rate_limit(self):
        """Simple rate limiting - stay under 750/minute"""
        self.request_count += 1
        
        # Reset counter each minute
        if time.time() - self.minute_start > 60:
            self.request_count = 1
            self.minute_start = time.time()
        
        # If approaching limit, wait
        if self.request_count >= 700:  # Safety margin
            wait_time = 60 - (time.time() - self.minute_start)
            if wait_time > 0:
                logger.info(f"Rate limit pause: {wait_time:.1f}s")
                time.sleep(wait_time)
            self.request_count = 0
            self.minute_start = time.time()
    
    def fetch_historical_prices(self, symbol, days=5):
        """Fetch historical prices and format for Snowflake"""
        self._rate_limit()
        
        url = f"{self.base_url}/historical-price-full/{symbol}"
        params = {"apikey": self.api_key}
        
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            
            data = response.json()
            historical = data.get('historical', [])[:days]
            
            # Convert to DataFrame with Snowflake schema
            if historical:
                df = pd.DataFrame(historical)
                df['symbol'] = symbol
                df['date'] = pd.to_datetime(df['date'])
                df['source'] = 'FMP'
                
                # Rename columns to match Snowflake schema
                column_mapping = {
                    'changePercent': 'change_percent',
                    'unadjustedVolume': 'unadjusted_volume',
                    'changeOverTime': 'change_over_time',
                    'adjClose': 'adj_close'
                }
                df.rename(columns=column_mapping, inplace=True)
                
                return df
            
        except Exception as e:
            logger.error(f"Error fetching prices for {symbol}: {e}")
            return pd.DataFrame()
    
    def fetch_company_profile(self, symbol):
        """Fetch company profile data"""
        self._rate_limit()
        
        url = f"{self.base_url}/profile/{symbol}"
        params = {"apikey": self.api_key}
        
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            
            data = response.json()
            if data:
                profile = data[0]
                # Map to Snowflake schema
                return {
                    'symbol': symbol,
                    'company_name': profile.get('companyName'),
                    'exchange': profile.get('exchange'),
                    'exchange_short': profile.get('exchangeShortName'),
                    'sector': profile.get('sector'),
                    'industry': profile.get('industry'),
                    'market_cap': profile.get('mktCap'),
                    'website': profile.get('website'),
                    'description': profile.get('description'),
                    'ceo': profile.get('ceo'),
                    'address': profile.get('address'),
                    'city': profile.get('city'),
                    'state': profile.get('state'),
                    'zip': profile.get('zip'),
                    'country': profile.get('country'),
                    'phone': profile.get('phone'),
                    'ipo_date': pd.to_datetime(profile.get('ipoDate')) if profile.get('ipoDate') else None,
                    'is_etf': profile.get('isEtf', False),
                    'is_actively_trading': profile.get('isActivelyTrading', True),
                    'source': 'FMP'
                }
                
        except Exception as e:
            logger.error(f"Error fetching profile for {symbol}: {e}")
            return None
    
    def fetch_fundamentals(self, symbol, period='quarter', limit=4):
        """Fetch fundamental data"""
        self._rate_limit()
        
        # Income statement
        income_url = f"{self.base_url}/income-statement/{symbol}"
        params = {"period": period, "limit": limit, "apikey": self.api_key}
        
        try:
            response = requests.get(income_url, params=params)
            response.raise_for_status()
            income_data = response.json()
            
            # Balance sheet
            self._rate_limit()
            balance_url = f"{self.base_url}/balance-sheet-statement/{symbol}"
            balance_response = requests.get(balance_url, params=params)
            balance_data = balance_response.json()
            
            # Cash flow
            self._rate_limit()
            cashflow_url = f"{self.base_url}/cash-flow-statement/{symbol}"
            cashflow_response = requests.get(cashflow_url, params=params)
            cashflow_data = cashflow_response.json()
            
            # Combine data
            fundamentals = []
            for i in range(min(len(income_data), len(balance_data), len(cashflow_data))):
                fundamentals.append({
                    'symbol': symbol,
                    'reporting_date': income_data[i].get('date'),
                    'fiscal_year': income_data[i].get('calendarYear'),
                    'fiscal_quarter': income_data[i].get('period', 'FY').replace('Q', ''),
                    
                    # Income statement
                    'revenue': income_data[i].get('revenue'),
                    'gross_profit': income_data[i].get('grossProfit'),
                    'operating_income': income_data[i].get('operatingIncome'),
                    'net_income': income_data[i].get('netIncome'),
                    'eps': income_data[i].get('eps'),
                    'eps_diluted': income_data[i].get('epsdiluted'),
                    
                    # Balance sheet
                    'total_assets': balance_data[i].get('totalAssets'),
                    'total_liabilities': balance_data[i].get('totalLiabilities'),
                    'total_equity': balance_data[i].get('totalStockholdersEquity'),
                    'cash_and_equivalents': balance_data[i].get('cashAndCashEquivalents'),
                    
                    # Cash flow
                    'operating_cash_flow': cashflow_data[i].get('operatingCashFlow'),
                    'capex': cashflow_data[i].get('capitalExpenditure'),
                    'free_cash_flow': cashflow_data[i].get('freeCashFlow'),
                    
                    'source': 'FMP'
                })
            
            return pd.DataFrame(fundamentals)
            
        except Exception as e:
            logger.error(f"Error fetching fundamentals for {symbol}: {e}")
            return pd.DataFrame()
```

## Daily Pipeline Script

```python
# daily_pipeline.py
import logging
from datetime import datetime
import pandas as pd
import sys

from config import SNOWFLAKE_CONFIG, FMP_API_KEY, SYMBOLS
from snowflake_connector import write_to_snowflake, execute_query
from fmp_loader import FMPLoader

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'logs/pipeline_{datetime.now():%Y%m%d}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def load_price_data(loader, symbols):
    """Load price data for all symbols"""
    all_prices = []
    
    for symbol in symbols:
        logger.info(f"Loading prices for {symbol}")
        df = loader.fetch_historical_prices(symbol, days=5)
        if not df.empty:
            all_prices.append(df)
    
    if all_prices:
        combined_df = pd.concat(all_prices, ignore_index=True)
        success = write_to_snowflake(combined_df, 'EQUITY_PRICES', SNOWFLAKE_CONFIG)
        logger.info(f"Loaded {len(combined_df)} price records")
        return success
    return False

def load_company_info(loader, symbols):
    """Load company information"""
    profiles = []
    
    for symbol in symbols:
        logger.info(f"Loading profile for {symbol}")
        profile = loader.fetch_company_profile(symbol)
        if profile:
            profiles.append(profile)
    
    if profiles:
        df = pd.DataFrame(profiles)
        # Handle the merge/update logic
        success = write_to_snowflake(df, 'COMPANY_INFO', SNOWFLAKE_CONFIG, if_exists='append')
        logger.info(f"Loaded {len(df)} company profiles")
        return success
    return False

def load_fundamentals(loader, symbols):
    """Load fundamental data"""
    all_fundamentals = []
    
    for symbol in symbols:
        logger.info(f"Loading fundamentals for {symbol}")
        df = loader.fetch_fundamentals(symbol, limit=4)  # Last 4 quarters
        if not df.empty:
            all_fundamentals.append(df)
    
    if all_fundamentals:
        combined_df = pd.concat(all_fundamentals, ignore_index=True)
        success = write_to_snowflake(combined_df, 'COMPANY_FUNDAMENTALS', SNOWFLAKE_CONFIG)
        logger.info(f"Loaded {len(combined_df)} fundamental records")
        return success
    return False

def run_daily_update():
    """Main pipeline execution"""
    logger.info("=" * 50)
    logger.info("Starting daily pipeline run")
    logger.info(f"Processing {len(SYMBOLS)} symbols")
    
    loader = FMPLoader(FMP_API_KEY)
    
    try:
        # 1. Load price data (run daily)
        load_price_data(loader, SYMBOLS)
        
        # 2. Load company info (run weekly - on Mondays)
        if datetime.now().weekday() == 0:
            logger.info("Monday - updating company info")
            load_company_info(loader, SYMBOLS)
        
        # 3. Load fundamentals (run weekly - on Sundays)
        if datetime.now().weekday() == 6:
            logger.info("Sunday - updating fundamentals")
            load_fundamentals(loader, SYMBOLS)
        
        logger.info("Daily pipeline completed successfully")
        
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    run_daily_update()
```

## Metrics Calculation Script

```python
# calculate_metrics.py
import logging
from config import SNOWFLAKE_CONFIG
from snowflake_connector import get_snowflake_connection

logger = logging.getLogger(__name__)

def calculate_daily_metrics():
    """Calculate metrics using Snowflake SQL"""
    
    queries = [
        # Calculate returns
        """
        INSERT INTO METRICS.DAILY_EQUITY_METRICS (
            symbol, date, return_1d, return_5d, return_1m, 
            return_3m, return_6m, return_1y
        )
        WITH price_data AS (
            SELECT 
                symbol,
                date,
                close,
                LAG(close, 1) OVER (PARTITION BY symbol ORDER BY date) as prev_close_1d,
                LAG(close, 5) OVER (PARTITION BY symbol ORDER BY date) as prev_close_5d,
                LAG(close, 21) OVER (PARTITION BY symbol ORDER BY date) as prev_close_1m,
                LAG(close, 63) OVER (PARTITION BY symbol ORDER BY date) as prev_close_3m,
                LAG(close, 126) OVER (PARTITION BY symbol ORDER BY date) as prev_close_6m,
                LAG(close, 252) OVER (PARTITION BY symbol ORDER BY date) as prev_close_1y
            FROM RAW.EQUITY_PRICES
            WHERE date >= DATEADD('year', -2, CURRENT_DATE())
        )
        SELECT
            symbol,
            date,
            (close / NULLIF(prev_close_1d, 0) - 1) as return_1d,
            (close / NULLIF(prev_close_5d, 0) - 1) as return_5d,
            (close / NULLIF(prev_close_1m, 0) - 1) as return_1m,
            (close / NULLIF(prev_close_3m, 0) - 1) as return_3m,
            (close / NULLIF(prev_close_6m, 0) - 1) as return_6m,
            (close / NULLIF(prev_close_1y, 0) - 1) as return_1y
        FROM price_data
        WHERE date = CURRENT_DATE()
        """,
        
        # Calculate moving averages
        """
        MERGE INTO METRICS.DAILY_EQUITY_METRICS t
        USING (
            SELECT 
                symbol,
                date,
                AVG(close) OVER (PARTITION BY symbol ORDER BY date ROWS 19 PRECEDING) as sma_20,
                AVG(close) OVER (PARTITION BY symbol ORDER BY date ROWS 49 PRECEDING) as sma_50,
                AVG(close) OVER (PARTITION BY symbol ORDER BY date ROWS 199 PRECEDING) as sma_200
            FROM RAW.EQUITY_PRICES
            WHERE date = CURRENT_DATE()
        ) s
        ON t.symbol = s.symbol AND t.date = s.date
        WHEN MATCHED THEN UPDATE SET
            t.sma_20 = s.sma_20,
            t.sma_50 = s.sma_50,
            t.sma_200 = s.sma_200
        """,
        
        # Calculate volatility
        """
        MERGE INTO METRICS.DAILY_EQUITY_METRICS t
        USING (
            SELECT 
                symbol,
                MAX(date) as date,
                STDDEV(return_1d) * SQRT(252) as volatility_20d
            FROM (
                SELECT 
                    symbol,
                    date,
                    (close / LAG(close) OVER (PARTITION BY symbol ORDER BY date) - 1) as return_1d
                FROM RAW.EQUITY_PRICES
                WHERE date >= DATEADD('day', -30, CURRENT_DATE())
            )
            WHERE return_1d IS NOT NULL
            GROUP BY symbol
        ) s
        ON t.symbol = s.symbol AND t.date = CURRENT_DATE()
        WHEN MATCHED THEN UPDATE SET
            t.volatility_20d = s.volatility_20d
        """
    ]
    
    with get_snowflake_connection(SNOWFLAKE_CONFIG) as conn:
        cursor = conn.cursor()
        for query in queries:
            try:
                cursor.execute(query)
                logger.info(f"Executed metric calculation query")
            except Exception as e:
                logger.error(f"Error in metric calculation: {e}")

if __name__ == "__main__":
    calculate_daily_metrics()
```

## Simple Monitoring

```python
# monitor.py
import logging
from datetime import datetime, timedelta
from config import SNOWFLAKE_CONFIG
from snowflake_connector import execute_query

logger = logging.getLogger(__name__)

def check_data_freshness():
    """Simple data freshness check"""
    query = """
    SELECT 
        'EQUITY_PRICES' as table_name,
        MAX(date) as last_date,
        COUNT(DISTINCT symbol) as symbol_count,
        DATEDIFF('hour', MAX(load_timestamp), CURRENT_TIMESTAMP()) as hours_since_load
    FROM RAW.EQUITY_PRICES
    WHERE load_timestamp >= DATEADD('day', -3, CURRENT_TIMESTAMP())
    """
    
    results = execute_query(query, SNOWFLAKE_CONFIG)
    
    for _, row in results.iterrows():
        if row['hours_since_load'] > 24:
            logger.warning(f"Data is stale: {row['table_name']} last updated {row['hours_since_load']} hours ago")
        
        logger.info(f"{row['table_name']}: {row['symbol_count']} symbols, last date: {row['last_date']}")

if __name__ == "__main__":
    check_data_freshness()
```

## Setup Instructions

1. **Install dependencies**:
```bash
pip install snowflake-connector-python[pandas] requests python-dotenv pandas
```

2. **Environment variables** (.env file):
```
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_USER=your_user
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_ROLE=SYSADMIN
FMP_API_KEY=your_fmp_key
```

3. **Create Snowflake objects** (run your existing SQL scripts)

4. **Test the pipeline**:
```bash
python daily_pipeline.py
```

5. **Schedule with cron**:
```bash
# Daily price updates at 4:30 PM EST
30 16 * * 1-5 cd /path/to/project && python daily_pipeline.py

# Calculate metrics at 5:00 PM EST
0 17 * * 1-5 cd /path/to/project && python calculate_metrics.py

# Check data freshness at 6:00 PM EST
0 18 * * 1-5 cd /path/to/project && python monitor.py
```

## Key Simplifications

- **No Airflow**: Just cron jobs
- **No complex monitoring**: Simple logging and basic checks
- **Direct Snowflake writes**: Using snowflake-connector-python
- **Simple rate limiting**: Basic counter approach
- **Minimal error handling**: Log and continue
- **No separate services**: Everything runs as scripts

## Expanding Later

1. Add more symbols gradually (S&P 500)
2. Add email alerts for failures
3. Create a simple Streamlit dashboard
4. Add more data sources
5. Implement the RSI calculation