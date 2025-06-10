#!/usr/bin/env python3
"""
Test script to measure timing of each ETL component
"""
import sys
import time
from pathlib import Path
from datetime import datetime, timedelta
from loguru import logger

sys.path.append(str(Path(__file__).parent.parent))

from src.utils.config import Config
from src.etl.company_etl import CompanyETL
from src.etl.historical_price_etl import HistoricalPriceETL
from src.etl.financial_statement_etl import FinancialStatementETL
from src.etl.ttm_calculation_etl import TTMCalculationETL
from src.etl.financial_ratio_etl import FinancialRatioETL
from src.etl.market_metrics_etl import MarketMetricsETL


def time_etl_component(name: str, func, *args, **kwargs):
    """Time an ETL component execution"""
    logger.info(f"\n{'='*60}")
    logger.info(f"Testing {name}")
    logger.info(f"{'='*60}")
    
    start_time = time.time()
    try:
        result = func(*args, **kwargs)
        elapsed = time.time() - start_time
        logger.info(f"✓ {name} completed in {elapsed:.2f} seconds")
        return elapsed, result
    except Exception as e:
        elapsed = time.time() - start_time
        logger.error(f"✗ {name} failed after {elapsed:.2f} seconds: {e}")
        return elapsed, None


def main():
    """Run timing tests"""
    # Test configuration
    TEST_SYMBOLS = ['AAPL']  # Single symbol for consistent timing
    
    # Load configuration
    config = Config.load()
    
    # Track timings
    timings = {}
    
    logger.info("Starting ETL pipeline timing tests...")
    logger.info(f"Test symbols: {TEST_SYMBOLS}")
    
    # 1. Company ETL
    etl = CompanyETL(config)
    elapsed, result = time_etl_component(
        "Company ETL (using run method)",
        etl.run,
        symbols=TEST_SYMBOLS
    )
    timings['company_run'] = elapsed
    
    # Test manual extract/transform/load
    etl2 = CompanyETL(config)
    start = time.time()
    data = etl2.extract(symbols=TEST_SYMBOLS)
    extract_time = time.time() - start
    
    transformed = etl2.transform(data)
    transform_time = time.time() - start - extract_time
    
    loaded = etl2.load(transformed)
    load_time = time.time() - start - extract_time - transform_time
    
    total_manual = time.time() - start
    logger.info(f"Company ETL (manual): Extract={extract_time:.2f}s, Transform={transform_time:.2f}s, Load={load_time:.2f}s, Total={total_manual:.2f}s")
    timings['company_manual'] = total_manual
    
    # 2. Price ETL
    etl = HistoricalPriceETL(config)
    from_date = datetime.now().date() - timedelta(days=30)
    to_date = datetime.now().date()
    
    elapsed, result = time_etl_component(
        "Price ETL (using run method)",
        etl.run,
        symbols=TEST_SYMBOLS,
        from_date=from_date,
        to_date=to_date
    )
    timings['price_run'] = elapsed
    
    # 3. Financial ETL (Annual)
    etl = FinancialStatementETL(config)
    elapsed, result = time_etl_component(
        "Financial ETL Annual (using run method)",
        etl.run,
        symbols=TEST_SYMBOLS,
        period='annual',
        limit=5
    )
    timings['financial_annual_run'] = elapsed
    
    # 4. Financial ETL (Quarterly)
    etl = FinancialStatementETL(config)
    elapsed, result = time_etl_component(
        "Financial ETL Quarterly (using run method)", 
        etl.run,
        symbols=TEST_SYMBOLS,
        period='quarterly',
        limit=8
    )
    timings['financial_quarterly_run'] = elapsed
    
    # 5. TTM Calculation ETL
    etl = TTMCalculationETL(config)
    elapsed, result = time_etl_component(
        "TTM Calculation ETL",
        etl.run,
        symbols=TEST_SYMBOLS
    )
    timings['ttm_run'] = elapsed
    
    # 6. Ratio ETL
    etl = FinancialRatioETL(config)
    elapsed, result = time_etl_component(
        "Financial Ratio ETL",
        etl.run,
        symbols=TEST_SYMBOLS
    )
    timings['ratio_run'] = elapsed
    
    # 7. Market Metrics ETL
    etl = MarketMetricsETL(config)
    elapsed, result = time_etl_component(
        "Market Metrics ETL",
        etl.run,
        symbols=TEST_SYMBOLS,
        start_date=str(from_date),
        end_date=str(to_date)
    )
    timings['market_metrics_run'] = elapsed
    
    # Summary
    logger.info(f"\n{'='*60}")
    logger.info("TIMING SUMMARY")
    logger.info(f"{'='*60}")
    
    total_time = sum(v for v in timings.values() if v is not None)
    for component, elapsed in timings.items():
        if elapsed is not None:
            percentage = (elapsed / total_time) * 100
            logger.info(f"{component:30} {elapsed:7.2f}s ({percentage:5.1f}%)")
    
    logger.info(f"{'-'*50}")
    logger.info(f"{'Total time':30} {total_time:7.2f}s")
    
    # Check for bottlenecks
    logger.info(f"\n{'='*60}")
    logger.info("ANALYSIS")
    logger.info(f"{'='*60}")
    
    slowest = max(timings.items(), key=lambda x: x[1] if x[1] is not None else 0)
    logger.info(f"Slowest component: {slowest[0]} ({slowest[1]:.2f}s)")
    
    if total_time > 120:
        logger.warning(f"Total time exceeds 2 minutes timeout threshold!")
        logger.info("Recommendations:")
        logger.info("- Consider running ETLs in parallel where possible")
        logger.info("- Batch API calls more efficiently")
        logger.info("- Cache or skip unchanged data")


if __name__ == "__main__":
    main()