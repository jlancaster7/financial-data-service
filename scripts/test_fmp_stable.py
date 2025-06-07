#!/usr/bin/env python3
"""
Test FMP API client with /stable/ endpoints
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from loguru import logger
from src.utils.config import Config
from src.api.fmp_client import FMPClient
from datetime import datetime, timedelta


def test_all_endpoints():
    """Test all FMP API endpoints with the /stable/ API"""
    logger.info("Testing FMP /stable/ API endpoints...")
    
    try:
        config = Config.load()
        logger.info(f"Using base URL: {config.fmp.base_url}")
        
        with FMPClient(config.fmp) as client:
            success_count = 0
            total_tests = 0
            
            # Test 1: Company Profile
            total_tests += 1
            try:
                logger.info("\n1. Testing Company Profile...")
                profile = client.get_company_profile("AAPL")
                logger.success(f"✓ Company: {profile.get('companyName', 'N/A')}")
                logger.info(f"  - Symbol: {profile.get('symbol')}")
                logger.info(f"  - Sector: {profile.get('sector')}")
                logger.info(f"  - Market Cap: ${profile.get('mktCap', 0):,.0f}")
                success_count += 1
            except Exception as e:
                logger.error(f"✗ Company Profile failed: {e}")
            
            # Test 2: Historical Prices
            total_tests += 1
            try:
                logger.info("\n2. Testing Historical Prices...")
                end_date = datetime.now().date()
                start_date = end_date - timedelta(days=7)
                prices = client.get_historical_prices("AAPL", from_date=start_date, to_date=end_date)
                logger.success(f"✓ Retrieved {len(prices)} days of price data")
                if prices:
                    logger.info(f"  - Latest date: {prices[0].get('date')}")
                    logger.info(f"  - Latest close: ${prices[0].get('close')}")
                    logger.info(f"  - Volume: {prices[0].get('volume'):,}")
                success_count += 1
            except Exception as e:
                logger.error(f"✗ Historical Prices failed: {e}")
            
            # Test 3: Income Statement
            total_tests += 1
            try:
                logger.info("\n3. Testing Income Statement...")
                income = client.get_income_statement("AAPL", period='annual', limit=1)
                logger.success(f"✓ Retrieved {len(income)} income statements")
                if income:
                    logger.info(f"  - Period: {income[0].get('date')}")
                    logger.info(f"  - Revenue: ${income[0].get('revenue', 0):,.0f}")
                    logger.info(f"  - Net Income: ${income[0].get('netIncome', 0):,.0f}")
                success_count += 1
            except Exception as e:
                logger.error(f"✗ Income Statement failed: {e}")
            
            # Test 4: Balance Sheet
            total_tests += 1
            try:
                logger.info("\n4. Testing Balance Sheet...")
                balance = client.get_balance_sheet("AAPL", period='annual', limit=1)
                logger.success(f"✓ Retrieved {len(balance)} balance sheets")
                if balance:
                    logger.info(f"  - Period: {balance[0].get('date')}")
                    logger.info(f"  - Total Assets: ${balance[0].get('totalAssets', 0):,.0f}")
                    logger.info(f"  - Total Equity: ${balance[0].get('totalEquity', 0):,.0f}")
                success_count += 1
            except Exception as e:
                logger.error(f"✗ Balance Sheet failed: {e}")
            
            # Test 5: Cash Flow
            total_tests += 1
            try:
                logger.info("\n5. Testing Cash Flow Statement...")
                cash_flow = client.get_cash_flow("AAPL", period='annual', limit=1)
                logger.success(f"✓ Retrieved {len(cash_flow)} cash flow statements")
                if cash_flow:
                    logger.info(f"  - Period: {cash_flow[0].get('date')}")
                    logger.info(f"  - Operating Cash Flow: ${cash_flow[0].get('operatingCashFlow', 0):,.0f}")
                    logger.info(f"  - Free Cash Flow: ${cash_flow[0].get('freeCashFlow', 0):,.0f}")
                success_count += 1
            except Exception as e:
                logger.error(f"✗ Cash Flow failed: {e}")
            
            # Test 6: Financial Ratios TTM
            total_tests += 1
            try:
                logger.info("\n6. Testing Financial Ratios TTM...")
                ratios = client.get_financial_ratios_ttm("AAPL")
                logger.success(f"✓ Retrieved financial ratios")
                logger.info(f"  - P/E Ratio: {ratios.get('priceToEarningsRatioTTM', 'N/A')}")
                logger.info(f"  - ROE: {ratios.get('returnOnEquityTTM', 'N/A')}")
                logger.info(f"  - Debt/Equity: {ratios.get('debtToEquityRatioTTM', 'N/A')}")
                success_count += 1
            except Exception as e:
                logger.error(f"✗ Financial Ratios TTM failed: {e}")
            
            # Test 7: Key Metrics TTM
            total_tests += 1
            try:
                logger.info("\n7. Testing Key Metrics TTM...")
                metrics = client.get_key_metrics_ttm("AAPL")
                logger.success(f"✓ Retrieved key metrics")
                logger.info(f"  - Market Cap: ${metrics.get('marketCap', 0):,.0f}")
                logger.info(f"  - Enterprise Value: ${metrics.get('enterpriseValueTTM', 0):,.0f}")
                logger.info(f"  - P/E Ratio: {metrics.get('peRatioTTM', 'N/A')}")
                success_count += 1
            except Exception as e:
                logger.error(f"✗ Key Metrics TTM failed: {e}")
            
            # Test 8: Historical Market Cap
            total_tests += 1
            try:
                logger.info("\n8. Testing Historical Market Cap...")
                market_cap = client.get_historical_market_cap("AAPL", limit=5)
                logger.success(f"✓ Retrieved {len(market_cap)} market cap records")
                if market_cap:
                    logger.info(f"  - Latest date: {market_cap[0].get('date')}")
                    logger.info(f"  - Market Cap: ${market_cap[0].get('marketCap', 0):,.0f}")
                success_count += 1
            except Exception as e:
                logger.error(f"✗ Historical Market Cap failed: {e}")
            
            # Test 9: S&P 500 Constituents
            total_tests += 1
            try:
                logger.info("\n9. Testing S&P 500 Constituents...")
                sp500 = client.get_sp500_constituents()
                logger.success(f"✓ Retrieved {len(sp500)} S&P 500 companies")
                if sp500:
                    logger.info(f"  - Sample: {sp500[0].get('symbol')} - {sp500[0].get('name')}")
                success_count += 1
            except Exception as e:
                logger.error(f"✗ S&P 500 Constituents failed: {e}")
            
            # Test 10: Treasury Rates
            total_tests += 1
            try:
                logger.info("\n10. Testing Treasury Rates...")
                treasury = client.get_treasury_rates()
                logger.success(f"✓ Retrieved {len(treasury)} treasury rate records")
                if treasury:
                    logger.info(f"  - Date: {treasury[0].get('date')}")
                    logger.info(f"  - 10-Year Rate: {treasury[0].get('year10', 'N/A')}%")
                success_count += 1
            except Exception as e:
                logger.error(f"✗ Treasury Rates failed: {e}")
            
            # Test 11: Economic Indicators
            total_tests += 1
            try:
                logger.info("\n11. Testing Economic Indicators (GDP)...")
                gdp = client.get_economic_indicator("GDP")
                logger.success(f"✓ Retrieved {len(gdp)} GDP records")
                if gdp:
                    logger.info(f"  - Date: {gdp[0].get('date')}")
                    logger.info(f"  - Value: ${gdp[0].get('value', 0):,.0f}")
                success_count += 1
            except Exception as e:
                logger.error(f"✗ Economic Indicators failed: {e}")
            
            # Summary
            logger.info(f"\n{'='*60}")
            logger.info(f"Test Summary: {success_count}/{total_tests} endpoints working")
            logger.info(f"{'='*60}")
            
            if success_count == total_tests:
                logger.success("✅ All endpoints working correctly!")
            elif success_count >= total_tests * 0.7:
                logger.warning("⚠️  Most endpoints working, some issues detected")
            else:
                logger.error("❌ Many endpoints failing")
            
    except Exception as e:
        logger.error(f"Test suite failed: {e}")


if __name__ == "__main__":
    test_all_endpoints()