#!/usr/bin/env python3
"""
Load historical data for top N stocks by market cap

This script:
1. Fetches company profiles to get market caps
2. Sorts by market cap and selects top N
3. Loads all available historical data
"""
import sys
import json
import argparse
from pathlib import Path
from datetime import datetime, timedelta
from loguru import logger

# Add the src directory to the path
sys.path.append(str(Path(__file__).parent.parent))

from src.utils.config import Config
from src.api.fmp_client import FMPClient
from src.db.snowflake_connector import SnowflakeConnector


def get_top_stocks_by_market_cap(fmp_client: FMPClient, n: int = 50):
    """Get top N stocks by market cap from S&P 500"""
    logger.info("Loading S&P 500 constituents...")
    
    # Load S&P 500 symbols
    sp500_file = Path(__file__).parent.parent / 'config' / 'sp500_constituents.json'
    with open(sp500_file, 'r') as f:
        data = json.load(f)
    
    symbols = data['symbols']
    logger.info(f"Found {len(symbols)} S&P 500 symbols")
    
    # Fetch company profiles one by one (since we don't have batch endpoint info)
    logger.info("Fetching company profiles to get market caps...")
    logger.info("This will take a few minutes due to API rate limits...")
    all_profiles = []
    
    # Let's just get a sample to identify top stocks quickly
    # We'll use well-known large cap stocks plus some others
    sample_symbols = [
        'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'NVDA', 'META', 'TSLA', 'BRK-B', 'V', 'JNJ',
        'WMT', 'JPM', 'PG', 'MA', 'HD', 'DIS', 'BAC', 'ADBE', 'NFLX', 'CRM',
        'PFE', 'TMO', 'ABT', 'KO', 'PEP', 'CSCO', 'AVGO', 'ACN', 'MRK', 'CVX',
        'COST', 'WFC', 'ABBV', 'LLY', 'DHR', 'TXN', 'UNH', 'VZ', 'INTC', 'COP',
        'PM', 'T', 'MS', 'UNP', 'RTX', 'NEE', 'BMY', 'QCOM', 'HON', 'IBM',
        'GS', 'BLK', 'SPGI', 'CAT', 'SBUX', 'AMT', 'GE', 'CVS', 'SCHW', 'MO',
        'AXP', 'AMGN', 'DE', 'LMT', 'SYK', 'MDLZ', 'GILD', 'MMC', 'TJX', 'ADI'
    ]
    
    # For full S&P 500, uncomment below (will take ~8-10 minutes)
    # sample_symbols = symbols[:200]  # Top 200 should cover most large caps
    
    for i, symbol in enumerate(sample_symbols):
        if i % 10 == 0:
            logger.info(f"Progress: {i}/{len(sample_symbols)} symbols fetched...")
        try:
            profile = fmp_client.get_company_profile(symbol)
            if isinstance(profile, dict):
                all_profiles.append(profile)
            else:
                logger.warning(f"Unexpected response for {symbol}")
        except Exception as e:
            logger.warning(f"Error fetching {symbol}: {e}")
    
    # Sort by market cap
    logger.info(f"Retrieved {len(all_profiles)} company profiles")
    all_profiles.sort(key=lambda x: x.get('marketCap', 0) or 0, reverse=True)
    
    # Get top N
    top_stocks = all_profiles[:n]
    
    # Display top stocks
    logger.info(f"\nTop {n} stocks by market cap:")
    for i, stock in enumerate(top_stocks, 1):
        market_cap_b = (stock.get('marketCap', 0) or 0) / 1_000_000_000
        logger.info(f"{i:2d}. {stock['symbol']:5s} - {stock.get('companyName', 'Unknown'):30s} - ${market_cap_b:,.1f}B")
    
    return [stock['symbol'] for stock in top_stocks]


def generate_load_commands(symbols: list, years_back: int = 5):
    """Generate commands to load data for given symbols"""
    
    # Calculate date range
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=365 * years_back)).strftime('%Y-%m-%d')
    
    commands = []
    
    # For large loads, break into smaller batches to avoid timeouts
    batch_size = 5
    
    for i in range(0, len(symbols), batch_size):
        batch = symbols[i:i + batch_size]
        symbol_str = ' '.join(batch)
        
        # Command for full pipeline with historical data
        cmd = f"python scripts/run_daily_pipeline.py --symbols {symbol_str} --from-date {start_date} --to-date {end_date} --period quarterly --limit 20"
        commands.append(cmd)
    
    return commands


def main():
    parser = argparse.ArgumentParser(description="Load top stocks by market cap")
    parser.add_argument(
        "--top-n",
        type=int,
        default=50,
        help="Number of top stocks to load (default: 50)"
    )
    parser.add_argument(
        "--years-back",
        type=int,
        default=5,
        help="Years of historical data to load (default: 5)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Just show what would be loaded without executing"
    )
    parser.add_argument(
        "--generate-commands",
        action="store_true",
        help="Generate commands to run separately"
    )
    
    args = parser.parse_args()
    
    # Load configuration
    config = Config.load()
    fmp_client = FMPClient(config.fmp)
    
    # Get top stocks
    top_symbols = get_top_stocks_by_market_cap(fmp_client, args.top_n)
    
    if args.generate_commands:
        # Generate commands for manual execution
        commands = generate_load_commands(top_symbols, args.years_back)
        
        logger.info(f"\n{'='*80}")
        logger.info(f"Generated {len(commands)} commands to load data for top {args.top_n} stocks")
        logger.info(f"{'='*80}\n")
        
        # Save to file
        output_file = Path(__file__).parent / 'load_commands.sh'
        with open(output_file, 'w') as f:
            f.write("#!/bin/bash\n")
            f.write("# Commands to load top stocks by market cap\n")
            f.write(f"# Generated on {datetime.now()}\n")
            f.write(f"# Top {args.top_n} stocks, {args.years_back} years of data\n\n")
            
            for i, cmd in enumerate(commands, 1):
                f.write(f"echo \"Running batch {i}/{len(commands)}...\"\n")
                f.write(f"{cmd}\n")
                f.write("echo \"Batch complete. Sleeping 5 seconds...\"\n")
                f.write("sleep 5\n\n")
        
        logger.info(f"Commands saved to: {output_file}")
        logger.info("\nTo run all commands:")
        logger.info(f"  chmod +x {output_file}")
        logger.info(f"  ./{output_file}")
        
        # Also print commands for review
        logger.info("\nGenerated commands:")
        for i, cmd in enumerate(commands, 1):
            logger.info(f"\n# Batch {i}/{len(commands)}:")
            logger.info(cmd)
    
    elif not args.dry_run:
        # Execute loading for top stocks
        logger.warning("Direct execution not implemented to avoid timeouts.")
        logger.warning("Please use --generate-commands flag to create executable script.")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())