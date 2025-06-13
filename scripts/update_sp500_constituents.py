#!/usr/bin/env python3
"""
Update S&P 500 Constituents List

This script fetches the latest S&P 500 constituents from FMP API
and updates the local reference list used by the pipeline.
"""
import sys
import json
from pathlib import Path
from datetime import datetime
from loguru import logger

# Add the src directory to the path
sys.path.append(str(Path(__file__).parent.parent))

from src.utils.config import Config
from src.api.fmp_client import FMPClient


def main():
    """Main function to update S&P 500 constituents"""
    logger.info("Starting S&P 500 constituents update")
    
    try:
        # Load configuration
        config = Config.load()
        
        # Create FMP client
        fmp_client = FMPClient(config.fmp)
        
        # Fetch current S&P 500 constituents
        logger.info("Fetching S&P 500 constituents from FMP API...")
        constituents = fmp_client.get_sp500_constituents()
        
        if not constituents:
            logger.error("No constituents data received from API")
            return 1
        
        logger.info(f"Retrieved {len(constituents)} S&P 500 constituents")
        
        # Extract symbols
        symbols = [c['symbol'] for c in constituents if c.get('symbol')]
        symbols.sort()
        
        # Create output file
        output_file = Path(__file__).parent.parent / 'config' / 'sp500_constituents.json'
        output_file.parent.mkdir(exist_ok=True)
        
        # Prepare data
        data = {
            'last_updated': datetime.now().isoformat(),
            'count': len(symbols),
            'symbols': symbols,
            'constituents': constituents
        }
        
        # Write to file
        with open(output_file, 'w') as f:
            json.dump(data, f, indent=2)
        
        logger.info(f"Successfully updated S&P 500 constituents list at {output_file}")
        logger.info(f"Total symbols: {len(symbols)}")
        
        # Show sample
        logger.info(f"Sample symbols: {symbols[:10]}")
        
        # Check for changes
        if output_file.exists():
            # Could implement comparison with previous version here
            pass
        
        return 0
        
    except Exception as e:
        logger.error(f"Failed to update S&P 500 constituents: {e}")
        return 2


if __name__ == "__main__":
    sys.exit(main())