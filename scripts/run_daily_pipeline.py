#!/usr/bin/env python3
"""
Main Pipeline Orchestrator - Run all ETL pipelines in proper sequence

This script orchestrates the execution of all ETL pipelines to ensure
data is loaded in the correct order with proper dependency management.
"""
import sys
import argparse
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Dict, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
from loguru import logger

# Add the src directory to the path
sys.path.append(str(Path(__file__).parent.parent))

from src.utils.config import Config
from src.api.fmp_client import FMPClient
from src.db.snowflake_connector import SnowflakeConnector
from src.etl.company_etl import CompanyETL
from src.etl.historical_price_etl import HistoricalPriceETL
from src.etl.financial_statement_etl import FinancialStatementETL
from src.etl.financial_ratio_etl import FinancialRatioETL
from src.etl.ttm_calculation_etl import TTMCalculationETL
from src.etl.market_metrics_etl import MarketMetricsETL
from src.etl.base_etl import ETLStatus


class PipelineOrchestrator:
    """Orchestrates the execution of all ETL pipelines"""
    
    def __init__(self, config: Config, dry_run: bool = False):
        self.config = config
        self.dry_run = dry_run
        self.fmp_client = FMPClient(config.fmp) if not dry_run else None
        # Create pooled connection for shared use
        self.snowflake = SnowflakeConnector(
            config.snowflake, 
            use_pooling=True
        ) if not dry_run else None
        self.results = {}
        self.start_time = None
        
    def get_symbols(self, args) -> List[str]:
        """Get symbols to process based on arguments"""
        if args.symbols:
            logger.info(f"Using provided symbols: {args.symbols}")
            return args.symbols
        elif args.sp500 and self.fmp_client:
            logger.info("Fetching S&P 500 constituents...")
            try:
                constituents = self.fmp_client.get_sp500_constituents()
                symbols = [c['symbol'] for c in constituents if c.get('symbol')]
                logger.info(f"Retrieved {len(symbols)} S&P 500 symbols")
                return symbols
            except Exception as e:
                logger.error(f"Failed to get S&P 500 constituents: {e}")
                return []
        else:
            # Default test symbols
            logger.warning("No symbols specified, using default test symbols")
            return ['AAPL', 'MSFT', 'GOOGL']
    
    def run_company_etl(self, symbols: List[str], args) -> bool:
        """Run company profile ETL"""
        logger.info(f"{'[DRY RUN] ' if self.dry_run else ''}Starting Company Profile ETL...")
        
        if self.dry_run:
            logger.info(f"Would process {len(symbols)} company profiles")
            self.results['company'] = {'status': 'dry_run', 'symbols': len(symbols)}
            return True
        
        try:
            # Create ETL with shared connector
            etl = CompanyETL(self.config)
            # Replace the ETL's snowflake connector with our shared one
            etl.snowflake = self.snowflake
            
            # Extract data
            company_data = etl.extract(symbols=symbols, load_to_analytics=not args.skip_analytics)
            etl.result.records_extracted = len(company_data)
            
            # Transform data
            transformed_data = etl.transform(company_data)
            etl.result.records_transformed = len(transformed_data.get('staging', []))
            
            # Load data
            records_loaded = etl.load(transformed_data)
            etl.result.records_loaded = records_loaded
            
            # Set status
            if etl.result.errors:
                etl.result.status = ETLStatus.PARTIAL
            else:
                etl.result.status = ETLStatus.SUCCESS
            
            # Get the result
            result = etl.result
            
            self.results['company'] = {
                'status': result.status.value,
                'records_loaded': result.records_loaded,
                'errors': result.errors
            }
            
            success = result.status in [ETLStatus.SUCCESS, ETLStatus.PARTIAL]
            if success:
                logger.info(f"✓ Company ETL completed: {result.records_loaded} records loaded")
            else:
                logger.error(f"✗ Company ETL failed: {result.errors}")
            
            return success
            
        except Exception as e:
            logger.error(f"Company ETL failed with exception: {e}")
            self.results['company'] = {'status': 'failed', 'error': str(e)}
            return False
    
    def run_price_etl(self, symbols: List[str], args) -> bool:
        """Run historical price ETL"""
        logger.info(f"{'[DRY RUN] ' if self.dry_run else ''}Starting Historical Price ETL...")
        
        # Determine date range
        if args.from_date:
            from_date = datetime.strptime(args.from_date, '%Y-%m-%d').date()
        else:
            from_date = datetime.now().date() - timedelta(days=args.days_back)
        
        to_date = datetime.now().date() if not args.to_date else datetime.strptime(args.to_date, '%Y-%m-%d').date()
        
        if self.dry_run:
            logger.info(f"Would process {len(symbols)} symbols for dates {from_date} to {to_date}")
            self.results['price'] = {'status': 'dry_run', 'symbols': len(symbols), 'date_range': f"{from_date} to {to_date}"}
            return True
        
        try:
            # Create ETL with shared connector
            etl = HistoricalPriceETL(self.config)
            # Replace the ETL's snowflake connector with our shared one
            etl.snowflake = self.snowflake
            
            # Extract data
            price_data = etl.extract(symbols=symbols, from_date=from_date, to_date=to_date)
            etl.result.records_extracted = len(price_data)
            
            # Transform data
            transformed_data = etl.transform(price_data)
            etl.result.records_transformed = len(transformed_data.get('staging', []))
            
            # Load data
            records_loaded = etl.load(transformed_data)
            etl.result.records_loaded = records_loaded
            
            # Update analytics layer if requested
            if not args.skip_analytics and records_loaded > 0:
                logger.info("Updating FACT_DAILY_PRICES...")
                etl.update_fact_table(symbols, from_date)
            
            # Set status
            if etl.result.errors:
                etl.result.status = ETLStatus.PARTIAL
            else:
                etl.result.status = ETLStatus.SUCCESS
            
            # Get the result
            result = etl.result
            
            self.results['price'] = {
                'status': result.status.value,
                'records_loaded': result.records_loaded,
                'errors': result.errors
            }
            
            success = result.status in [ETLStatus.SUCCESS, ETLStatus.PARTIAL]
            if success:
                logger.info(f"✓ Price ETL completed: {result.records_loaded} records loaded")
            else:
                logger.error(f"✗ Price ETL failed: {result.errors}")
            
            return success
            
        except Exception as e:
            logger.error(f"Price ETL failed with exception: {e}")
            self.results['price'] = {'status': 'failed', 'error': str(e)}
            return False
    
    def run_financial_etl(self, symbols: List[str], args) -> bool:
        """Run financial statement ETL"""
        logger.info(f"{'[DRY RUN] ' if self.dry_run else ''}Starting Financial Statement ETL...")
        
        if self.dry_run:
            logger.info(f"Would process {len(symbols)} symbols for {args.period} periods (limit: {args.limit})")
            self.results['financial'] = {'status': 'dry_run', 'symbols': len(symbols), 'period': args.period}
            return True
        
        try:
            # Create ETL with shared connector
            etl = FinancialStatementETL(self.config)
            # Replace the ETL's snowflake connector with our shared one
            etl.snowflake = self.snowflake
            
            # Extract data
            statement_data = etl.extract(symbols=symbols, period=args.period, limit=args.limit)
            # Count total records extracted across all statement types
            total_extracted = sum(len(v) for v in statement_data.values())
            etl.result.records_extracted = total_extracted
            
            # Transform data
            transformed_data = etl.transform(statement_data)
            # Count total records transformed
            total_transformed = sum(len(v) for k, v in transformed_data.items() if k.startswith('staging_'))
            etl.result.records_transformed = total_transformed
            
            # Load data
            records_loaded = etl.load(transformed_data)
            etl.result.records_loaded = records_loaded
            
            # Update analytics layer if requested
            if not args.skip_analytics and records_loaded > 0:
                logger.info("Updating FACT_FINANCIALS...")
                etl.update_fact_table(symbols)
            
            # Set status
            if etl.result.errors:
                etl.result.status = ETLStatus.PARTIAL
            else:
                etl.result.status = ETLStatus.SUCCESS
            
            # Get the result
            result = etl.result
            
            self.results['financial'] = {
                'status': result.status.value,
                'records_loaded': result.records_loaded,
                'errors': result.errors
            }
            
            success = result.status in [ETLStatus.SUCCESS, ETLStatus.PARTIAL]
            if success:
                logger.info(f"✓ Financial ETL completed: {result.records_loaded} records loaded")
            else:
                logger.error(f"✗ Financial ETL failed: {result.errors}")
            
            return success
            
        except Exception as e:
            logger.error(f"Financial ETL failed with exception: {e}")
            self.results['financial'] = {'status': 'failed', 'error': str(e)}
            return False
    
    def run_ratio_etl(self, symbols: List[str], args) -> bool:
        """Run financial ratio ETL"""
        logger.info(f"{'[DRY RUN] ' if self.dry_run else ''}Starting Financial Ratio ETL...")
        
        if self.dry_run:
            logger.info("Would calculate financial ratios from FACT_FINANCIALS")
            self.results['ratio'] = {'status': 'dry_run'}
            return True
        
        try:
            # Create ETL with shared connector
            etl = FinancialRatioETL(self.config)
            # Replace the ETL's snowflake connector with our shared one
            etl.snowflake = self.snowflake
            
            # Run the ETL
            result = etl.run(
                symbols=symbols if not args.all_symbols else None,
                fiscal_start_date=args.from_date,
                fiscal_end_date=args.to_date
            )
            
            self.results['ratio'] = result
            
            success = result.get('status') == 'success'
            if success:
                logger.info(f"✓ Ratio ETL completed: {result.get('records_loaded', 0)} ratios calculated")
            else:
                logger.error(f"✗ Ratio ETL failed: {result.get('error', 'Unknown error')}")
            
            return success
            
        except Exception as e:
            logger.error(f"Ratio ETL failed with exception: {e}")
            self.results['ratio'] = {'status': 'failed', 'error': str(e)}
            return False
    
    def run_ttm_calculation_etl(self, symbols: List[str], args) -> bool:
        """Run TTM calculation ETL"""
        logger.info(f"{'[DRY RUN] ' if self.dry_run else ''}Starting TTM Calculation ETL...")
        
        if self.dry_run:
            logger.info("Would calculate TTM financial metrics")
            self.results['ttm_calculation'] = {'status': 'dry_run'}
            return True
        
        try:
            # Create ETL with shared connector
            etl = TTMCalculationETL(self.config)
            # Replace the ETL's snowflake connector with our shared one
            etl.snowflake = self.snowflake
            
            # Run the ETL
            result = etl.run(symbols=symbols if not args.all_symbols else None)
            
            self.results['ttm_calculation'] = result
            
            success = result.get('status') == 'success'
            if success:
                logger.info(f"✓ TTM Calculation ETL completed: {result.get('records_loaded', 0)} TTM records created")
            else:
                logger.error(f"✗ TTM Calculation ETL failed: {result.get('error', 'Unknown error')}")
            
            return success
            
        except Exception as e:
            logger.error(f"TTM Calculation ETL failed with exception: {e}")
            self.results['ttm_calculation'] = {'status': 'failed', 'error': str(e)}
            return False
    
    def run_market_metrics_etl(self, symbols: List[str], args) -> bool:
        """Run market metrics ETL"""
        logger.info(f"{'[DRY RUN] ' if self.dry_run else ''}Starting Market Metrics ETL...")
        
        if self.dry_run:
            logger.info("Would calculate daily market metrics")
            self.results['market_metrics'] = {'status': 'dry_run'}
            return True
        
        try:
            # Create ETL with shared connector
            etl = MarketMetricsETL(self.config)
            # Replace the ETL's snowflake connector with our shared one
            etl.snowflake = self.snowflake
            
            # Determine date range for market metrics
            if args.from_date:
                start_date = args.from_date
            else:
                start_date = (datetime.now() - timedelta(days=args.days_back)).strftime('%Y-%m-%d')
            
            end_date = args.to_date if args.to_date else datetime.now().strftime('%Y-%m-%d')
            
            # Run the ETL
            result = etl.run(
                symbols=symbols if not args.all_symbols else None,
                start_date=start_date,
                end_date=end_date
            )
            
            self.results['market_metrics'] = result
            
            success = result.get('status') == 'success'
            if success:
                logger.info(f"✓ Market Metrics ETL completed: {result.get('records_loaded', 0)} metrics calculated")
            else:
                logger.error(f"✗ Market Metrics ETL failed: {result.get('error', 'Unknown error')}")
            
            return success
            
        except Exception as e:
            logger.error(f"Market Metrics ETL failed with exception: {e}")
            self.results['market_metrics'] = {'status': 'failed', 'error': str(e)}
            return False
    
    def run_daily_update(self, args) -> int:
        """
        Run all ETL pipelines in the correct sequence
        
        Returns:
            Exit code: 0 for success, 1 for partial success, 2 for failure
        """
        import time
        pipeline_start_time = time.time()
        start_datetime = datetime.now()
        logger.info(f"{'='*60}")
        logger.info(f"Starting Daily Pipeline Update at {start_datetime}")
        logger.info(f"Dry Run: {self.dry_run}")
        logger.info(f"Using Connection Pooling: {self.snowflake.use_pooling if self.snowflake else 'N/A'}")
        logger.info(f"{'='*60}")
        
        # Get symbols to process
        symbols = self.get_symbols(args)
        if not symbols:
            logger.error("No symbols to process")
            return 2
        
        # Track successes
        all_success = True
        any_success = False
        
        # Run pipelines in dependency order
        pipelines = []
        
        if not args.skip_company:
            pipelines.append(('company', self.run_company_etl))
        
        if not args.skip_price:
            pipelines.append(('price', self.run_price_etl))
        
        if not args.skip_financial:
            pipelines.append(('financial', self.run_financial_etl))
        
        # TTM calculation should run after financial data is loaded
        if not args.skip_financial and not args.skip_ttm:
            pipelines.append(('ttm_calculation', self.run_ttm_calculation_etl))
        
        if not args.skip_ratio:
            pipelines.append(('ratio', self.run_ratio_etl))
        
        if not args.skip_market_metrics:
            pipelines.append(('market_metrics', self.run_market_metrics_etl))
        
        # Group pipelines by dependencies
        # Group 1: Independent ETLs (can run in parallel)
        independent_etls = []
        if 'company' in [name for name, _ in pipelines]:
            independent_etls.append(('company', self.run_company_etl))
        if 'price' in [name for name, _ in pipelines]:
            independent_etls.append(('price', self.run_price_etl))
        if 'financial' in [name for name, _ in pipelines]:
            independent_etls.append(('financial', self.run_financial_etl))
        
        # Group 2: Dependent ETLs (must run after Group 1)
        dependent_etls = []
        for name, func in pipelines:
            if name not in ['company', 'price', 'financial']:
                dependent_etls.append((name, func))
        
        pipeline_timings = {}
        
        # Ensure connection is established before parallel execution
        if self.snowflake:
            self.snowflake.connect()
        
        # Execute Group 1 in parallel
        if independent_etls:
            logger.info(f"\n{'='*60}")
            logger.info("PHASE 1: Running independent ETLs in parallel")
            logger.info(f"Parallel ETLs: {[name for name, _ in independent_etls]}")
            logger.info(f"{'='*60}")
            
            group1_start = time.time()
            
            with ThreadPoolExecutor(max_workers=len(independent_etls)) as executor:
                futures = {}
                
                # Submit all independent ETLs
                for name, func in independent_etls:
                    logger.info(f"Starting {name} ETL (parallel)")
                    future = executor.submit(func, symbols, args)
                    futures[future] = (name, time.time())
                
                # Wait for completion
                for future in as_completed(futures):
                    name, start_time = futures[future]
                    try:
                        success = future.result()
                        duration = time.time() - start_time
                        pipeline_timings[name] = duration
                        if success:
                            any_success = True
                        else:
                            all_success = False
                    except Exception as e:
                        logger.error(f"{name} ETL failed with exception: {e}")
                        all_success = False
                        pipeline_timings[name] = time.time() - start_time
            
            group1_duration = time.time() - group1_start
            logger.info(f"Phase 1 completed in {group1_duration:.1f}s")
        
        # Execute Group 2 sequentially (dependent ETLs)
        if dependent_etls:
            logger.info(f"\n{'='*60}")
            logger.info("PHASE 2: Running dependent ETLs sequentially")
            logger.info(f"Sequential ETLs: {[name for name, _ in dependent_etls]}")
            logger.info(f"{'='*60}")
            
            for name, pipeline_func in dependent_etls:
                logger.info(f"\n{'-'*60}")
                pipeline_start = time.time()
                success = pipeline_func(symbols, args)
                pipeline_duration = time.time() - pipeline_start
                pipeline_timings[name] = pipeline_duration
                
                if success:
                    any_success = True
                else:
                    all_success = False
        
        # Summary
        end_datetime = datetime.now()
        total_duration = time.time() - pipeline_start_time
        
        logger.info(f"\n{'='*60}")
        logger.info("Pipeline Execution Summary")
        logger.info(f"{'='*60}")
        logger.info(f"Start Time: {start_datetime}")
        logger.info(f"End Time: {end_datetime}")
        logger.info(f"Duration: {total_duration:.1f} seconds")
        logger.info(f"Symbols Processed: {len(symbols)}")
        
        # Pipeline results
        logger.info("\nPipeline Results:")
        for pipeline, result in self.results.items():
            status = result.get('status', 'not_run')
            records = result.get('records_loaded', 0)
            timing = pipeline_timings.get(pipeline, 0)
            logger.info(f"  {pipeline.title()}: {status} ({records} records, {timing:.1f}s)")
            if result.get('errors'):
                for error in result['errors'][:3]:  # Show first 3 errors
                    logger.error(f"    - {error}")
        
        # Performance Summary
        logger.info("\nPerformance Summary:")
        total_pipeline_time = sum(pipeline_timings.values())
        logger.info(f"  Total Pipeline Time: {total_pipeline_time:.1f}s")
        logger.info(f"  Average Time per Symbol: {total_pipeline_time/len(symbols):.1f}s")
        if pipeline_timings:
            logger.info(f"  Slowest Pipeline: {max(pipeline_timings, key=pipeline_timings.get)} ({max(pipeline_timings.values()):.1f}s)")
        
        # Determine exit code
        if all_success:
            logger.info("\n✓ All pipelines completed successfully")
            return 0
        elif any_success:
            logger.warning("\n⚠ Some pipelines completed with errors")
            return 1
        else:
            logger.error("\n✗ All pipelines failed")
            return 2


    def cleanup(self):
        """Clean up resources"""
        if self.snowflake:
            if hasattr(self.snowflake, 'close_pool'):
                self.snowflake.close_pool()
            self.snowflake.disconnect()
        logger.info("Cleanup completed")


def main():
    """Main execution function"""
    parser = argparse.ArgumentParser(
        description="Orchestrate all ETL pipelines for daily data updates",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run all pipelines for default test symbols
  python run_daily_pipeline.py
  
  # Run all pipelines for specific symbols
  python run_daily_pipeline.py --symbols AAPL MSFT GOOGL
  
  # Run all pipelines for S&P 500
  python run_daily_pipeline.py --sp500
  
  # Skip specific pipelines
  python run_daily_pipeline.py --skip-financial --symbols AAPL
  
  # Run with ratio and market metrics calculation
  python run_daily_pipeline.py --symbols AAPL MSFT
  
  # Only run ratio and metrics calculation for all symbols
  python run_daily_pipeline.py --skip-company --skip-price --skip-financial --all-symbols
  
  # Dry run to see what would be executed
  python run_daily_pipeline.py --dry-run --sp500
        """
    )
    
    # Symbol selection
    symbol_group = parser.add_mutually_exclusive_group()
    symbol_group.add_argument(
        "--symbols",
        nargs="+",
        help="Specific symbols to process"
    )
    symbol_group.add_argument(
        "--sp500",
        action="store_true",
        help="Process all S&P 500 companies"
    )
    
    # Pipeline selection
    parser.add_argument(
        "--skip-company",
        action="store_true",
        help="Skip company profile ETL"
    )
    parser.add_argument(
        "--skip-price",
        action="store_true",
        help="Skip historical price ETL"
    )
    parser.add_argument(
        "--skip-financial",
        action="store_true",
        help="Skip financial statement ETL"
    )
    parser.add_argument(
        "--skip-ratio",
        action="store_true",
        help="Skip financial ratio calculation ETL"
    )
    parser.add_argument(
        "--skip-ttm",
        action="store_true",
        help="Skip TTM (trailing twelve month) calculation ETL"
    )
    parser.add_argument(
        "--skip-market-metrics",
        action="store_true",
        help="Skip market metrics calculation ETL"
    )
    parser.add_argument(
        "--all-symbols",
        action="store_true",
        help="Process all symbols in database (for ratio/metrics ETL)"
    )
    
    # Common options
    parser.add_argument(
        "--skip-analytics",
        action="store_true",
        help="Skip analytics layer updates"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be executed without running"
    )
    
    # Price ETL specific options
    parser.add_argument(
        "--days-back",
        type=int,
        default=30,
        help="Number of days of historical prices (default: 30)"
    )
    parser.add_argument(
        "--from-date",
        help="Start date for historical prices (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--to-date",
        help="End date for historical prices (YYYY-MM-DD)"
    )
    
    # Financial ETL specific options
    parser.add_argument(
        "--period",
        choices=['annual', 'quarterly'],
        default='annual',
        help="Financial statement period (default: annual)"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Number of periods to fetch (default: no limit)"
    )
    
    args = parser.parse_args()
    
    # Load configuration
    try:
        config = Config.load()
    except Exception as e:
        logger.error(f"Failed to load configuration: {e}")
        return 2
    
    # Create and run orchestrator
    orchestrator = PipelineOrchestrator(config, dry_run=args.dry_run)
    try:
        exit_code = orchestrator.run_daily_update(args)
        return exit_code
    finally:
        orchestrator.cleanup()


if __name__ == "__main__":
    sys.exit(main())