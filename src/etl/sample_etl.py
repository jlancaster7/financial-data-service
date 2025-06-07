"""
Sample ETL implementation for testing the framework
"""
from typing import Dict, List, Any
from loguru import logger

from src.etl.base_etl import BaseETL


class SampleETL(BaseETL):
    """Sample ETL implementation for testing"""
    
    def __init__(self, *args, symbols: List[str] = None, **kwargs):
        """
        Initialize sample ETL
        
        Args:
            symbols: List of stock symbols to process
        """
        super().__init__(*args, **kwargs)
        self.symbols = symbols or ['AAPL', 'MSFT', 'GOOGL']
    
    def extract(self) -> List[Dict[str, Any]]:
        """
        Extract sample company profiles
        
        Returns:
            List of company profile data
        """
        logger.info(f"Extracting data for symbols: {self.symbols}")
        
        profiles = []
        for symbol in self.symbols:
            try:
                # Use real FMP API
                profile = self.fmp_client.get_company_profile(symbol)
                if profile:
                    profiles.append(profile)
                    logger.debug(f"Extracted profile for {symbol}")
            except Exception as e:
                logger.error(f"Failed to extract profile for {symbol}: {e}")
                self.result.errors.append(f"Extract failed for {symbol}: {str(e)}")
        
        return profiles
    
    def transform(self, raw_data: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
        """
        Transform company profile data
        
        Args:
            raw_data: List of raw profile data
            
        Returns:
            Transformed data for raw and staging layers
        """
        logger.info(f"Transforming {len(raw_data)} company profiles")
        
        # Use the transformer to process profiles
        transformed = self.transformer.transform_company_profile(raw_data)
        
        # Add any custom transformations here
        for record in transformed.get('staging', []):
            # Example: Add a custom field
            record['etl_job_name'] = self.job_name
        
        return transformed
    
    def load(self, transformed_data: Dict[str, List[Dict[str, Any]]]) -> int:
        """
        Load data to Snowflake (simulated for sample)
        
        Args:
            transformed_data: Transformed data to load
            
        Returns:
            Number of records loaded
        """
        total_loaded = 0
        
        # Load raw data
        raw_records = transformed_data.get('raw', [])
        if raw_records:
            logger.info(f"Loading {len(raw_records)} records to RAW_COMPANY_PROFILE")
            try:
                # In real implementation, this would use snowflake.bulk_insert
                # For now, just simulate the load
                logger.debug("Simulating load to RAW_COMPANY_PROFILE")
                total_loaded += len(raw_records)
            except Exception as e:
                logger.error(f"Failed to load raw data: {e}")
                raise
        
        # Load staging data
        staging_records = transformed_data.get('staging', [])
        if staging_records:
            logger.info(f"Loading {len(staging_records)} records to STG_COMPANY_PROFILE")
            try:
                # In real implementation, this would use snowflake.bulk_insert
                # For now, just simulate the load
                logger.debug("Simulating load to STG_COMPANY_PROFILE")
                total_loaded += len(staging_records)
            except Exception as e:
                logger.error(f"Failed to load staging data: {e}")
                raise
        
        return total_loaded