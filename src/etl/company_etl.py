"""
ETL pipeline for extracting and loading company profile data
"""
from typing import Dict, List, Any, Optional
from datetime import datetime, timezone
from loguru import logger

from src.etl.base_etl import BaseETL
from src.db.snowflake_connector import SnowflakeConnector
from src.api.fmp_client import FMPClient
from src.utils.config import Config


class CompanyETL(BaseETL):
    """ETL pipeline for company profile data"""
    
    def __init__(self, config: Config):
        """
        Initialize Company ETL
        
        Args:
            config: Application configuration
        """
        # Create instances
        snowflake_connector = SnowflakeConnector(config.snowflake)
        fmp_client = FMPClient(config.fmp)
        
        # Initialize base class
        super().__init__(
            job_name="company_profile_etl",
            snowflake_connector=snowflake_connector,
            fmp_client=fmp_client,
            batch_size=config.app.batch_size,
            enable_monitoring=config.app.enable_monitoring
        )
        
        # Store config for later use
        self.config = config
        self.existing_companies = {}
        self.load_to_analytics = True  # Default to True, can be overridden in extract()
        
    def extract(self, symbols: List[str], load_to_analytics: bool = True) -> List[Dict[str, Any]]:
        """
        Extract company profiles from FMP API
        
        Args:
            symbols: List of stock symbols to process
            load_to_analytics: Whether to update DIM_COMPANY table
            
        Returns:
            List of company profile data
        """
        self.load_to_analytics = load_to_analytics
        logger.info(f"Extracting company profiles for {len(symbols)} symbols")
        
        # First, get existing companies to determine what's new vs update
        if self.load_to_analytics:
            self._load_existing_companies()
        
        profiles = []
        failed_symbols = []
        
        # Use batch endpoint if available for better performance
        if hasattr(self.fmp_client, 'batch_get_company_profiles'):
            try:
                batch_profiles = self.fmp_client.batch_get_company_profiles(symbols)
                profiles.extend(batch_profiles.values())
                logger.info(f"Extracted {len(batch_profiles)} profiles in batch")
            except Exception as e:
                logger.warning(f"Batch extraction failed, falling back to individual: {e}")
                # Fall back to individual extraction
                for symbol in symbols:
                    profile = self._extract_single_profile(symbol)
                    if profile:
                        profiles.append(profile)
                    else:
                        failed_symbols.append(symbol)
        else:
            # Extract individually
            for symbol in symbols:
                profile = self._extract_single_profile(symbol)
                if profile:
                    profiles.append(profile)
                else:
                    failed_symbols.append(symbol)
        
        if failed_symbols:
            self.result.errors.append(f"Failed to extract profiles for: {', '.join(failed_symbols)}")
            logger.warning(f"Failed to extract {len(failed_symbols)} profiles")
        
        logger.info(f"Successfully extracted {len(profiles)} company profiles")
        
        # Store metadata about extraction
        self.result.metadata['symbols_requested'] = len(symbols)
        self.result.metadata['symbols_extracted'] = len(profiles)
        self.result.metadata['symbols_failed'] = len(failed_symbols)
        
        return profiles
    
    def _extract_single_profile(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Extract a single company profile"""
        try:
            profile = self.fmp_client.get_company_profile(symbol)
            if profile:
                logger.debug(f"Extracted profile for {symbol}")
                return profile
            else:
                logger.warning(f"No profile data returned for {symbol}")
                return None
        except Exception as e:
            logger.error(f"Failed to extract profile for {symbol}: {e}")
            self.result.errors.append(f"Extract failed for {symbol}: {str(e)}")
            return None
    
    def _load_existing_companies(self):
        """Load existing companies from DIM_COMPANY for comparison"""
        try:
            query = """
            SELECT 
                symbol,
                company_name,
                sector,
                industry,
                market_cap_category,
                is_current
            FROM ANALYTICS.DIM_COMPANY
            WHERE is_current = TRUE
            """
            
            existing = self.snowflake.fetch_all(query)
            self.existing_companies = {row['SYMBOL']: row for row in existing}
            logger.info(f"Loaded {len(self.existing_companies)} existing companies from DIM_COMPANY")
        except Exception as e:
            logger.warning(f"Could not load existing companies: {e}")
            self.existing_companies = {}
    
    def transform(self, raw_data: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
        """
        Transform company profile data
        
        Args:
            raw_data: List of raw profile data from FMP
            
        Returns:
            Transformed data for raw and staging layers
        """
        logger.info(f"Transforming {len(raw_data)} company profiles")
        
        # Use the transformer to process all profiles
        transformed = self.transformer.transform_company_profile(raw_data)
        
        # Add metadata to staging records
        for record in transformed.get('staging', []):
            symbol = record['symbol']
            
            # Check if this is a new company or an update
            if symbol in self.existing_companies:
                existing = self.existing_companies[symbol]
                record['is_new_company'] = False
                
                # Check for significant changes
                changes = []
                if existing.get('COMPANY_NAME') != record.get('company_name'):
                    changes.append('name')
                if existing.get('SECTOR') != record.get('sector'):
                    changes.append('sector')
                if existing.get('INDUSTRY') != record.get('industry'):
                    changes.append('industry')
                
                # Check market cap change (significant if > 10%)
                old_cap = existing.get('MARKET_CAP', 0) or 0
                new_cap = record.get('market_cap', 0) or 0
                if old_cap > 0 and abs(new_cap - old_cap) / old_cap > 0.1:
                    changes.append('market_cap')
                
                record['has_changes'] = len(changes) > 0
                record['changed_fields'] = changes
            else:
                record['is_new_company'] = True
                record['has_changes'] = True
                record['changed_fields'] = ['all']
        
        # Track transformation stats
        self.result.metadata['new_companies'] = sum(
            1 for r in transformed.get('staging', []) 
            if r.get('is_new_company', False)
        )
        self.result.metadata['updated_companies'] = sum(
            1 for r in transformed.get('staging', []) 
            if not r.get('is_new_company', False) and r.get('has_changes', False)
        )
        
        return transformed
    
    def load(self, transformed_data: Dict[str, List[Dict[str, Any]]]) -> int:
        """
        Load transformed data to Snowflake
        
        Args:
            transformed_data: Transformed data with 'raw' and 'staging' keys
            
        Returns:
            Number of records loaded
        """
        total_loaded = 0
        
        # Load to RAW layer
        raw_records = transformed_data.get('raw', [])
        if raw_records:
            logger.info(f"Loading {len(raw_records)} records to RAW_COMPANY_PROFILE")
            try:
                self.snowflake.bulk_insert('RAW_DATA.RAW_COMPANY_PROFILE', raw_records)
                total_loaded += len(raw_records)
                logger.info(f"Successfully loaded {len(raw_records)} records to raw layer")
            except Exception as e:
                logger.error(f"Failed to load raw data: {e}")
                self.result.errors.append(f"Raw layer load failed: {str(e)}")
                raise
        
        # Load to STAGING layer
        staging_records = transformed_data.get('staging', [])
        if staging_records:
            # Remove temporary metadata fields before loading
            clean_staging = []
            for record in staging_records:
                clean_record = record.copy()
                # Remove fields not in staging table
                for field in ['is_new_company', 'has_changes', 'changed_fields']:
                    clean_record.pop(field, None)
                clean_staging.append(clean_record)
            
            logger.info(f"Loading {len(clean_staging)} records to STG_COMPANY_PROFILE")
            try:
                self.snowflake.bulk_insert('STAGING.STG_COMPANY_PROFILE', clean_staging)
                total_loaded += len(clean_staging)
                logger.info(f"Successfully loaded {len(clean_staging)} records to staging layer")
            except Exception as e:
                logger.error(f"Failed to load staging data: {e}")
                self.result.errors.append(f"Staging layer load failed: {str(e)}")
                raise
        
        # Update ANALYTICS layer if enabled
        if self.load_to_analytics and staging_records:
            try:
                analytics_loaded = self._update_analytics_layer(staging_records)
                total_loaded += analytics_loaded
                logger.info(f"Updated {analytics_loaded} records in analytics layer")
            except Exception as e:
                logger.error(f"Failed to update analytics layer: {e}")
                self.result.errors.append(f"Analytics layer update failed: {str(e)}")
                # Don't raise here - raw and staging loads were successful
        
        return total_loaded
    
    def _update_analytics_layer(self, staging_records: List[Dict[str, Any]]) -> int:
        """
        Update DIM_COMPANY table with SCD Type 2 logic
        
        Args:
            staging_records: Records from staging with metadata
            
        Returns:
            Number of records updated/inserted
        """
        logger.info("Updating DIM_COMPANY with new and changed companies")
        
        updates_made = 0
        current_timestamp = datetime.now(timezone.utc)
        
        for record in staging_records:
            symbol = record['symbol']
            
            if record.get('has_changes', False):
                try:
                    if not record.get('is_new_company', False):
                        # Update existing record - set is_current to FALSE
                        update_query = """
                        UPDATE ANALYTICS.DIM_COMPANY
                        SET 
                            is_current = FALSE,
                            valid_to = %(valid_to)s
                        WHERE symbol = %(symbol)s
                        AND is_current = TRUE
                        """
                        
                        self.snowflake.execute(
                            update_query,
                            {
                                'symbol': symbol,
                                'valid_to': current_timestamp
                            }
                        )
                    
                    # Insert new current record
                    # Determine market cap category
                    market_cap = record.get('market_cap', 0) or 0
                    if market_cap >= 200_000_000_000:
                        market_cap_category = 'Mega Cap'
                    elif market_cap >= 10_000_000_000:
                        market_cap_category = 'Large Cap'
                    elif market_cap >= 2_000_000_000:
                        market_cap_category = 'Mid Cap'
                    elif market_cap >= 300_000_000:
                        market_cap_category = 'Small Cap'
                    else:
                        market_cap_category = 'Micro Cap'
                    
                    # Build headquarters location
                    location_parts = []
                    if record.get('headquarters_city'):
                        location_parts.append(record['headquarters_city'])
                    if record.get('headquarters_state'):
                        location_parts.append(record['headquarters_state'])
                    if record.get('headquarters_country'):
                        location_parts.append(record['headquarters_country'])
                    headquarters_location = ', '.join(location_parts) if location_parts else None
                    
                    dim_record = {
                        'symbol': record['symbol'],
                        'company_name': record['company_name'],
                        'sector': record['sector'],
                        'industry': record['industry'],
                        'exchange': record['exchange'],
                        'market_cap_category': market_cap_category,
                        'headquarters_location': headquarters_location,
                        'is_current': True,
                        'valid_from': current_timestamp,
                        'valid_to': datetime(9999, 12, 31, tzinfo=timezone.utc)
                    }
                    
                    self.snowflake.bulk_insert('ANALYTICS.DIM_COMPANY', [dim_record])
                    updates_made += 1
                    
                    logger.debug(f"Updated DIM_COMPANY for {symbol} (new: {record.get('is_new_company', False)})")
                    
                except Exception as e:
                    logger.error(f"Failed to update DIM_COMPANY for {symbol}: {e}")
                    raise
        
        logger.info(f"DIM_COMPANY update complete: {updates_made} records updated/inserted")
        return updates_made