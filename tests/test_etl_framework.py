"""
Unit tests for ETL framework
"""
import pytest
from unittest.mock import Mock, MagicMock, patch
from datetime import datetime, timezone
from typing import Dict, List, Any

from src.etl.base_etl import BaseETL, ETLStatus, ETLResult
from src.etl.sample_etl import SampleETL
from src.db.snowflake_connector import SnowflakeConnector
from src.api.fmp_client import FMPClient


class ConcreteETL(BaseETL):
    """Concrete implementation for testing base class"""
    
    def __init__(self, *args, extract_data=None, transform_data=None, load_count=None, **kwargs):
        super().__init__(*args, **kwargs)
        self._extract_data = extract_data or []
        self._transform_data = transform_data or {'raw': [], 'staging': []}
        self._load_count = load_count or 0
    
    def extract(self) -> List[Dict[str, Any]]:
        return self._extract_data
    
    def transform(self, raw_data: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
        return self._transform_data
    
    def load(self, transformed_data: Dict[str, List[Dict[str, Any]]]) -> int:
        # Return count of records in the batch passed to load
        total = 0
        for layer, records in transformed_data.items():
            total += len(records)
        return total


class TestETLResult:
    """Test ETL result data class"""
    
    def test_duration_seconds(self):
        """Test duration calculation"""
        start = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        end = datetime(2024, 1, 1, 12, 0, 10, tzinfo=timezone.utc)  # 10 seconds later
        result = ETLResult(
            job_name="test",
            status=ETLStatus.SUCCESS,
            start_time=start,
            end_time=end,
            records_extracted=0,
            records_transformed=0,
            records_loaded=0,
            errors=[],
            metadata={}
        )
        assert result.duration_seconds == 10.0
    
    def test_duration_seconds_none(self):
        """Test duration when end_time is None"""
        result = ETLResult(
            job_name="test",
            status=ETLStatus.RUNNING,
            start_time=datetime.now(timezone.utc),
            end_time=None,
            records_extracted=0,
            records_transformed=0,
            records_loaded=0,
            errors=[],
            metadata={}
        )
        assert result.duration_seconds is None
    
    def test_to_dict(self):
        """Test conversion to dictionary"""
        result = ETLResult(
            job_name="test",
            status=ETLStatus.SUCCESS,
            start_time=datetime.now(timezone.utc),
            end_time=datetime.now(timezone.utc),
            records_extracted=10,
            records_transformed=8,
            records_loaded=8,
            errors=[],
            metadata={"key": "value"}
        )
        
        data = result.to_dict()
        assert data['job_name'] == "test"
        assert data['status'] == "success"
        assert data['records_extracted'] == 10
        assert data['records_transformed'] == 8
        assert data['records_loaded'] == 8
        assert data['metadata'] == {"key": "value"}


class TestBaseETL:
    """Test base ETL framework"""
    
    @pytest.fixture
    def mock_snowflake(self):
        """Mock Snowflake connector"""
        return Mock(spec=SnowflakeConnector)
    
    @pytest.fixture
    def mock_fmp(self):
        """Mock FMP client"""
        return Mock(spec=FMPClient)
    
    @pytest.fixture
    def etl_instance(self, mock_snowflake, mock_fmp):
        """Create ETL instance for testing"""
        return ConcreteETL(
            job_name="test_job",
            snowflake_connector=mock_snowflake,
            fmp_client=mock_fmp,
            batch_size=2,
            max_retries=2,
            retry_delay=0.1
        )
    
    def test_successful_run(self, etl_instance):
        """Test successful ETL run"""
        # Set up test data
        extract_data = [{"id": 1}, {"id": 2}]
        transform_data = {
            'raw': [{"id": 1, "raw": True}, {"id": 2, "raw": True}],
            'staging': [{"id": 1, "staging": True}, {"id": 2, "staging": True}]
        }
        
        etl_instance._extract_data = extract_data
        etl_instance._transform_data = transform_data
        etl_instance._load_count = 4  # 2 raw + 2 staging
        
        # Run ETL
        result = etl_instance.run()
        
        # Verify result
        assert result.status == ETLStatus.SUCCESS
        assert result.records_extracted == 2
        assert result.records_transformed == 2
        assert result.records_loaded == 4
        assert len(result.errors) == 0
        assert result.end_time is not None
    
    def test_no_data_extracted(self, etl_instance):
        """Test ETL with no data extracted"""
        etl_instance._extract_data = []
        
        result = etl_instance.run()
        
        assert result.status == ETLStatus.SUCCESS
        assert result.records_extracted == 0
        assert result.records_transformed == 0
        assert result.records_loaded == 0
    
    def test_extract_with_retry(self, etl_instance):
        """Test extract retry logic"""
        # Mock extract to fail once then succeed
        call_count = 0
        def extract_with_failure():
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise Exception("Temporary failure")
            return [{"id": 1}]
        
        etl_instance.extract = extract_with_failure
        etl_instance._transform_data = {'raw': [{"id": 1}], 'staging': [{"id": 1}]}
        etl_instance._load_count = 2
        
        result = etl_instance.run()
        
        assert result.status == ETLStatus.SUCCESS
        assert call_count == 2  # First attempt failed, second succeeded
    
    def test_extract_max_retries_exceeded(self, etl_instance):
        """Test extract fails after max retries"""
        def always_fail():
            raise Exception("Permanent failure")
        
        etl_instance.extract = always_fail
        
        with pytest.raises(Exception, match="Permanent failure"):
            etl_instance.run()
        
        assert etl_instance.result.status == ETLStatus.FAILED
        assert "Permanent failure" in etl_instance.result.errors[0]
    
    def test_validation_integration(self, etl_instance):
        """Test data validation integration"""
        # Set up profile data for validation
        etl_instance._extract_data = [{"symbol": "AAPL", "company_name": "Apple Inc."}]
        etl_instance._transform_data = {
            'staging': [{"symbol": "AAPL", "company_name": "Apple Inc."}]
        }
        etl_instance._load_count = 1
        
        result = etl_instance.run()
        
        assert result.status == ETLStatus.SUCCESS
    
    def test_batch_loading(self, etl_instance):
        """Test batch loading functionality"""
        # Create 5 records to test batching with batch_size=2
        etl_instance._extract_data = [{"id": i} for i in range(5)]
        etl_instance._transform_data = {
            'staging': [{"id": i, "staging": True} for i in range(5)]
        }
        
        # Track load calls
        load_calls = []
        def track_loads(data):
            load_calls.append(len(data['staging']))
            return len(data['staging'])
        
        etl_instance.load = track_loads
        
        result = etl_instance.run()
        
        # Should have 3 batches: 2, 2, 1
        assert len(load_calls) == 3
        assert load_calls == [2, 2, 1]
        assert result.records_loaded == 5
    
    def test_monitoring_hooks(self, etl_instance):
        """Test monitoring hooks execution"""
        hook_calls = []
        
        def pre_extract_hook(etl, **kwargs):
            hook_calls.append(('pre_extract', kwargs))
        
        def post_transform_hook(etl, **kwargs):
            hook_calls.append(('post_transform', kwargs))
        
        etl_instance.add_pre_extract_hook(pre_extract_hook)
        etl_instance.add_post_transform_hook(post_transform_hook)
        
        etl_instance._extract_data = [{"id": 1}]
        etl_instance._transform_data = {'staging': [{"id": 1}]}
        etl_instance._load_count = 1
        
        etl_instance.run()
        
        assert len(hook_calls) == 2
        assert hook_calls[0][0] == 'pre_extract'
        assert hook_calls[1][0] == 'post_transform'
    
    def test_partial_failure(self, etl_instance):
        """Test partial failure handling"""
        etl_instance._extract_data = [{"id": 1}]
        etl_instance._transform_data = {'staging': [{"id": 1}]}
        etl_instance._load_count = 1
        
        # Add an error during processing
        etl_instance.result.errors.append("Some warning occurred")
        
        result = etl_instance.run()
        
        assert result.status == ETLStatus.PARTIAL
        assert len(result.errors) == 1


class TestSampleETL:
    """Test sample ETL implementation"""
    
    @pytest.fixture
    def mock_snowflake(self):
        """Mock Snowflake connector"""
        return Mock(spec=SnowflakeConnector)
    
    @pytest.fixture
    def mock_fmp(self):
        """Mock FMP client"""
        mock = Mock(spec=FMPClient)
        # Mock successful profile responses
        mock.get_company_profile.side_effect = lambda symbol: {
            'symbol': symbol,
            'companyName': f'{symbol} Inc.',
            'sector': 'Technology'
        }
        return mock
    
    @pytest.fixture
    def sample_etl(self, mock_snowflake, mock_fmp):
        """Create sample ETL instance"""
        return SampleETL(
            job_name="sample_test",
            snowflake_connector=mock_snowflake,
            fmp_client=mock_fmp,
            symbols=['AAPL', 'MSFT']
        )
    
    def test_extract_profiles(self, sample_etl, mock_fmp):
        """Test profile extraction"""
        profiles = sample_etl.extract()
        
        assert len(profiles) == 2
        assert profiles[0]['symbol'] == 'AAPL'
        assert profiles[1]['symbol'] == 'MSFT'
        assert mock_fmp.get_company_profile.call_count == 2
    
    def test_extract_with_failure(self, sample_etl, mock_fmp):
        """Test extraction with API failure"""
        # Make MSFT fail
        def side_effect(symbol):
            if symbol == 'MSFT':
                raise Exception("API Error")
            return {'symbol': symbol, 'companyName': f'{symbol} Inc.'}
        
        mock_fmp.get_company_profile.side_effect = side_effect
        
        profiles = sample_etl.extract()
        
        assert len(profiles) == 1  # Only AAPL succeeded
        assert profiles[0]['symbol'] == 'AAPL'
        assert len(sample_etl.result.errors) == 1
        assert "Extract failed for MSFT" in sample_etl.result.errors[0]
    
    def test_transform_profiles(self, sample_etl):
        """Test profile transformation"""
        raw_profiles = [
            {'symbol': 'AAPL', 'companyName': 'Apple Inc.'},
            {'symbol': 'MSFT', 'companyName': 'Microsoft Corp.'}
        ]
        
        transformed = sample_etl.transform(raw_profiles)
        
        assert 'raw' in transformed
        assert 'staging' in transformed
        assert len(transformed['staging']) == 2
        
        # Check custom field added
        for record in transformed['staging']:
            assert record['etl_job_name'] == 'sample_test'
    
    def test_load_simulation(self, sample_etl):
        """Test load simulation"""
        transformed_data = {
            'raw': [{'symbol': 'AAPL'}, {'symbol': 'MSFT'}],
            'staging': [{'symbol': 'AAPL'}, {'symbol': 'MSFT'}]
        }
        
        loaded = sample_etl.load(transformed_data)
        
        assert loaded == 4  # 2 raw + 2 staging
    
    @patch('src.etl.sample_etl.logger')
    def test_full_run(self, mock_logger, sample_etl):
        """Test full ETL run"""
        result = sample_etl.run()
        
        assert result.status == ETLStatus.SUCCESS
        assert result.records_extracted == 2
        assert result.records_transformed == 2
        assert result.records_loaded == 4  # 2 raw + 2 staging