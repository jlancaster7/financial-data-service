"""
ETL monitoring module for persisting job results to Snowflake
"""
from typing import Dict, Any, List, Optional
from datetime import datetime, timezone
import uuid
import json
from loguru import logger

from src.db.snowflake_connector import SnowflakeConnector
from src.etl.base_etl import ETLResult, ETLStatus


class ETLMonitor:
    """Persists ETL job results and metrics to Snowflake monitoring tables"""
    
    def __init__(self, snowflake_connector: SnowflakeConnector):
        """
        Initialize ETL monitor
        
        Args:
            snowflake_connector: Snowflake database connector
        """
        self.snowflake = snowflake_connector
    
    def save_job_result(self, result: ETLResult) -> str:
        """
        Save ETL job result to monitoring tables
        
        Args:
            result: ETL job result
            
        Returns:
            job_id: Generated job ID
        """
        job_id = str(uuid.uuid4())
        
        try:
            # Save main job record
            job_record = {
                'job_id': job_id,
                'job_name': result.job_name,
                'status': result.status.value,
                'start_time': result.start_time,
                'end_time': result.end_time,
                'duration_seconds': result.duration_seconds,
                'records_extracted': result.records_extracted,
                'records_transformed': result.records_transformed,
                'records_loaded': result.records_loaded,
                'error_count': len(result.errors),
                'metadata': json.dumps(result.metadata) if result.metadata else json.dumps({})
            }
            
            self.snowflake.bulk_insert('ETL_JOB_HISTORY', [job_record])
            logger.info(f"Saved job result for {result.job_name} with job_id: {job_id}")
            
            # Save errors if any
            if result.errors:
                self._save_job_errors(job_id, result.errors)
            
            return job_id
            
        except Exception as e:
            logger.error(f"Failed to save job result: {e}")
            raise
    
    def _save_job_errors(self, job_id: str, errors: List[str]):
        """Save job errors to ETL_JOB_ERRORS table"""
        error_records = []
        
        for error in errors:
            error_record = {
                'error_id': str(uuid.uuid4()),
                'job_id': job_id,
                'error_timestamp': datetime.now(timezone.utc),
                'error_type': 'ETL_ERROR',  # Could be enhanced to classify errors
                'error_message': error[:5000],  # Truncate to fit column
                'error_details': json.dumps({'full_error': error}) if len(error) > 5000 else json.dumps({})
            }
            error_records.append(error_record)
        
        if error_records:
            self.snowflake.bulk_insert('ETL_JOB_ERRORS', error_records)
            logger.debug(f"Saved {len(error_records)} errors for job {job_id}")
    
    def save_job_metrics(self, job_id: str, metrics: Dict[str, Any]):
        """
        Save detailed job metrics
        
        Args:
            job_id: Job ID from save_job_result
            metrics: Dictionary of metric name -> value pairs
        """
        metric_records = []
        
        for name, value in metrics.items():
            if isinstance(value, dict):
                # Handle nested metrics
                metric_value = value.get('value', 0)
                metric_unit = value.get('unit', '')
                phase = value.get('phase', '')
            else:
                metric_value = value
                metric_unit = ''
                phase = ''
            
            metric_record = {
                'metric_id': str(uuid.uuid4()),
                'job_id': job_id,
                'metric_name': name,
                'metric_value': float(metric_value),
                'metric_unit': metric_unit,
                'phase': phase,
                'recorded_at': datetime.now(timezone.utc)
            }
            metric_records.append(metric_record)
        
        if metric_records:
            self.snowflake.bulk_insert('ETL_JOB_METRICS', metric_records)
            logger.debug(f"Saved {len(metric_records)} metrics for job {job_id}")
    
    def save_data_quality_issues(
        self, 
        job_id: str, 
        table_name: str,
        issues: List[Dict[str, Any]]
    ):
        """
        Save data quality issues found during ETL
        
        Args:
            job_id: Job ID from save_job_result
            table_name: Table where issues were found
            issues: List of issue dictionaries
        """
        issue_records = []
        
        for issue in issues:
            issue_record = {
                'issue_id': str(uuid.uuid4()),
                'job_id': job_id,
                'table_name': table_name,
                'record_identifier': str(issue.get('record_identifier', '')),
                'issue_type': issue.get('issue_type', 'UNKNOWN'),
                'issue_description': issue.get('description', '')[:1000],
                'severity': issue.get('severity', 'WARNING'),
                'detected_at': datetime.now(timezone.utc)
            }
            issue_records.append(issue_record)
        
        if issue_records:
            self.snowflake.bulk_insert('ETL_DATA_QUALITY_ISSUES', issue_records)
            logger.debug(f"Saved {len(issue_records)} data quality issues for job {job_id}")
    
    def get_job_history(
        self, 
        job_name: Optional[str] = None,
        status: Optional[str] = None,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """
        Get job execution history
        
        Args:
            job_name: Filter by job name
            status: Filter by status
            limit: Maximum number of records
            
        Returns:
            List of job history records
        """
        query = """
        SELECT 
            job_id,
            job_name,
            status,
            start_time,
            end_time,
            duration_seconds,
            records_extracted,
            records_transformed,
            records_loaded,
            error_count,
            metadata
        FROM ETL_JOB_HISTORY
        WHERE 1=1
        """
        
        params = {}
        
        if job_name:
            query += " AND job_name = %(job_name)s"
            params['job_name'] = job_name
        
        if status:
            query += " AND status = %(status)s"
            params['status'] = status
        
        query += f" ORDER BY start_time DESC LIMIT {limit}"
        
        return self.snowflake.fetch_all(query, params)
    
    def get_recent_errors(self, days: int = 7) -> List[Dict[str, Any]]:
        """
        Get recent ETL errors
        
        Args:
            days: Number of days to look back
            
        Returns:
            List of recent errors
        """
        query = """
        SELECT 
            j.job_name,
            j.start_time,
            e.error_timestamp,
            e.error_type,
            e.error_message,
            e.error_details
        FROM ETL_JOB_ERRORS e
        JOIN ETL_JOB_HISTORY j ON e.job_id = j.job_id
        WHERE e.error_timestamp >= DATEADD('day', -%(days)s, CURRENT_TIMESTAMP())
        ORDER BY e.error_timestamp DESC
        """
        
        return self.snowflake.fetch_all(query, {'days': days})
    
    def get_job_summary(self) -> List[Dict[str, Any]]:
        """
        Get summary statistics for all jobs
        
        Returns:
            List of job summary records
        """
        return self.snowflake.fetch_all("SELECT * FROM V_ETL_JOB_CURRENT_STATUS")