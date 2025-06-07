-- ETL Monitoring Tables
-- These tables track ETL job execution, performance, and errors

USE DATABASE EQUITY_DATA;
USE SCHEMA RAW_DATA;

-- ETL Job Execution History
CREATE TABLE IF NOT EXISTS ETL_JOB_HISTORY (
    job_id VARCHAR(36) DEFAULT UUID_STRING(),
    job_name VARCHAR(100) NOT NULL,
    status VARCHAR(20) NOT NULL, -- PENDING, RUNNING, SUCCESS, FAILED, PARTIAL
    start_time TIMESTAMP_NTZ NOT NULL,
    end_time TIMESTAMP_NTZ,
    duration_seconds NUMBER(10,2),
    records_extracted NUMBER DEFAULT 0,
    records_transformed NUMBER DEFAULT 0,
    records_loaded NUMBER DEFAULT 0,
    error_count NUMBER DEFAULT 0,
    metadata VARIANT,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (job_id)
);

-- ETL Job Errors
CREATE TABLE IF NOT EXISTS ETL_JOB_ERRORS (
    error_id VARCHAR(36) DEFAULT UUID_STRING(),
    job_id VARCHAR(36) NOT NULL,
    error_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    error_type VARCHAR(50),
    error_message VARCHAR(5000),
    error_details VARIANT,
    PRIMARY KEY (error_id),
    FOREIGN KEY (job_id) REFERENCES ETL_JOB_HISTORY(job_id)
);

-- ETL Job Metrics (for detailed performance tracking)
CREATE TABLE IF NOT EXISTS ETL_JOB_METRICS (
    metric_id VARCHAR(36) DEFAULT UUID_STRING(),
    job_id VARCHAR(36) NOT NULL,
    metric_name VARCHAR(100) NOT NULL,
    metric_value NUMBER(20,4),
    metric_unit VARCHAR(20),
    phase VARCHAR(20), -- EXTRACT, TRANSFORM, LOAD
    recorded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (metric_id),
    FOREIGN KEY (job_id) REFERENCES ETL_JOB_HISTORY(job_id)
);

-- ETL Data Quality Issues
CREATE TABLE IF NOT EXISTS ETL_DATA_QUALITY_ISSUES (
    issue_id VARCHAR(36) DEFAULT UUID_STRING(),
    job_id VARCHAR(36) NOT NULL,
    table_name VARCHAR(100),
    record_identifier VARCHAR(100),
    issue_type VARCHAR(100),
    issue_description VARCHAR(1000),
    severity VARCHAR(20), -- WARNING, ERROR, CRITICAL
    detected_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (issue_id),
    FOREIGN KEY (job_id) REFERENCES ETL_JOB_HISTORY(job_id)
);

-- Add clustering keys for better query performance
-- Snowflake uses micro-partitions and clustering instead of traditional indexes
ALTER TABLE ETL_JOB_HISTORY CLUSTER BY (start_time, job_name);
ALTER TABLE ETL_JOB_ERRORS CLUSTER BY (error_timestamp, job_id);
ALTER TABLE ETL_JOB_METRICS CLUSTER BY (job_id, recorded_at);
ALTER TABLE ETL_DATA_QUALITY_ISSUES CLUSTER BY (job_id, detected_at);

-- Create a view for current job status
CREATE OR REPLACE VIEW V_ETL_JOB_CURRENT_STATUS AS
SELECT 
    job_name,
    MAX(start_time) as last_run_time,
    MAX_BY(status, start_time) as last_status,
    MAX_BY(duration_seconds, start_time) as last_duration_seconds,
    MAX_BY(records_loaded, start_time) as last_records_loaded,
    COUNT(*) as total_runs,
    SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) as successful_runs,
    SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) as failed_runs,
    AVG(duration_seconds) as avg_duration_seconds
FROM ETL_JOB_HISTORY
GROUP BY job_name;

-- Create a view for recent errors
CREATE OR REPLACE VIEW V_ETL_RECENT_ERRORS AS
SELECT 
    j.job_name,
    j.start_time,
    e.error_timestamp,
    e.error_type,
    e.error_message
FROM ETL_JOB_ERRORS e
JOIN ETL_JOB_HISTORY j ON e.job_id = j.job_id
WHERE e.error_timestamp >= DATEADD('day', -7, CURRENT_TIMESTAMP())
ORDER BY e.error_timestamp DESC;

-- Grant permissions
GRANT SELECT, INSERT ON ETL_JOB_HISTORY TO ROLE EQUITY_DATA_LOADER;
GRANT SELECT, INSERT ON ETL_JOB_ERRORS TO ROLE EQUITY_DATA_LOADER;
GRANT SELECT, INSERT ON ETL_JOB_METRICS TO ROLE EQUITY_DATA_LOADER;
GRANT SELECT, INSERT ON ETL_DATA_QUALITY_ISSUES TO ROLE EQUITY_DATA_LOADER;
GRANT SELECT ON V_ETL_JOB_CURRENT_STATUS TO ROLE EQUITY_DATA_LOADER;
GRANT SELECT ON V_ETL_JOB_CURRENT_STATUS TO ROLE EQUITY_DATA_READER;
GRANT SELECT ON V_ETL_RECENT_ERRORS TO ROLE EQUITY_DATA_LOADER;
GRANT SELECT ON V_ETL_RECENT_ERRORS TO ROLE EQUITY_DATA_READER;