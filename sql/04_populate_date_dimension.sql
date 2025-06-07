-- Populate Date Dimension Table
-- Story 1.1: Set up Snowflake Environment

USE DATABASE EQUITY_DATA;
USE SCHEMA ANALYTICS;

-- Generate dates from 2020-01-01 to 2030-12-31
INSERT INTO DIM_DATE (date_key, date, year, quarter, month, day, day_of_week, 
                      day_name, month_name, is_weekend, is_month_end, 
                      is_quarter_end, is_year_end)
SELECT 
    TO_NUMBER(TO_CHAR(date, 'YYYYMMDD')) AS date_key,
    date,
    YEAR(date) AS year,
    QUARTER(date) AS quarter,
    MONTH(date) AS month,
    DAY(date) AS day,
    DAYOFWEEK(date) AS day_of_week,
    DAYNAME(date) AS day_name,
    MONTHNAME(date) AS month_name,
    CASE WHEN DAYOFWEEK(date) IN (1, 7) THEN TRUE ELSE FALSE END AS is_weekend,
    CASE WHEN date = LAST_DAY(date) THEN TRUE ELSE FALSE END AS is_month_end,
    CASE WHEN MONTH(date) IN (3, 6, 9, 12) AND date = LAST_DAY(date) THEN TRUE ELSE FALSE END AS is_quarter_end,
    CASE WHEN MONTH(date) = 12 AND date = LAST_DAY(date) THEN TRUE ELSE FALSE END AS is_year_end
FROM (
    SELECT DATEADD(DAY, SEQ4(), '2020-01-01'::DATE) AS date
    FROM TABLE(GENERATOR(ROWCOUNT => 4018))  -- Number of days from 2020-01-01 to 2030-12-31
) dates
WHERE NOT EXISTS (
    SELECT 1 FROM DIM_DATE WHERE DIM_DATE.date = dates.date
);