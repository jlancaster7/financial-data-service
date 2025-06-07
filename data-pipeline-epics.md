# Data Pipeline Implementation - Epics & Stories

## Project Overview
Build a simplified data pipeline to populate Snowflake with equity market data from Financial Modeling Prep (FMP) API.

## Overall Progress Summary
- **Sprint 1**: ✅ COMPLETED (13/13 points - 100%)
- **Sprint 2**: ✅ COMPLETED (14/14 points - 100%)
- **Sprint 3**: 🚧 IN PROGRESS (13/21 points - 62%)
- **Sprint 4**: 📋 TODO (0/17 points - 0%)
- **Total Progress**: 40/65 points (62% complete)

## What's Next
1. **Story 4.2**: Create Staging Layer Transformations (3 points)
   - Most transformations already implemented in ETL pipelines
   - Focus on SQL views and additional data quality rules
   
2. **Story 5.2**: Implement Analytics Layer Updates (5 points)
   - Calculate financial ratios for FACT_FINANCIAL_RATIOS
   - Create FACT_MARKET_METRICS for daily market-based metrics
   - Ensure proper grain alignment (quarterly vs daily)

## Epic 1: Core Infrastructure Setup
**Goal:** Establish the foundational infrastructure and database schema

### Story 1.1: Set up Snowflake Environment
**As a** data engineer  
**I want to** create the Snowflake database schema  
**So that** we have a properly structured data warehouse

**Acceptance Criteria:**
- [x] Create EQUITY_DATA database
- [x] Create schemas: RAW_DATA, STAGING, ANALYTICS
- [x] Execute all CREATE TABLE statements for three-layer architecture
- [x] Create and populate DIM_DATE dimension table
- [x] Set up appropriate roles (EQUITY_DATA_LOADER, EQUITY_DATA_READER)
- [x] Document connection parameters

**Story Points:** 3  
**Dependencies:** Snowflake account access

### Story 1.2: Set up Development Environment
**As a** developer  
**I want to** configure the local development environment  
**So that** the team can run the pipeline locally

**Acceptance Criteria:**
- [x] Create project structure with all directories
- [x] Set up requirements.txt with dependencies
- [x] Create .env.example file with all required variables
- [x] Set up logging configuration (using loguru)
- [x] Create README with setup instructions
- [x] Set up GitHub repository with CI/CD pipeline

**Story Points:** 2  
**Dependencies:** None

### Story 1.3: Configure Snowflake Connection Module
**As a** developer  
**I want to** create a reusable Snowflake connection module  
**So that** all scripts can easily interact with Snowflake

**Acceptance Criteria:**
- [x] Implement `snowflake_connector.py` with connection management
- [x] Add bulk_insert function with error handling
- [x] Add execute and fetch functions for reading data
- [x] Implement context managers for automatic cleanup
- [x] Handle connection pooling and retries

**Story Points:** 3  
**Dependencies:** Story 1.1, 1.2

---

## Epic 2: FMP Data Integration
**Goal:** Build the integration with Financial Modeling Prep API

### Story 2.1: Implement FMP API Client
**As a** data engineer  
**I want to** create an FMP API client with rate limiting  
**So that** we can fetch data without hitting API limits

**Acceptance Criteria:**
- [x] Create FMPClient class with rate limiting (300 req/min)
- [x] Implement get_historical_prices method
- [x] Implement get_company_profile method
- [x] Implement get_income_statement, get_balance_sheet, get_cash_flow methods
- [x] Add proper error handling and logging
- [x] Test with real API key
- [x] Add additional endpoints (ratios, metrics, market cap, etc.)

**Story Points:** 5  
**Dependencies:** Story 1.2

### Story 2.2: Create Data Transformation Logic
**As a** data engineer  
**I want to** transform FMP data to match our Snowflake schema  
**So that** data loads correctly into our tables

**Acceptance Criteria:**
- [x] Transform FMP JSON to RAW_HISTORICAL_PRICES (VARIANT storage)
- [x] Transform FMP profile to RAW_COMPANY_PROFILE
- [x] Transform FMP statements to RAW_INCOME_STATEMENT, RAW_BALANCE_SHEET, RAW_CASH_FLOW
- [x] Create staging layer transformations (JSON to structured)
- [x] Handle null values and data type conversions
- [x] Create unit tests for transformations
- [x] Implement data quality validation

**Story Points:** 3  
**Dependencies:** Story 2.1

---

## Epic 3: Daily Pipeline Implementation
**Goal:** Build the main pipeline that orchestrates data loading

### Story 3.1: Create ETL Pipeline Framework ✅
**As a** data engineer  
**I want to** create a reusable ETL framework  
**So that** all data loads follow consistent patterns

**Acceptance Criteria:**
- [x] Create base ETL class with extract, transform, load methods
- [x] Implement error handling and retry logic
- [x] Add logging and monitoring hooks
- [x] Create batch processing capabilities
- [x] Test framework with sample data
- [x] Implement ETL monitoring tables and persistence

**Story Points:** 3  
**Dependencies:** Story 1.3, Story 2.2

**Implementation Notes:**
- Created comprehensive ETL monitoring infrastructure
- ETL_JOB_HISTORY, ETL_JOB_ERRORS, ETL_JOB_METRICS tables
- Automatic job result persistence when monitoring enabled

### Story 3.2: Extract Company Data
**As a** data analyst  
**I want to** extract and load company profile data  
**So that** I can analyze company characteristics

**Acceptance Criteria:**
- [x] Extract company profiles from FMP API
- [x] Load data into RAW_COMPANY_PROFILE table
- [x] Transform and load into STG_COMPANY_PROFILE
- [x] Update DIM_COMPANY in analytics layer
- [x] Handle new companies and updates

**Story Points:** 3  
**Dependencies:** Story 3.1

### Story 3.3: Extract Historical Price Data
**As a** data analyst  
**I want to** load historical price and volume data  
**So that** I can analyze price movements

**Acceptance Criteria:**
- [x] Extract historical prices from FMP API
- [x] Load data into RAW_HISTORICAL_PRICES table
- [x] Transform and load into STG_HISTORICAL_PRICES
- [x] Update FACT_DAILY_PRICES in analytics layer
- [x] Handle date ranges and incremental updates
- [x] Implement MERGE for staging tables to prevent duplicates

**Story Points:** 5  
**Dependencies:** Story 3.1

---

## Epic 4: Financial Statement Pipeline
**Goal:** Load and transform financial statement data

### Story 4.1: Extract Financial Statement Data ✅
**As a** data analyst  
**I want to** load income statement, balance sheet, and cash flow data  
**So that** I can analyze company fundamentals

**Acceptance Criteria:**
- [x] Extract financial statements from FMP API
- [x] Load into RAW_INCOME_STATEMENT, RAW_BALANCE_SHEET, RAW_CASH_FLOW
- [x] Transform and load into staging tables
- [x] Update FACT_FINANCIALS with raw financial data (split from FACT_FINANCIAL_METRICS)
- [x] Handle quarterly and annual periods
- [x] Implement MERGE for staging tables to prevent duplicates
- [x] Capture filing_date and accepted_date to prevent look-ahead bias
- [x] Fix field mappings to capture all available FMP data

**Story Points:** 5  
**Dependencies:** Story 3.1

**Implementation Notes:**
- Split FACT_FINANCIAL_METRICS into FACT_FINANCIALS (raw data) and FACT_FINANCIAL_RATIOS (calculated)
- Added filing date capture for point-in-time analysis
- Fixed field mappings for operating_expenses, shares_outstanding, current_assets/liabilities, dividends_paid

### Story 4.2: Create Staging Layer Transformations 📋 TODO
**As a** data engineer  
**I want to** transform raw JSON data to structured format  
**So that** analysts can query data easily

**Acceptance Criteria:**
- [ ] Create SQL/Python transformations for all staging tables
- [ ] Handle data type conversions and null values
- [ ] Implement data quality checks
- [ ] Create reusable transformation functions
- [ ] Document transformation logic

**Story Points:** 3  
**Dependencies:** Stories 3.2, 3.3, 4.1

**Note:** Most transformations are already implemented in the ETL pipelines. This story may focus on:
- Creating SQL views for complex transformations
- Adding more sophisticated data quality rules
- Building transformation documentation

---

## Epic 5: Analytics and Orchestration
**Goal:** Build analytics layer and pipeline orchestration

### Story 5.1: Create Main Pipeline Orchestrator ✅
**As a** operations engineer  
**I want to** run all data loads from a single script  
**So that** scheduling and monitoring is simplified

**Acceptance Criteria:**
- [x] Create run_daily_update function
- [x] Orchestrate all ETL jobs with proper sequencing
- [x] Add command line arguments for selective runs
- [x] Implement proper exit codes for monitoring
- [x] Create --dry-run option for testing

**Story Points:** 3  
**Dependencies:** All previous ETL stories

**Implementation Notes:**
- Created scripts/run_daily_pipeline.py with PipelineOrchestrator class
- Supports running individual pipelines or all at once
- Proper exit codes: 0 for success, 1 for partial success, 2 for failure
- Command line options for symbol selection, pipeline selection, date ranges
- Dry run mode for testing without database changes
- Comprehensive logging and summary reporting

### Story 5.2: Implement Analytics Layer Updates
**As a** data analyst  
**I want to** maintain dimension and fact tables  
**So that** I can perform efficient analytics queries

**Acceptance Criteria:**
- [x] Update DIM_COMPANY with SCD Type 2 logic (implemented in Story 3.2)
- [ ] Calculate and store pure financial ratios in FACT_FINANCIAL_RATIOS (quarterly/annual)
- [ ] Create separate FACT_MARKET_METRICS table for market-based metrics (daily)
- [x] Implement incremental updates for fact tables (MERGE logic)
- [x] Create data quality checks (DataQualityValidator)
- [ ] Test star schema query performance

**Story Points:** 5  
**Dependencies:** Epic 4 stories

**Partial Implementation Notes:**
- DIM_COMPANY SCD Type 2 already implemented
- Data quality validation framework in place
- FACT_FINANCIAL_RATIOS table created but calculation logic pending

**IMPORTANT ARCHITECTURAL DECISION:**
- **DO NOT** store market-based metrics in FACT_FINANCIAL_RATIOS
- Market-based metrics (P/E, P/B, EV/EBITDA, etc.) change daily with stock price
- Pure financial ratios (ROE, ROA, Debt-to-Equity, etc.) are quarterly/annual
- Create separate FACT_MARKET_METRICS table for daily market-based calculations
- This separation ensures:
  - Proper grain alignment (quarterly vs daily)
  - Efficient storage (avoid duplicating quarterly data daily)
  - Clear distinction between fundamental and market metrics

---

## Epic 6: Monitoring and Operations
**Goal:** Ensure pipeline reliability and observability

### Story 6.1: Implement Data Freshness Monitoring
**As a** operations engineer  
**I want to** monitor data freshness  
**So that** I'm alerted to pipeline failures

**Acceptance Criteria:**
- [ ] Create monitor.py script
- [ ] Check last update time for each table
- [ ] Check symbol coverage
- [ ] Log warnings for stale data (>24 hours)
- [ ] Return proper exit codes for alerting

**Story Points:** 2  
**Dependencies:** Story 5.1

### Story 6.2: Set up Email Alerts
**As a** operations engineer  
**I want to** receive email alerts for failures  
**So that** I can respond quickly to issues

**Acceptance Criteria:**
- [ ] Add email configuration to config.py
- [ ] Create send_alert function
- [ ] Alert on pipeline failures
- [ ] Alert on data staleness
- [ ] Daily success summary option

**Story Points:** 2  
**Dependencies:** Story 6.1

### Story 6.3: Create Operational Runbook
**As a** operations engineer  
**I want to** have clear troubleshooting procedures  
**So that** any team member can resolve issues

**Acceptance Criteria:**
- [ ] Document common failure scenarios
- [ ] Create troubleshooting steps
- [ ] Add data recovery procedures
- [ ] Include contact information
- [ ] Test procedures with team

**Story Points:** 3  
**Dependencies:** All previous stories

---

## Epic 7: Testing and Deployment
**Goal:** Ensure code quality and reliable deployment

### Story 7.1: Create Unit Tests ✅
**As a** developer  
**I want to** have comprehensive unit tests  
**So that** we can safely make changes

**Acceptance Criteria:**
- [x] Test FMP client methods
- [x] Test data transformations
- [x] Test Snowflake operations (with mocks)
- [x] Achieve 80% code coverage
- [x] Set up pytest configuration

**Story Points:** 5  
**Dependencies:** Epics 1-4

**Implementation Notes:**
- Comprehensive test suite with 69 tests
- Tests for all ETL pipelines (company, price, financial statements)
- Mock testing for API calls and database operations

### Story 7.2: Create Integration Tests
**As a** developer  
**I want to** test the full pipeline end-to-end  
**So that** we catch integration issues

**Acceptance Criteria:**
- [ ] Create test Snowflake schema
- [ ] Test with subset of symbols
- [ ] Verify data in all tables
- [ ] Test metric calculations
- [ ] Clean up test data

**Story Points:** 3  
**Dependencies:** Story 7.2

### Story 7.3: Set up Cron Scheduling
**As a** operations engineer  
**I want to** automate pipeline runs  
**So that** data is updated without manual intervention

**Acceptance Criteria:**
- [ ] Create cron entries for daily pipeline
- [ ] Schedule metric calculations
- [ ] Schedule monitoring checks
- [ ] Add log rotation
- [ ] Document timezone considerations

**Story Points:** 2  
**Dependencies:** Story 6.2

---

## Implementation Plan

### Sprint 1 (Weeks 1-2): Foundation ✅ COMPLETED
- Epic 1: All stories (8 points) ✅
- Story 2.1: FMP API Client (5 points) ✅
- **Total: 13 points**

### Sprint 2 (Weeks 3-4): Core ETL Pipeline ✅ COMPLETED
- Story 2.2: Data Transformation (3 points) ✅
- Story 3.1: ETL Pipeline Framework (3 points) ✅
- ETL Monitoring Infrastructure ✅
- Story 3.2: Extract Company Data (3 points) ✅
- VARIANT Column Handling Implementation ✅
- **Total: 14 points** (All completed except Story 3.3 moved to Sprint 3)

### Sprint 3 (Weeks 5-6): Financial Data & Analytics 🚧 IN PROGRESS (13/21 points - 62% complete)
- Story 3.3: Extract Historical Price Data (5 points) ✅
- Story 4.1: Extract Financial Statement Data (5 points) ✅
- Story 5.1: Create Main Pipeline Orchestrator (3 points) ✅
- Story 4.2: Create Staging Layer Transformations (3 points) 📋 TODO
- Story 5.2: Implement Analytics Layer Updates (5 points) 📋 TODO
- **Total: 21 points**

**Completed in Sprint 3:**
- ✅ Historical price ETL with duplicate prevention (MERGE)
- ✅ Financial statement ETL for all three statement types
- ✅ Filing date capture implementation
- ✅ Field mapping fixes for complete data capture
- ✅ Main pipeline orchestrator with CLI interface
- ✅ Standardized ETL interfaces across all pipelines
- ✅ Comprehensive documentation (CLAUDE.md, updated README)

### Sprint 4 (Weeks 7-8): Operations & Deployment
- Epic 6: All stories (7 points)
- Epic 7: All stories (10 points)
- Buffer for fixes and optimization
- **Total: 17 points**

## Definition of Done
- [ ] Code reviewed by at least one team member
- [ ] Unit tests written and passing
- [ ] Documentation updated
- [ ] Deployed to development environment
- [ ] Acceptance criteria verified
- [ ] No critical issues in logs

## Technical Debt & Future Enhancements
1. Add more sophisticated error recovery
2. ~~Implement data quality checks~~ ✅ (DataQualityValidator implemented)
3. Add support for more asset classes (bonds, options)
4. Create data lineage tracking
5. Build Streamlit dashboard for monitoring
6. ~~Implement incremental loading for historical data~~ ✅ (MERGE statements implemented)
7. Add support for real-time data feeds
8. Implement financial ratio calculations for FACT_FINANCIAL_RATIOS
9. Add more comprehensive ETL monitoring dashboards
10. Optimize VARIANT column handling for better performance

## Risks & Mitigations
1. **Risk:** FMP API changes or downtime  
   **Mitigation:** Implement robust error handling, consider backup data source

2. **Risk:** Snowflake costs exceed budget  
   **Mitigation:** Monitor query costs, optimize clustering keys

3. **Risk:** Pipeline fails silently  
   **Mitigation:** Implement comprehensive monitoring and alerting

4. **Risk:** Data quality issues go unnoticed  
   **Mitigation:** Add data validation in Sprint 5 if time permits