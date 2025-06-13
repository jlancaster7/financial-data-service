# Integration Test Plan for Financial Data Service

## Overview
Integration tests will verify that all components of the financial data pipeline work correctly together, including parallel processing, data flow between layers, and calculation accuracy.

## Test Scope

### 1. Full Pipeline Flow Test
**Objective**: Verify complete data flow from API → Raw → Staging → Analytics

**Test Steps**:
1. Mock FMP API responses for a test symbol
2. Run the full pipeline orchestrator
3. Verify each ETL completes successfully
4. Check that data flows through all layers
5. Ensure proper exit codes (0 for success)

**Key Validations**:
- All ETL pipelines execute in correct order
- Parallel ETLs (Company, Price, Financial) run concurrently
- Dependent ETLs (TTM, Ratios, Metrics) run sequentially after
- No data loss between layers

### 2. Parallel Processing Integrity Test
**Objective**: Ensure parallel execution maintains data integrity

**Test Steps**:
1. Process multiple symbols (TEST1, TEST2, TEST3) simultaneously
2. Track execution order and timing
3. Verify each symbol's data is processed correctly
4. Check for race conditions or data mixing

**Key Validations**:
- Each symbol's data remains isolated
- All symbols are processed
- Connection pooling works correctly
- No deadlocks or resource contention

### 3. TTM Calculation Accuracy Test
**Objective**: Verify TTM calculations are mathematically correct

**Test Steps**:
1. Provide 4 quarters of financial data
2. Run TTM calculation ETL
3. Verify summed metrics (revenue, net income, cash flow)
4. Check point-in-time values (shares outstanding, equity)

**Expected Results**:
- TTM Revenue = Sum of 4 quarters
- TTM Net Income = Sum of 4 quarters
- Shares Outstanding = Latest quarter value
- Calculation respects accepted_date for point-in-time

### 4. Market Metrics Integration Test
**Objective**: Verify market metrics use pre-calculated TTM values

**Test Steps**:
1. Set up price data, TTM data, and financial ratios
2. Run market metrics ETL
3. Verify P/E, P/S, dividend yield calculations
4. Check proper joins with TTM table

**Expected Calculations**:
- P/E TTM = Price / TTM EPS
- P/S TTM = Price / Revenue per share TTM
- Dividend Yield = (Dividends per share / Price) * 100

### 5. Error Handling and Recovery Test
**Objective**: Ensure pipeline handles failures gracefully

**Test Scenarios**:
1. API failure for one ETL (e.g., price data unavailable)
2. Database connection failure
3. Data validation failure

**Expected Behavior**:
- Partial success exit code (1) when some ETLs fail
- Other ETLs continue processing
- Errors are logged appropriately
- No data corruption

### 6. Data Quality Validation Test
**Objective**: Verify data quality checks are performed

**Test Steps**:
1. Process data with known quality issues
2. Verify validation queries are executed
3. Check that bad data is flagged or rejected

**Key Validations**:
- Required fields are present
- Data types are correct
- Business rules are enforced
- Duplicate prevention works

## Test Implementation Strategy

### Test Database Setup
- Use separate test database: `EQUITY_DATA_TEST`
- Isolate test data from production
- Clean up after tests complete

### Mocking Strategy
1. **FMP API**: Mock all API responses to avoid rate limits and ensure consistent test data
2. **Snowflake**: Mock database operations for speed, but verify SQL is correct
3. **Time**: Control dates for TTM calculations

### Test Data
Create realistic test data including:
- Company profiles with all required fields
- 2 days of price data
- 4 quarters of financial statements
- Edge cases (negative values, nulls, etc.)

### Execution Plan
1. Run tests in isolation first
2. Then run full integration suite
3. Measure test execution time
4. Ensure tests are repeatable

## Success Criteria
- All 6 test categories pass
- Tests complete in < 30 seconds
- No test data persists after completion
- Tests can run in CI/CD pipeline
- Code coverage > 80% for integration paths

## Risks and Mitigations
1. **Risk**: Tests too slow due to database operations
   - **Mitigation**: Use mocks for Snowflake operations

2. **Risk**: Test data doesn't represent real scenarios
   - **Mitigation**: Base test data on actual API responses

3. **Risk**: Parallel tests interfere with each other
   - **Mitigation**: Use unique test identifiers

## Next Steps
1. Review and approve test plan
2. Implement test infrastructure
3. Write individual test cases
4. Run tests and iterate
5. Add to CI/CD pipeline