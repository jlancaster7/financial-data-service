# FMP API Field Mappings

This document describes how fields from the Financial Modeling Prep (FMP) API are mapped to our database schema.

## Income Statement Mappings

| FMP API Field | Database Column | Notes |
|--------------|-----------------|-------|
| symbol | symbol | |
| date | fiscal_date | |
| period | period | |
| filingDate | filing_date | |
| acceptedDate | accepted_date | |
| revenue | revenue | |
| costOfRevenue | cost_of_revenue | |
| grossProfit | gross_profit | |
| **operatingExpenses** | **operating_expenses** | Previously unmapped |
| operatingIncome | operating_income | |
| netIncome | net_income | |
| eps | eps | |
| epsDiluted | eps_diluted | |
| **weightedAverageShsOut** | **shares_outstanding** | Previously mapped from wrong field |
| **weightedAverageShsOutDil** | **shares_outstanding_diluted** | New field added |

## Balance Sheet Mappings

| FMP API Field | Database Column | Notes |
|--------------|-----------------|-------|
| symbol | symbol | |
| date | fiscal_date | |
| period | period | |
| filingDate | filing_date | |
| acceptedDate | accepted_date | |
| totalAssets | total_assets | |
| **totalCurrentAssets** | **current_assets** | Previously unmapped |
| totalLiabilities | total_liabilities | |
| **totalCurrentLiabilities** | **current_liabilities** | Previously unmapped |
| totalEquity / totalStockholdersEquity | total_equity | Falls back to stockholders equity |
| cashAndCashEquivalents | cash_and_equivalents | |
| totalDebt | total_debt | |
| netDebt | net_debt | |

## Cash Flow Statement Mappings

| FMP API Field | Database Column | Notes |
|--------------|-----------------|-------|
| symbol | symbol | |
| date | fiscal_date | |
| period | period | |
| filingDate | filing_date | |
| acceptedDate | accepted_date | |
| operatingCashFlow | operating_cash_flow | |
| netCashProvidedByInvestingActivities | investing_cash_flow | |
| netCashProvidedByFinancingActivities | financing_cash_flow | |
| freeCashFlow | free_cash_flow | |
| capitalExpenditure | capital_expenditures | |
| **commonDividendsPaid / netDividendsPaid** | **dividends_paid** | Falls back to netDividendsPaid if commonDividendsPaid is null |

## Recent Changes (2025-06-07)

The following field mapping issues were identified and fixed:

1. **operating_expenses**: Was not being mapped from FMP's `operatingExpenses` field
2. **shares_outstanding**: Was incorrectly looking for `sharesOutstanding`, now correctly maps from `weightedAverageShsOut`
3. **shares_outstanding_diluted**: Added new field mapping from `weightedAverageShsOutDil`
4. **current_assets**: Was not being mapped from FMP's `totalCurrentAssets` field
5. **current_liabilities**: Was not being mapped from FMP's `totalCurrentLiabilities` field
6. **dividends_paid**: Was looking for `dividendsPaid`, now correctly maps from `commonDividendsPaid` with fallback to `netDividendsPaid`

These changes ensure all available financial data from the FMP API is properly captured in our database.