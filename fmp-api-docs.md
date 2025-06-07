# Financial Modeling Prep API Documentation

## Table of Contents
1. [Company Profile Data API](#company-profile-data-api)
2. [Key Metrics API](#key-metrics-api)
3. [Financial Ratios API](#financial-ratios-api)
4. [Stock Price and Volume Data API](#stock-price-and-volume-data-api)
5. [Income Statement API](#income-statement-api)
6. [Balance Sheet Statement API](#balance-sheet-statement-api)
7. [Cash Flow Statement API](#cash-flow-statement-api)
8. [Financial Ratios TTM API](#financial-ratios-ttm-api)
9. [Key Metrics TTM API](#key-metrics-ttm-api)
10. [S&P 500 Index API](#sp-500-index-api)
11. [Sector PE Snapshot API](#sector-pe-snapshot-api)
12. [Historical Sector PE API](#historical-sector-pe-api)
13. [Treasury Rates API](#treasury-rates-api)
14. [Economics Indicators API](#economics-indicators-api)
15. [Historical Market Cap API](#historical-market-cap-api)

---

## Key Metrics API

Access essential financial metrics for a company with the FMP Financial Key Metrics API. Evaluate revenue, net income, P/E ratio, and more to assess performance and compare it to competitors.

**Endpoint:**
```
https://financialmodelingprep.com/stable/key-metrics?symbol=AAPL
```

**Parameters:**

| Parameter | Type | Required | Example | Description |
|-----------|------|----------|---------|-------------|
| symbol | string | ✓ | AAPL | Stock ticker symbol |
| limit | number | | 5 | Maximum records (up to 1000) |
| period | string | | Q1,Q2,Q3,Q4,FY | Reporting period |

**Note:** Currency is as reported in financials

**Response Example:**
```json
[
  {
    "symbol": "AAPL",
    "date": "2024-09-28",
    "fiscalYear": "2024",
    "period": "FY",
    "reportedCurrency": "USD",
    "marketCap": 3495160329570,
    "enterpriseValue": 3571846329570,
    "evToSales": 9.134339201273542,
    "evToOperatingCashFlow": 30.204866893043786,
    "evToFreeCashFlow": 32.82735788662494,
    "evToEBITDA": 26.524727497716487,
    "netDebtToEBITDA": 0.5694744580836323,
    "currentRatio": 0.8673125765340832,
    "incomeQuality": 1.2615643936161134,
    "grahamNumber": 22.587017267616833,
    "grahamNetNet": -12.352478525015636,
    "taxBurden": 0.7590881483581001,
    "interestBurden": 1.0021831580314244,
    "workingCapital": -23405000000,
    "investedCapital": 22275000000,
    "returnOnAssets": 0.25682503150857583,
    "operatingReturnOnAssets": 0.3434290787011036,
    "returnOnTangibleAssets": 0.25682503150857583,
    "returnOnEquity": 1.6459350307287095,
    "returnOnInvestedCapital": 0.4430708117427921,
    "returnOnCapitalEmployed": 0.6533607652660827,
    "earningsYield": 0.026818798327209237,
    "freeCashFlowYield": 0.03113076074921754,
    "capexToOperatingCashFlow": 0.07988736110406414,
    "capexToDepreciation": 0.8254259501965924,
    "capexToRevenue": 0.02415896275269477,
    "salesGeneralAndAdministrativeToRevenue": 0,
    "researchAndDevelopementToRevenue": 0.08022299794136074,
    "stockBasedCompensationToRevenue": 0.02988990755303234,
    "intangiblesToTotalAssets": 0,
    "averageReceivables": 63614000000,
    "averagePayables": 65785500000,
    "averageInventory": 6808500000,
    "daysOfSalesOutstanding": 61.83255974529134,
    "daysOfPayablesOutstanding": 119.65847721913745,
    "daysOfInventoryOutstanding": 12.642570548414087,
    "operatingCycle": 74.47513029370543,
    "cashConversionCycle": -45.18334692543202,
    "freeCashFlowToEquity": 32121000000,
    "freeCashFlowToFirm": 117192805288.09166,
    "tangibleAssetValue": 56950000000,
    "netCurrentAssetValue": -155043000000
  }
]
```

---

## Financial Ratios API

Analyze a company's financial performance using the Financial Ratios API. This API provides detailed profitability, liquidity, and efficiency ratios, enabling users to assess a company's operational and financial health across various metrics.

**Endpoint:**
```
https://financialmodelingprep.com/stable/ratios?symbol=AAPL
```

**Parameters:**

| Parameter | Type | Required | Example | Description |
|-----------|------|----------|---------|-------------|
| symbol | string | ✓ | AAPL | Stock ticker symbol |
| limit | number | | 5 | Maximum records (up to 1000) |
| period | string | | Q1,Q2,Q3,Q4,FY | Reporting period |

**Note:** Currency is as reported in financials

**Response Example:**
```json
[
  {
    "symbol": "AAPL",
    "date": "2024-09-28",
    "fiscalYear": "2024",
    "period": "FY",
    "reportedCurrency": "USD",
    "grossProfitMargin": 0.4620634981523393,
    "ebitMargin": 0.31510222870075566,
    "ebitdaMargin": 0.3443707085043538,
    "operatingProfitMargin": 0.31510222870075566,
    "pretaxProfitMargin": 0.3157901466620635,
    "continuousOperationsProfitMargin": 0.23971255769943867,
    "netProfitMargin": 0.23971255769943867,
    "bottomLineProfitMargin": 0.23971255769943867,
    "receivablesTurnover": 5.903038811648023,
    "payablesTurnover": 3.0503480278422272,
    "inventoryTurnover": 28.870710952511665,
    "fixedAssetTurnover": 8.560310858143607,
    "assetTurnover": 1.0713874732862074,
    "currentRatio": 0.8673125765340832,
    "quickRatio": 0.8260068483831466,
    "solvencyRatio": 0.3414634938155374,
    "cashRatio": 0.16975259648963673,
    "priceToEarningsRatio": 37.287278415656736,
    "priceToEarningsGrowthRatio": -45.93792700808932,
    "forwardPriceToEarningsGrowthRatio": -45.93792700808932,
    "priceToBookRatio": 61.37243774486391,
    "priceToSalesRatio": 8.93822887866815,
    "priceToFreeCashFlowRatio": 32.12256867269569,
    "priceToOperatingCashFlowRatio": 29.55638142954995,
    "debtToAssetsRatio": 0.29215025480848267,
    "debtToEquityRatio": 1.872326602282704,
    "debtToCapitalRatio": 0.6518501763673821,
    "longTermDebtToCapitalRatio": 0.6009110021023125,
    "financialLeverageRatio": 6.408779631255487,
    "workingCapitalTurnoverRatio": -31.099932397502684,
    "operatingCashFlowRatio": 0.6704045534944896,
    "operatingCashFlowSalesRatio": 0.3024128274962599,
    "freeCashFlowOperatingCashFlowRatio": 0.9201126388959359,
    "debtServiceCoverageRatio": 5.024761722304708,
    "interestCoverageRatio": 0,
    "shortTermOperatingCashFlowCoverageRatio": 5.663777000814215,
    "operatingCashFlowCoverageRatio": 1.109022873702276,
    "capitalExpenditureCoverageRatio": 12.517624642743728,
    "dividendPaidAndCapexCoverageRatio": 4.7912969490701345,
    "dividendPayoutRatio": 0.16252026969360758,
    "dividendYield": 0.0043585983369965175,
    "dividendYieldPercentage": 0.43585983369965176,
    "revenuePerShare": 25.484914639368924,
    "netIncomePerShare": 6.109054070954992,
    "interestDebtPerShare": 6.949329249507765,
    "cashPerShare": 4.247388013764271,
    "bookValuePerShare": 3.711600978715614,
    "tangibleBookValuePerShare": 3.711600978715614,
    "shareholdersEquityPerShare": 3.711600978715614,
    "operatingCashFlowPerShare": 7.706965094592383,
    "capexPerShare": 0.6156891035281195,
    "freeCashFlowPerShare": 7.091275991064264,
    "netIncomePerEBT": 0.7590881483581001,
    "ebtPerEbit": 1.0021831580314244,
    "priceToFairValue": 61.37243774486391,
    "debtToMarketCap": 0.03050761336980449,
    "effectiveTaxRate": 0.24091185164189982,
    "enterpriseValueMultiple": 26.524727497716487
  }
]
```

---

## Company Profile Data API

Access detailed company profile data with the FMP Company Profile Data API. This API provides key financial and operational information for a specific stock symbol, including the company's market capitalization, stock price, industry, and much more.

**Endpoint:**
```
https://financialmodelingprep.com/stable/profile?symbol=AAPL
```

**Parameters:**

| Parameter | Type | Required | Example | Description |
|-----------|------|----------|---------|-------------|
| symbol | string | ✓ | AAPL | Stock ticker symbol |

**Response Example:**
```json
[
  {
    "symbol": "AAPL",
    "price": 232.8,
    "marketCap": 3500823120000,
    "beta": 1.24,
    "lastDividend": 0.99,
    "range": "164.08-260.1",
    "change": 4.79,
    "changePercentage": 2.1008,
    "volume": 0,
    "averageVolume": 50542058,
    "companyName": "Apple Inc.",
    "currency": "USD",
    "cik": "0000320193",
    "isin": "US0378331005",
    "cusip": "037833100",
    "exchangeFullName": "NASDAQ Global Select",
    "exchange": "NASDAQ",
    "industry": "Consumer Electronics",
    "website": "https://www.apple.com",
    "description": "Apple Inc. designs, manufactures, and markets smartphones, personal computers, tablets, wearables, and accessories worldwide. The company offers iPhone, a line of smartphones; Mac, a line of personal computers; iPad, a line of multi-purpose tablets; and wearables, home, and accessories comprising AirPods, Apple TV, Apple Watch, Beats products, and HomePod. It also provides AppleCare support and cloud services; and operates various platforms, including the App Store that allow customers to discov...",
    "ceo": "Mr. Timothy D. Cook",
    "sector": "Technology",
    "country": "US",
    "fullTimeEmployees": "164000",
    "phone": "(408) 996-1010",
    "address": "One Apple Park Way",
    "city": "Cupertino",
    "state": "CA",
    "zip": "95014",
    "image": "https://images.financialmodelingprep.com/symbol/AAPL.png",
    "ipoDate": "1980-12-12",
    "defaultImage": false,
    "isEtf": false,
    "isActivelyTrading": true,
    "isAdr": false,
    "isFund": false
  }
]
```

---

## Stock Price and Volume Data API

Access full price and volume data for any stock symbol using the FMP Comprehensive Stock Price and Volume Data API. Get detailed insights, including open, high, low, close prices, trading volume, price changes, percentage changes, and volume-weighted average price (VWAP).

**Endpoint:**
```
https://financialmodelingprep.com/stable/historical-price-eod/full?symbol=AAPL
```

**Parameters:**

| Parameter | Type | Required | Example | Description |
|-----------|------|----------|---------|-------------|
| symbol | string | ✓ | AAPL | Stock ticker symbol |
| from | date | | 2025-01-10 | Start date for historical data |
| to | date | | 2025-04-10 | End date for historical data |

**Response Example:**
```json
[
  {
    "symbol": "AAPL",
    "date": "2025-02-04",
    "open": 227.2,
    "high": 233.13,
    "low": 226.65,
    "close": 232.8,
    "volume": 44489128,
    "change": 5.6,
    "changePercent": 2.46479,
    "vwap": 230.86
  }
]
```

---

## Income Statement API

Access real-time income statement data for public companies, private companies, and ETFs with the FMP Real-Time Income Statements API. Track profitability, compare competitors, and identify business trends with up-to-date financial data.

**Endpoint:**
```
https://financialmodelingprep.com/stable/income-statement?symbol=AAPL
```

**Parameters:**

| Parameter | Type | Required | Example | Description |
|-----------|------|----------|---------|-------------|
| symbol | string | ✓ | AAPL | Stock ticker symbol |
| limit | number | | 5 | Maximum records (up to 1000) |
| period | string | | Q1,Q2,Q3,Q4,FY | Reporting period |

**Note:** Currency is as reported in financials

**Response Example:**
```json
[
  {
    "date": "2024-09-28",
    "symbol": "AAPL",
    "reportedCurrency": "USD",
    "cik": "0000320193",
    "filingDate": "2024-11-01",
    "acceptedDate": "2024-11-01 06:01:36",
    "fiscalYear": "2024",
    "period": "FY",
    "revenue": 391035000000,
    "costOfRevenue": 210352000000,
    "grossProfit": 180683000000,
    "researchAndDevelopmentExpenses": 31370000000,
    "generalAndAdministrativeExpenses": 0,
    "sellingAndMarketingExpenses": 0,
    "sellingGeneralAndAdministrativeExpenses": 26097000000,
    "otherExpenses": 0,
    "operatingExpenses": 57467000000,
    "costAndExpenses": 267819000000,
    "netInterestIncome": 0,
    "interestIncome": 0,
    "interestExpense": 0,
    "depreciationAndAmortization": 11445000000,
    "ebitda": 134661000000,
    "ebit": 123216000000,
    "nonOperatingIncomeExcludingInterest": 0,
    "operatingIncome": 123216000000,
    "totalOtherIncomeExpensesNet": 269000000,
    "incomeBeforeTax": 123485000000,
    "incomeTaxExpense": 29749000000,
    "netIncomeFromContinuingOperations": 93736000000,
    "netIncomeFromDiscontinuedOperations": 0,
    "otherAdjustmentsToNetIncome": 0,
    "netIncome": 93736000000,
    "netIncomeDeductions": 0,
    "bottomLineNetIncome": 93736000000,
    "eps": 6.11,
    "epsDiluted": 6.08,
    "weightedAverageShsOut": 15343783000,
    "weightedAverageShsOutDil": 15408095000
  }
]
```

---

## Balance Sheet Statement API

Access detailed balance sheet statements for publicly traded companies with the Balance Sheet Data API. Analyze assets, liabilities, and shareholder equity to gain insights into a company's financial health.

**Endpoint:**
```
https://financialmodelingprep.com/stable/balance-sheet-statement?symbol=AAPL
```

**Parameters:**

| Parameter | Type | Required | Example | Description |
|-----------|------|----------|---------|-------------|
| symbol | string | ✓ | AAPL | Stock ticker symbol |
| limit | number | | 5 | Maximum records (up to 1000) |
| period | string | | Q1,Q2,Q3,Q4,FY | Reporting period |

**Note:** Currency is as reported in financials

**Response Example:**
```json
[
  {
    "date": "2024-09-28",
    "symbol": "AAPL",
    "reportedCurrency": "USD",
    "cik": "0000320193",
    "filingDate": "2024-11-01",
    "acceptedDate": "2024-11-01 06:01:36",
    "fiscalYear": "2024",
    "period": "FY",
    "cashAndCashEquivalents": 29943000000,
    "shortTermInvestments": 35228000000,
    "cashAndShortTermInvestments": 65171000000,
    "netReceivables": 66243000000,
    "accountsReceivables": 33410000000,
    "otherReceivables": 32833000000,
    "inventory": 7286000000,
    "prepaids": 0,
    "otherCurrentAssets": 14287000000,
    "totalCurrentAssets": 152987000000,
    "propertyPlantEquipmentNet": 45680000000,
    "goodwill": 0,
    "intangibleAssets": 0,
    "goodwillAndIntangibleAssets": 0,
    "longTermInvestments": 91479000000,
    "taxAssets": 19499000000,
    "otherNonCurrentAssets": 55335000000,
    "totalNonCurrentAssets": 211993000000,
    "otherAssets": 0,
    "totalAssets": 364980000000,
    "totalPayables": 95561000000,
    "accountPayables": 68960000000,
    "otherPayables": 26601000000,
    "accruedExpenses": 0,
    "shortTermDebt": 20879000000,
    "capitalLeaseObligationsCurrent": 1632000000,
    "taxPayables": 26601000000,
    "deferredRevenue": 8249000000,
    "otherCurrentLiabilities": 50071000000,
    "totalCurrentLiabilities": 176392000000,
    "longTermDebt": 85750000000,
    "deferredRevenueNonCurrent": 10798000000,
    "deferredTaxLiabilitiesNonCurrent": 0,
    "otherNonCurrentLiabilities": 35090000000,
    "totalNonCurrentLiabilities": 131638000000,
    "otherLiabilities": 0,
    "capitalLeaseObligations": 12430000000,
    "totalLiabilities": 308030000000,
    "treasuryStock": 0,
    "preferredStock": 0,
    "commonStock": 83276000000,
    "retainedEarnings": -19154000000,
    "additionalPaidInCapital": 0,
    "accumulatedOtherComprehensiveIncomeLoss": -7172000000,
    "otherTotalStockholdersEquity": 0,
    "totalStockholdersEquity": 56950000000,
    "totalEquity": 56950000000,
    "minorityInterest": 0,
    "totalLiabilitiesAndTotalEquity": 364980000000,
    "totalInvestments": 126707000000,
    "totalDebt": 106629000000,
    "netDebt": 76686000000
  }
]
```

---

## Cash Flow Statement API

Gain insights into a company's cash flow activities with the Cash Flow Statements API. Analyze cash generated and used from operations, investments, and financing activities to evaluate the financial health and sustainability of a business.

**Endpoint:**
```
https://financialmodelingprep.com/stable/cash-flow-statement?symbol=AAPL
```

**Parameters:**

| Parameter | Type | Required | Example | Description |
|-----------|------|----------|---------|-------------|
| symbol | string | ✓ | AAPL | Stock ticker symbol |
| limit | number | | 5 | Maximum records (up to 1000) |
| period | string | | Q1,Q2,Q3,Q4,FY | Reporting period |

**Note:** Currency is as reported in financials

**Response Example:**
```json
[
  {
    "date": "2024-09-28",
    "symbol": "AAPL",
    "reportedCurrency": "USD",
    "cik": "0000320193",
    "filingDate": "2024-11-01",
    "acceptedDate": "2024-11-01 06:01:36",
    "fiscalYear": "2024",
    "period": "FY",
    "netIncome": 93736000000,
    "depreciationAndAmortization": 11445000000,
    "deferredIncomeTax": 0,
    "stockBasedCompensation": 11688000000,
    "changeInWorkingCapital": 3651000000,
    "accountsReceivables": -5144000000,
    "inventory": -1046000000,
    "accountsPayables": 6020000000,
    "otherWorkingCapital": 3821000000,
    "otherNonCashItems": -2266000000,
    "netCashProvidedByOperatingActivities": 118254000000,
    "investmentsInPropertyPlantAndEquipment": -9447000000,
    "acquisitionsNet": 0,
    "purchasesOfInvestments": -48656000000,
    "salesMaturitiesOfInvestments": 62346000000,
    "otherInvestingActivities": -1308000000,
    "netCashProvidedByInvestingActivities": 2935000000,
    "netDebtIssuance": -5998000000,
    "longTermNetDebtIssuance": -9958000000,
    "shortTermNetDebtIssuance": 3960000000,
    "netStockIssuance": -94949000000,
    "netCommonStockIssuance": -94949000000,
    "commonStockIssuance": 0,
    "commonStockRepurchased": -94949000000,
    "netPreferredStockIssuance": 0,
    "netDividendsPaid": -15234000000,
    "commonDividendsPaid": -15234000000,
    "preferredDividendsPaid": 0,
    "otherFinancingActivities": -5802000000,
    "netCashProvidedByFinancingActivities": -121983000000,
    "effectOfForexChangesOnCash": 0,
    "netChangeInCash": -794000000,
    "cashAtEndOfPeriod": 29943000000,
    "cashAtBeginningOfPeriod": 30737000000,
    "operatingCashFlow": 118254000000,
    "capitalExpenditure": -9447000000,
    "freeCashFlow": 108807000000,
    "incomeTaxesPaid": 26102000000,
    "interestPaid": 0
  }
]
```

---

## Financial Ratios TTM API

Gain access to trailing twelve-month (TTM) financial ratios with the TTM Ratios API. This API provides key performance metrics over the past year, including profitability, liquidity, and efficiency ratios.

**Endpoint:**
```
https://financialmodelingprep.com/stable/ratios-ttm?symbol=AAPL
```

**Parameters:**

| Parameter | Type | Required | Example | Description |
|-----------|------|----------|---------|-------------|
| symbol | string | ✓ | AAPL | Stock ticker symbol |

**Note:** Currency is as reported in financials

**Response Example:**
```json
[
  {
    "symbol": "AAPL",
    "grossProfitMarginTTM": 0.46518849807964424,
    "ebitMarginTTM": 0.3175535678188801,
    "ebitdaMarginTTM": 0.34705882352941175,
    "operatingProfitMarginTTM": 0.3175535678188801,
    "pretaxProfitMarginTTM": 0.31773296947645036,
    "continuousOperationsProfitMarginTTM": 0.24295027289266222,
    "netProfitMarginTTM": 0.24295027289266222,
    "bottomLineProfitMarginTTM": 0.24295027289266222,
    "receivablesTurnoverTTM": 6.673186524129093,
    "payablesTurnoverTTM": 3.4187853335486995,
    "inventoryTurnoverTTM": 30.626103313558097,
    "fixedAssetTurnoverTTM": 8.590592372311098,
    "assetTurnoverTTM": 1.1501809145995903,
    "currentRatioTTM": 0.9229383853427077,
    "quickRatioTTM": 0.8750666712845911,
    "solvencyRatioTTM": 0.3888081578786054,
    "cashRatioTTM": 0.20987774044955496,
    "priceToEarningsRatioTTM": 32.889608822880916,
    "priceToEarningsGrowthRatioTTM": 9.104441715061135,
    "forwardPriceToEarningsGrowthRatioTTM": 9.104441715061135,
    "priceToBookRatioTTM": 47.370141231313106,
    "priceToSalesRatioTTM": 7.958949686678795,
    "priceToFreeCashFlowRatioTTM": 32.04339747098139,
    "priceToOperatingCashFlowRatioTTM": 29.201395167968677,
    "debtToAssetsRatioTTM": 0.28132292892744526,
    "debtToEquityRatioTTM": 1.4499985020521886,
    "debtToCapitalRatioTTM": 0.5918364851397372,
    "longTermDebtToCapitalRatioTTM": 0.557055084464615,
    "financialLeverageRatioTTM": 5.154213727193745,
    "workingCapitalTurnoverRatioTTM": -22.92267593397046,
    "operatingCashFlowRatioTTM": 0.7501402694558931,
    "operatingCashFlowSalesRatioTTM": 0.2736355366889024,
    "freeCashFlowOperatingCashFlowRatioTTM": 0.9077049513361775,
    "debtServiceCoverageRatioTTM": 8.390251498870981,
    "interestCoverageRatioTTM": 0,
    "shortTermOperatingCashFlowCoverageRatioTTM": 8.432142022891847,
    "operatingCashFlowCoverageRatioTTM": 1.1187512267688715,
    "capitalExpenditureCoverageRatioTTM": 10.834817408704351,
    "dividendPaidAndCapexCoverageRatioTTM": 4.287173396674584,
    "dividendPayoutRatioTTM": 0.15876235049401977,
    "dividendYieldTTM": 0.0047691720717283476,
    "enterpriseValueTTM": 3216333928000,
    "revenuePerShareTTM": 26.24103186081379,
    "netIncomePerShareTTM": 6.375265851569754,
    "interestDebtPerShareTTM": 6.418298067250137,
    "cashPerShareTTM": 3.565573803101025,
    "bookValuePerShareTTM": 4.426417032959892,
    "tangibleBookValuePerShareTTM": 4.426417032959892,
    "shareholdersEquityPerShareTTM": 4.426417032959892,
    "operatingCashFlowPerShareTTM": 7.180478836504368,
    "capexPerShareTTM": 0.6627226436447186,
    "freeCashFlowPerShareTTM": 6.5177561928596495,
    "netIncomePerEBTTTM": 0.7646366484818603,
    "ebtPerEbitTTM": 1.0005649492739208,
    "priceToFairValueTTM": 47.370141231313106,
    "debtToMarketCapTTM": 0.030731461471514124,
    "effectiveTaxRateTTM": 0.23536335151813975,
    "enterpriseValueMultipleTTM": 23.41672438697653
  }
]
```

---

## Key Metrics TTM API

Retrieve a comprehensive set of trailing twelve-month (TTM) key performance metrics with the TTM Key Metrics API. Access data related to a company's profitability, capital efficiency, and liquidity, allowing for detailed analysis of its financial health over the past year.

**Endpoint:**
```
https://financialmodelingprep.com/stable/key-metrics-ttm?symbol=AAPL
```

**Parameters:**

| Parameter | Type | Required | Example | Description |
|-----------|------|----------|---------|-------------|
| symbol | string | ✓ | AAPL | Stock ticker symbol |

**Note:** Currency is as reported in financials

**Response Example:**
```json
[
  {
    "symbol": "AAPL",
    "marketCap": 3149833928000,
    "enterpriseValueTTM": 3216333928000,
    "evToSalesTTM": 8.126980816656559,
    "evToOperatingCashFlowTTM": 29.70001965021146,
    "evToFreeCashFlowTTM": 32.71990486169747,
    "evToEBITDATTM": 23.41672438697653,
    "netDebtToEBITDATTM": 0.48415749315627005,
    "currentRatioTTM": 0.9229383853427077,
    "incomeQualityTTM": 1.1263026521060842,
    "grahamNumberTTM": 25.198029099282905,
    "grahamNetNetTTM": -11.64435843011051,
    "taxBurdenTTM": 0.7646366484818603,
    "interestBurdenTTM": 1.0005649492739208,
    "workingCapitalTTM": -11125000000,
    "investedCapitalTTM": 34944000000,
    "returnOnAssetsTTM": 0.27943676707790227,
    "operatingReturnOnAssetsTTM": 0.35448090090471257,
    "returnOnTangibleAssetsTTM": 0.27943676707790227,
    "returnOnEquityTTM": 1.4534598087751787,
    "returnOnInvestedCapitalTTM": 0.45208108089346594,
    "returnOnCapitalEmployedTTM": 0.6292559583416784,
    "earningsYieldTTM": 0.030404739849149914,
    "freeCashFlowYieldTTM": 0.03120767705439485,
    "capexToOperatingCashFlowTTM": 0.09229504866382256,
    "capexToDepreciationTTM": 0.855956153121521,
    "capexToRevenueTTM": 0.025255205174853447,
    "salesGeneralAndAdministrativeToRevenueTTM": 0,
    "researchAndDevelopementToRevenueTTM": 0.08071053163533455,
    "stockBasedCompensationToRevenueTTM": 0.030263290883363655,
    "intangiblesToTotalAssetsTTM": 0,
    "averageReceivablesTTM": 62774500000,
    "averagePayablesTTM": 65435000000,
    "averageInventoryTTM": 7098500000,
    "daysOfSalesOutstandingTTM": 54.69650798463715,
    "daysOfPayablesOutstandingTTM": 106.76306476988712,
    "daysOfInventoryOutstandingTTM": 11.917937984569374,
    "operatingCycleTTM": 66.61444596920653,
    "cashConversionCycleTTM": -40.148618800680595,
    "freeCashFlowToEquityTTM": 31799000000,
    "freeCashFlowToFirmTTM": 85497710797.9578,
    "tangibleAssetValueTTM": 66758000000,
    "netCurrentAssetValueTTM": -144087000000
  }
]
```

---

## S&P 500 Index API

Access detailed data on the S&P 500 index using the S&P 500 Index API. Track the performance and key information of the companies that make up this major stock market index.

**Endpoint:**
```
https://financialmodelingprep.com/stable/sp500-constituent
```

**Parameters:**
No parameters required

**Response Example:**
```json
[
  {
    "symbol": "APO",
    "name": "Apollo Global Management",
    "sector": "Financial Services",
    "subSector": "Asset Management - Global",
    "headQuarter": "New York City, New York",
    "dateFirstAdded": "2024-12-23",
    "cik": "0001858681",
    "founded": "1990"
  }
]
```

---

## Sector PE Snapshot API

Retrieve the price-to-earnings (P/E) ratios for various sectors using the Sector P/E Snapshot API. Compare valuation levels across sectors to better understand market valuations.

**Endpoint:**
```
https://financialmodelingprep.com/stable/sector-pe-snapshot?date=2024-02-01
```

**Parameters:**

| Parameter | Type | Required | Example | Description |
|-----------|------|----------|---------|-------------|
| date | string | ✓ | 2024-02-01 | Date for snapshot |
| exchange | string | | NASDAQ | Stock exchange |
| sector | string | | Energy | Specific sector |

**Response Example:**
```json
[
  {
    "date": "2024-02-01",
    "sector": "Basic Materials",
    "exchange": "NASDAQ",
    "pe": 15.687711758428254
  }
]
```

---

## Historical Sector PE API

Access historical price-to-earnings (P/E) ratios for various sectors using the Historical Sector P/E API. Analyze how sector valuations have evolved over time to understand long-term trends and market shifts.

**Endpoint:**
```
https://financialmodelingprep.com/stable/historical-sector-pe?sector=Energy
```

**Parameters:**

| Parameter | Type | Required | Example | Description |
|-----------|------|----------|---------|-------------|
| sector | string | ✓ | Energy | Specific sector |
| from | string | | 2024-02-01 | Start date |
| to | string | | 2024-03-01 | End date |
| exchange | string | | NASDAQ | Stock exchange |

**Response Example:**
```json
[
  {
    "date": "2024-02-01",
    "sector": "Energy",
    "exchange": "NASDAQ",
    "pe": 14.411400922841464
  }
]
```

---

## Treasury Rates API

Access real-time and historical Treasury rates for all maturities with the FMP Treasury Rates API. Track key benchmarks for interest rates across the economy.

**Endpoint:**
```
https://financialmodelingprep.com/stable/treasury-rates
```

**Parameters:**

| Parameter | Type | Required | Example | Description |
|-----------|------|----------|---------|-------------|
| from | date | | 2025-01-10 | Start date |
| to | date | | 2025-04-10 | End date |

**Note:** Maximum 90-day date range

**Response Example:**
```json
[
  {
    "date": "2024-02-29",
    "month1": 5.53,
    "month2": 5.5,
    "month3": 5.45,
    "month6": 5.3,
    "year1": 5.01,
    "year2": 4.64,
    "year3": 4.43,
    "year5": 4.26,
    "year7": 4.28,
    "year10": 4.25,
    "year20": 4.51,
    "year30": 4.38
  }
]
```

---

## Economics Indicators API

Access real-time and historical economic data for key indicators like GDP, unemployment, and inflation with the FMP Economic Indicators API. Use this data to measure economic performance and identify growth trends.

**Endpoint:**
```
https://financialmodelingprep.com/stable/economic-indicators?name=GDP
```

**Parameters:**

| Parameter | Type | Required | Example | Description |
|-----------|------|----------|---------|-------------|
| name | string | ✓ | GDP | Economic indicator name |
| from | date | | 2025-01-10 | Start date |
| to | date | | 2025-04-10 | End date |

**Available Indicators:**
- GDP
- realGDP
- nominalPotentialGDP
- realGDPPerCapita
- federalFunds
- CPI
- inflationRate
- inflation
- retailSales
- consumerSentiment
- durableGoods
- unemploymentRate
- totalNonfarmPayroll
- initialClaims
- industrialProductionTotalIndex
- newPrivatelyOwnedHousingUnitsStartedTotalUnits
- totalVehicleSales
- retailMoneyFunds
- smoothedUSRecessionProbabilities
- 3MonthOr90DayRatesAndYieldsCertificatesOfDeposit
- commercialBankInterestRateOnCreditCardPlansAllAccounts
- 30YearFixedRateMortgageAverage
- 15YearFixedRateMortgageAverage

**Note:** Maximum 90-day date range

**Response Example:**
```json
[
  {
    "name": "GDP",
    "date": "2024-01-01",
    "value": 28624.069
  }
]
```

---

## Historical Market Cap API

Access historical market capitalization data for a company using the FMP Historical Market Capitalization API. This API helps track the changes in market value over time, enabling long-term assessments of a company's growth or decline.

**Endpoint:**
```
https://financialmodelingprep.com/stable/historical-market-capitalization?symbol=AAPL
```

**Parameters:**

| Parameter | Type | Required | Example | Description |
|-----------|------|----------|---------|-------------|
| symbol | string | ✓ | AAPL | Stock ticker symbol |
| limit | number | | 100 | Maximum records (up to 5000) |
| from | date | | 2024-01-01 | Start date |
| to | date | | 2024-03-01 | End date |

**Note:** Currency is as trading

**Response Example:**
```json
[
  {
    "symbol": "AAPL",
    "date": "2024-02-29",
    "marketCap": 2784608472000
  }
]
```