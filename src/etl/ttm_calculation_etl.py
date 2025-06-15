"""
ETL pipeline for calculating and loading TTM (Trailing Twelve Month) financial metrics
"""

from typing import Dict, List, Any, Optional
from datetime import datetime, timezone
from loguru import logger

from src.etl.base_etl import BaseETL
from src.db.snowflake_connector import SnowflakeConnector
from src.utils.config import Config


class TTMCalculationETL(BaseETL):
    """ETL pipeline for calculating TTM financial metrics"""

    def __init__(self, config: Config):
        """
        Initialize TTM Calculation ETL

        Args:
            config: Application configuration
        """
        # Create Snowflake connector
        snowflake_connector = SnowflakeConnector(config.snowflake)

        # Initialize base class
        super().__init__(
            job_name="ttm_calculation_etl",
            snowflake_connector=snowflake_connector,
            fmp_client=None,  # Not needed for calculations
            batch_size=config.app.batch_size,
            enable_monitoring=config.app.enable_monitoring,
        )

        # Store config for later use
        self.config = config

    def extract(self, symbols: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """
        Extract quarterly financial data to calculate TTM metrics

        Args:
            symbols: Optional list of symbols to process. If None, process all.

        Returns:
            List of potential TTM calculation opportunities
        """
        logger.info("Extracting data for TTM calculations")

        # Query to find all dates where we have 4 quarters of data available
        query = """
        WITH quarterly_data AS (
            -- Get all quarterly financial data with accepted dates
            SELECT 
                c.company_key,
                c.symbol,
                ff.fiscal_date_key,
                fd.date as fiscal_date,
                ff.accepted_date,
                ff.period_type,
                ff.revenue,
                ff.cost_of_revenue,
                ff.gross_profit,
                ff.operating_expenses,
                ff.operating_income,
                ff.net_income,
                ff.eps,
                ff.eps_diluted,
                ff.operating_cash_flow,
                ff.investing_cash_flow,
                ff.financing_cash_flow,
                ff.free_cash_flow,
                ff.capital_expenditures,
                ff.dividends_paid,
                ff.shares_outstanding,
                ff.total_assets,
                ff.current_assets,
                ff.total_liabilities,
                ff.current_liabilities,
                ff.total_equity,
                ff.cash_and_equivalents,
                ff.total_debt,
                ff.net_debt
            FROM ANALYTICS.FACT_FINANCIALS ff
            JOIN ANALYTICS.DIM_COMPANY c ON ff.company_key = c.company_key
            JOIN ANALYTICS.DIM_DATE fd ON ff.fiscal_date_key = fd.date_key
            WHERE ff.period_type IN ('Q1', 'Q2', 'Q3', 'Q4')
        ),
        calculation_dates AS (
            -- Find all unique accepted dates where new quarterly data became available
            SELECT DISTINCT
                company_key,
                symbol,
                accepted_date::DATE as calculation_date,
                accepted_date
            FROM quarterly_data
        ),
        ttm_opportunities AS (
            -- For each calculation date, check if we have 4 quarters available
            SELECT 
                cd.company_key,
                cd.symbol,
                cd.calculation_date,
                cd.accepted_date,
                COUNT(DISTINCT qd.fiscal_date_key) as quarters_available,
                MIN(qd.fiscal_date) as oldest_quarter_date,
                MAX(qd.fiscal_date) as newest_quarter_date
            FROM calculation_dates cd
            JOIN quarterly_data qd 
                ON cd.company_key = qd.company_key
                AND qd.accepted_date <= cd.accepted_date
                AND qd.fiscal_date > DATEADD(month, -15, cd.calculation_date)  -- Look back ~15 months
            GROUP BY cd.company_key, cd.symbol, cd.calculation_date, cd.accepted_date
            HAVING COUNT(DISTINCT qd.fiscal_date_key) >= 4
        )
        SELECT DISTINCT
            t.company_key,
            t.symbol,
            t.calculation_date,
            t.accepted_date,
            t.quarters_available,
            t.oldest_quarter_date,
            t.newest_quarter_date
        FROM ttm_opportunities t
        -- Exclude dates where we already have TTM calculations
        LEFT JOIN ANALYTICS.FACT_FINANCIALS_TTM existing
            ON t.company_key = existing.company_key
            AND t.calculation_date = existing.calculation_date
        WHERE existing.ttm_key IS NULL
        """

        params = []

        # Add symbol filter if provided
        if symbols:
            placeholders = ",".join(["%s" for _ in symbols])
            query += f" AND t.symbol IN ({placeholders})"
            params.extend(symbols)

        query += " ORDER BY t.symbol, t.calculation_date"

        try:
            records = self.snowflake.fetch_all(query, tuple(params) if params else None)
            logger.info(f"Found {len(records)} TTM calculation opportunities")
            return records
        except Exception as e:
            logger.error(f"Failed to extract TTM opportunities: {e}")
            raise

    def transform(
        self, raw_data: List[Dict[str, Any]]
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Calculate TTM metrics for each opportunity

        Args:
            raw_data: List of TTM calculation opportunities

        Returns:
            Dict with 'ttm_records' key containing calculated TTM data
        """
        logger.info(f"Calculating TTM metrics for {len(raw_data)} opportunities")

        ttm_records = []

        for opportunity in raw_data:
            try:
                # Get the 4 most recent quarters for this calculation date
                query = """
                WITH available_quarters AS (
                    SELECT 
                        ff.*,
                        fd.date as fiscal_date,
                        ROW_NUMBER() OVER (ORDER BY ff.fiscal_date_key DESC) as quarter_rank
                    FROM ANALYTICS.FACT_FINANCIALS ff
                    JOIN ANALYTICS.DIM_DATE fd ON ff.fiscal_date_key = fd.date_key
                    WHERE ff.company_key = %s
                    AND ff.period_type IN ('Q1', 'Q2', 'Q3', 'Q4')
                    AND ff.accepted_date <= %s
                    AND fd.date > DATEADD(month, -15, %s)
                )
                SELECT 
                    -- Flow metrics (SUM)
                    SUM(revenue) as ttm_revenue,
                    SUM(cost_of_revenue) as ttm_cost_of_revenue,
                    SUM(gross_profit) as ttm_gross_profit,
                    SUM(operating_expenses) as ttm_operating_expenses,
                    SUM(operating_income) as ttm_operating_income,
                    SUM(net_income) as ttm_net_income,
                    SUM(eps) as ttm_eps,
                    SUM(eps_diluted) as ttm_eps_diluted,
                    SUM(operating_cash_flow) as ttm_operating_cash_flow,
                    SUM(investing_cash_flow) as ttm_investing_cash_flow,
                    SUM(financing_cash_flow) as ttm_financing_cash_flow,
                    SUM(free_cash_flow) as ttm_free_cash_flow,
                    SUM(capital_expenditures) as ttm_capital_expenditures,
                    SUM(dividends_paid) as ttm_dividends_paid,
                    -- Stock metrics (from most recent quarter)
                    MAX(CASE WHEN quarter_rank = 1 THEN shares_outstanding END) as latest_shares_outstanding,
                    MAX(CASE WHEN quarter_rank = 1 THEN total_assets END) as latest_total_assets,
                    MAX(CASE WHEN quarter_rank = 1 THEN current_assets END) as latest_current_assets,
                    MAX(CASE WHEN quarter_rank = 1 THEN total_liabilities END) as latest_total_liabilities,
                    MAX(CASE WHEN quarter_rank = 1 THEN current_liabilities END) as latest_current_liabilities,
                    MAX(CASE WHEN quarter_rank = 1 THEN total_equity END) as latest_total_equity,
                    MAX(CASE WHEN quarter_rank = 1 THEN cash_and_equivalents END) as latest_cash_and_equivalents,
                    MAX(CASE WHEN quarter_rank = 1 THEN total_debt END) as latest_total_debt,
                    MAX(CASE WHEN quarter_rank = 1 THEN net_debt END) as latest_net_debt,
                    -- Metadata
                    COUNT(*) as quarters_used,
                    MIN(fiscal_date) as oldest_quarter,
                    MAX(fiscal_date) as newest_quarter
                FROM available_quarters
                WHERE quarter_rank <= 4
                """

                params = (
                    opportunity.get("company_key", opportunity.get("COMPANY_KEY")),
                    opportunity.get("accepted_date", opportunity.get("ACCEPTED_DATE")),
                    opportunity.get(
                        "calculation_date", opportunity.get("CALCULATION_DATE")
                    ),
                )

                result = self.snowflake.fetch_all(query, params)

                if result and len(result) > 0:
                    ttm_data = result[0]

                    # Create TTM record
                    ttm_record = {
                        "company_key": opportunity.get(
                            "company_key", opportunity.get("COMPANY_KEY")
                        ),
                        "calculation_date": opportunity.get(
                            "calculation_date", opportunity.get("CALCULATION_DATE")
                        ),
                        "accepted_date": opportunity.get(
                            "accepted_date", opportunity.get("ACCEPTED_DATE")
                        ),
                        "quarters_included": ttm_data["QUARTERS_USED"],
                        "oldest_quarter_date": ttm_data["OLDEST_QUARTER"],
                        "newest_quarter_date": ttm_data["NEWEST_QUARTER"],
                        # Flow metrics
                        "ttm_revenue": ttm_data["TTM_REVENUE"],
                        "ttm_cost_of_revenue": ttm_data["TTM_COST_OF_REVENUE"],
                        "ttm_gross_profit": ttm_data["TTM_GROSS_PROFIT"],
                        "ttm_operating_expenses": ttm_data["TTM_OPERATING_EXPENSES"],
                        "ttm_operating_income": ttm_data["TTM_OPERATING_INCOME"],
                        "ttm_net_income": ttm_data["TTM_NET_INCOME"],
                        "ttm_eps": ttm_data["TTM_EPS"],
                        "ttm_eps_diluted": ttm_data["TTM_EPS_DILUTED"],
                        "ttm_operating_cash_flow": ttm_data["TTM_OPERATING_CASH_FLOW"],
                        "ttm_investing_cash_flow": ttm_data["TTM_INVESTING_CASH_FLOW"],
                        "ttm_financing_cash_flow": ttm_data["TTM_FINANCING_CASH_FLOW"],
                        "ttm_free_cash_flow": ttm_data["TTM_FREE_CASH_FLOW"],
                        "ttm_capital_expenditures": ttm_data[
                            "TTM_CAPITAL_EXPENDITURES"
                        ],
                        "ttm_dividends_paid": ttm_data["TTM_DIVIDENDS_PAID"],
                        # Stock metrics
                        "latest_shares_outstanding": ttm_data[
                            "LATEST_SHARES_OUTSTANDING"
                        ],
                        "latest_total_assets": ttm_data["LATEST_TOTAL_ASSETS"],
                        "latest_current_assets": ttm_data["LATEST_CURRENT_ASSETS"],
                        "latest_total_liabilities": ttm_data[
                            "LATEST_TOTAL_LIABILITIES"
                        ],
                        "latest_current_liabilities": ttm_data[
                            "LATEST_CURRENT_LIABILITIES"
                        ],
                        "latest_total_equity": ttm_data["LATEST_TOTAL_EQUITY"],
                        "latest_cash_and_equivalents": ttm_data[
                            "LATEST_CASH_AND_EQUIVALENTS"
                        ],
                        "latest_total_debt": ttm_data["LATEST_TOTAL_DEBT"],
                        "latest_net_debt": ttm_data["LATEST_NET_DEBT"],
                    }

                    ttm_records.append(ttm_record)

            except Exception as e:
                logger.error(
                    f"Failed to calculate TTM for {opportunity.get('symbol', opportunity.get('SYMBOL'))} on {opportunity.get('calculation_date', opportunity.get('CALCULATION_DATE'))}: {e}"
                )

        logger.info(f"Successfully calculated {len(ttm_records)} TTM records")
        return {"ttm_records": ttm_records}

    def load(self, transformed_data: Dict[str, List[Dict[str, Any]]]) -> int:
        """
        Load calculated TTM metrics to FACT_FINANCIALS_TTM

        Args:
            transformed_data: Dict with 'ttm_records' key containing TTM data

        Returns:
            Number of records loaded
        """
        ttm_records = transformed_data.get("ttm_records", [])

        if not ttm_records:
            logger.warning("No TTM data to load")
            return 0

        logger.info(f"Loading {len(ttm_records)} TTM records to FACT_FINANCIALS_TTM")

        try:
            # Bulk insert TTM records
            affected = self.snowflake.bulk_insert(
                "ANALYTICS.FACT_FINANCIALS_TTM", ttm_records
            )
            logger.info(
                f"Successfully loaded {affected} records to FACT_FINANCIALS_TTM"
            )
            return affected

        except Exception as e:
            logger.error(f"Failed to load TTM data: {e}")
            raise

    def run(self, symbols: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        Run the complete TTM calculation ETL pipeline

        Args:
            symbols: Optional list of symbols to process

        Returns:
            ETL result summary
        """
        logger.info(f"Starting {self.job_name} ETL pipeline")

        try:
            # Extract
            opportunities = self.extract(symbols)

            if not opportunities:
                logger.info("No new TTM calculations needed")
                return {
                    "status": "success",
                    "records_processed": 0,
                    "message": "No new TTM calculations needed",
                }

            # Transform
            transformed_data = self.transform(opportunities)

            # Load
            records_loaded = self.load(transformed_data)

            return {
                "status": "success",
                "opportunities_found": len(opportunities),
                "records_loaded": records_loaded,
            }

        except Exception as e:
            logger.error(f"TTM calculation ETL pipeline failed: {e}")
            return {"status": "failed", "error": str(e), "records_processed": 0}
