#!/bin/bash
# Commands to load top stocks by market cap
# Generated on 2025-06-12 22:34:58.495912
# Top 50 stocks, 5 years of data

echo "Running batch 1/10..."
python scripts/run_daily_pipeline.py --symbols MSFT NVDA AAPL AMZN GOOGL --from-date 2020-06-13 --to-date 2025-06-12 --period quarterly --limit 20
echo "Batch complete. Sleeping 5 seconds..."
sleep 5

echo "Running batch 2/10..."
python scripts/run_daily_pipeline.py --symbols META AVGO BRK-B TSLA LLY --from-date 2020-06-13 --to-date 2025-06-12 --period quarterly --limit 20
echo "Batch complete. Sleeping 5 seconds..."
sleep 5

echo "Running batch 3/10..."
python scripts/run_daily_pipeline.py --symbols WMT JPM V MA NFLX --from-date 2020-06-13 --to-date 2025-06-12 --period quarterly --limit 20
echo "Batch complete. Sleeping 5 seconds..."
sleep 5

echo "Running batch 4/10..."
python scripts/run_daily_pipeline.py --symbols COST PG JNJ HD ABBV --from-date 2020-06-13 --to-date 2025-06-12 --period quarterly --limit 20
echo "Batch complete. Sleeping 5 seconds..."
sleep 5

echo "Running batch 5/10..."
python scripts/run_daily_pipeline.py --symbols BAC KO UNH PM IBM --from-date 2020-06-13 --to-date 2025-06-12 --period quarterly --limit 20
echo "Batch complete. Sleeping 5 seconds..."
sleep 5

echo "Running batch 6/10..."
python scripts/run_daily_pipeline.py --symbols CSCO GE CRM CVX WFC --from-date 2020-06-13 --to-date 2025-06-12 --period quarterly --limit 20
echo "Batch complete. Sleeping 5 seconds..."
sleep 5

echo "Running batch 7/10..."
python scripts/run_daily_pipeline.py --symbols ABT DIS MS AXP MRK --from-date 2020-06-13 --to-date 2025-06-12 --period quarterly --limit 20
echo "Batch complete. Sleeping 5 seconds..."
sleep 5

echo "Running batch 8/10..."
python scripts/run_daily_pipeline.py --symbols T ACN GS RTX VZ --from-date 2020-06-13 --to-date 2025-06-12 --period quarterly --limit 20
echo "Batch complete. Sleeping 5 seconds..."
sleep 5

echo "Running batch 9/10..."
python scripts/run_daily_pipeline.py --symbols PEP TXN ADBE QCOM CAT --from-date 2020-06-13 --to-date 2025-06-12 --period quarterly --limit 20
echo "Batch complete. Sleeping 5 seconds..."
sleep 5

echo "Running batch 10/10..."
python scripts/run_daily_pipeline.py --symbols SCHW AMGN TMO SPGI BLK --from-date 2020-06-13 --to-date 2025-06-12 --period quarterly --limit 20
echo "Batch complete. Sleeping 5 seconds..."
sleep 5

