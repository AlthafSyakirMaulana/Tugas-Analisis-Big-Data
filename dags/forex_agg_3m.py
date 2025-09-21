from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import psycopg2, os

PG_CONN = dict(
    host=os.getenv("POSTGRES_HOST", "postgres"),
    dbname=os.getenv("POSTGRES_DB", "raw"),
    user=os.getenv("POSTGRES_USER", "postgres"),
    password=os.getenv("POSTGRES_PASSWORD", "postgres"),
)

SQL = """
WITH w AS (
  SELECT
    base_currency,
    symbol,
    date_trunc('minute', fetched_at) - interval '1 minute' * (extract(minute from fetched_at)::int % 3) AS window_start,
    date_trunc('minute', fetched_at) - interval '1 minute' * (extract(minute from fetched_at)::int % 3) + interval '3 minute' AS window_end,
    rate,
    fetched_at
  FROM raw_currency
  WHERE fetched_at >= now() - interval '1 hour'
),
agg AS (
  SELECT
    window_start, window_end, base_currency, symbol,
    (ARRAY_AGG(rate ORDER BY fetched_at ASC))[1]  AS rate_open,
    (ARRAY_AGG(rate ORDER BY fetched_at DESC))[1] AS rate_close,
    MIN(rate) AS rate_min,
    MAX(rate) AS rate_max,
    AVG(rate) AS rate_avg,
    COUNT(*)  AS samples
  FROM w
  GROUP BY window_start, window_end, base_currency, symbol
)
INSERT INTO agg_currency_3m
(window_start, window_end, base_currency, symbol,
 rate_open, rate_close, rate_min, rate_max, rate_avg, pct_change, samples)
SELECT
  a.window_start, a.window_end, a.base_currency, a.symbol,
  a.rate_open, a.rate_close, a.rate_min, a.rate_max, a.rate_avg,
  CASE WHEN a.rate_open = 0 THEN 0
       ELSE ((a.rate_close - a.rate_open) / a.rate_open) * 100 END AS pct_change,
  a.samples
FROM agg a
ON CONFLICT (window_start, base_currency, symbol) DO UPDATE
SET rate_open  = EXCLUDED.rate_open,
    rate_close = EXCLUDED.rate_close,
    rate_min   = EXCLUDED.rate_min,
    rate_max   = EXCLUDED.rate_max,
    rate_avg   = EXCLUDED.rate_avg,
    pct_change = EXCLUDED.pct_change,
    samples    = EXCLUDED.samples;
"""

def run_sql():
    conn = psycopg2.connect(**PG_CONN)
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(SQL)
    conn.close()

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 0,
}

with DAG(
    dag_id="forex_agg_3m",
    start_date=datetime(2025, 1, 1),
    schedule_interval="*/3 * * * *",
    catchup=False,
    default_args=default_args,
    dagrun_timeout=timedelta(minutes=5),
    description="Aggregate forex rates into 3-minute windows",
) as dag:
    PythonOperator(task_id="aggregate_into_3m", python_callable=run_sql)
