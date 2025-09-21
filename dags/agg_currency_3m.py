from datetime import datetime, timedelta, timezone
from airflow import DAG
from airflow.operators.python import PythonOperator
import psycopg2

PG = dict(host="postgres", dbname="raw", user="postgres", password="postgres")

def load_currency():
    t_end = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    t_start = t_end - timedelta(minutes=3)
    sql = """
    WITH w AS (
      SELECT
        %(t_start)s AS window_start,
        %(t_end)s   AS window_end,
        base_currency, symbol,
        MIN(fetched_at) AS first_ts,
        MAX(fetched_at) AS last_ts,
        COUNT(*) AS samples
      FROM raw_currency
      WHERE fetched_at >= %(t_start)s AND fetched_at < %(t_end)s
      GROUP BY base_currency, symbol
    ),
    ohlc AS (
      SELECT
        w.window_start, w.window_end, rc.base_currency, rc.symbol, w.samples,
        (SELECT rate FROM raw_currency r1
          WHERE r1.base_currency=w.base_currency AND r1.symbol=w.symbol AND r1.fetched_at=w.first_ts
          ORDER BY id ASC LIMIT 1) AS rate_open,
        (SELECT rate FROM raw_currency r2
          WHERE r2.base_currency=w.base_currency AND r2.symbol=w.symbol AND r2.fetched_at=w.last_ts
          ORDER BY id DESC LIMIT 1) AS rate_close,
        MIN(rc.rate) AS rate_min,
        MAX(rc.rate) AS rate_max,
        AVG(rc.rate) AS rate_avg
      FROM w
      JOIN raw_currency rc
        ON rc.base_currency=w.base_currency AND rc.symbol=w.symbol
       AND rc.fetched_at >= w.window_start AND rc.fetched_at < w.window_end
      GROUP BY w.window_start, w.window_end, rc.base_currency, rc.symbol, w.samples, w.first_ts, w.last_ts
    )
    INSERT INTO agg_currency_3m
      (window_start, window_end, base_currency, symbol,
       rate_open, rate_close, rate_min, rate_max, rate_avg, pct_change, samples)
    SELECT
      window_start, window_end, base_currency, symbol,
      rate_open, rate_close, rate_min, rate_max, rate_avg,
      CASE WHEN rate_open IS NULL OR rate_open=0 THEN NULL
           ELSE (rate_close - rate_open) / rate_open END,
      samples
    FROM ohlc
    ON CONFLICT (window_start, base_currency, symbol) DO NOTHING;
    """
    conn = psycopg2.connect(**PG); conn.autocommit=True
    cur = conn.cursor()
    cur.execute(sql, {"t_start": t_start, "t_end": t_end})
    cur.close(); conn.close()

with DAG(
    dag_id="agg_currency_3m",
    schedule_interval="*/3 * * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(seconds=10)}
) as dag:
    PythonOperator(task_id="transform_load_currency", python_callable=load_currency)
