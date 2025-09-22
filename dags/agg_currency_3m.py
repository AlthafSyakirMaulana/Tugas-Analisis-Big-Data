from datetime import timedelta
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

def load_currency(**context):
    t_start = context["data_interval_start"]
    t_end   = context["data_interval_end"]

    hook = PostgresHook(postgres_conn_id="postgres_raw")  # atau "POSTGRES_RAW" kalau id-mu huruf besar

    sql = """
    -- Hapus window yang sama agar idempotent
    DELETE FROM agg_currency_3m
    WHERE window_start >= %(t_start)s AND window_start < %(t_end)s;

    WITH w AS (
        SELECT
            -- bucket 3 menit untuk tiap bar
            (date_trunc('minute', r.fetched_at)
             - make_interval(mins => MOD(extract(minute from r.fetched_at)::int, 3)))      AS window_start,
            (date_trunc('minute', r.fetched_at)
             - make_interval(mins => MOD(extract(minute from r.fetched_at)::int, 3))
            ) + interval '3 minute'                                                         AS window_end,
            r.base_currency,
            r.symbol,
            r.rate,
            r.fetched_at
        FROM raw_currency r
        WHERE r.fetched_at >= %(t_start)s
          AND r.fetched_at <  %(t_end)s
    ),
    ordered AS (
        SELECT
            window_start, window_end, base_currency, symbol, rate, fetched_at,
            row_number() OVER (PARTITION BY window_start, base_currency, symbol
                               ORDER BY fetched_at ASC)  AS rn_open,
            row_number() OVER (PARTITION BY window_start, base_currency, symbol
                               ORDER BY fetched_at DESC) AS rn_close
        FROM w
    ),
    agg AS (
        SELECT
            window_start,
            window_end,
            base_currency,
            symbol,
            MAX(rate) FILTER (WHERE rn_open  = 1) AS rate_open,
            MAX(rate) FILTER (WHERE rn_close = 1) AS rate_close,
            MIN(rate)                            AS rate_min,
            MAX(rate)                            AS rate_max,
            AVG(rate)                            AS rate_avg,
            COUNT(*)                             AS samples
        FROM ordered
        GROUP BY window_start, window_end, base_currency, symbol
    )
    INSERT INTO agg_currency_3m (
        window_start, window_end, base_currency, symbol,
        rate_open, rate_close, rate_min, rate_max, rate_avg, pct_change, samples
    )
    SELECT
        a.window_start,
        a.window_end,
        a.base_currency,
        a.symbol,
        a.rate_open,
        a.rate_close,
        a.rate_min,
        a.rate_max,
        a.rate_avg,
        CASE
            WHEN a.rate_open IS NULL OR a.rate_open = 0 THEN NULL
            ELSE ((a.rate_close - a.rate_open) / a.rate_open) * 100.0
        END AS pct_change,
        a.samples
    FROM agg a;
    """
    hook.run(sql, parameters={"t_start": t_start, "t_end": t_end})

with DAG(
    dag_id="agg_currency_3m",
    schedule="*/3 * * * *",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(seconds=10)},
    tags=["forex"],
) as dag:
    PythonOperator(
        task_id="transform_load_currency",
        python_callable=load_currency,
    )
