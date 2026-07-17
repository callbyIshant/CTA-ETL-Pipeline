import logging
from datetime import datetime

import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import dag, task

RAW_CSV_PATH = "/opt/airflow/data/raw/cta_ridership_daily.csv"
STAGING_DDL_PATH = "/opt/airflow/sql/staging/create_staging_table.sql"
TRANSFORM_SQL_PATH = "/opt/airflow/sql/transform/create_analytics_table.sql"

logger = logging.getLogger(__name__)


@dag(
    dag_id="cta_etl_pipeline",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["cta", "etl", "portfolio"],
)
def cta_etl_pipeline():

    @task
    def extract() -> dict:
        df = pd.read_csv(RAW_CSV_PATH)
        logger.info("Extracted %d rows, %d columns", len(df), len(df.columns))
        logger.info("Columns: %s", list(df.columns))
        logger.info("Sample row: %s", df.iloc[0].to_dict())
        return {"row_count": len(df), "columns": list(df.columns)}

    @task
    def load_to_staging():
        hook = PostgresHook(postgres_conn_id="warehouse_default")

        with open(STAGING_DDL_PATH) as f:
            hook.run(f.read())

        conn = hook.get_conn()
        cur = conn.cursor()
        cur.execute("TRUNCATE TABLE staging.cta_ridership_raw")

        with open(RAW_CSV_PATH, "r") as f:
            cur.copy_expert(
                """
                COPY staging.cta_ridership_raw
                    (station_id, stationname, ride_date, daytype, rides)
                FROM STDIN WITH CSV HEADER
                """,
                f,
            )
        conn.commit()

        cur.execute("SELECT COUNT(*) FROM staging.cta_ridership_raw")
        staged_count = cur.fetchone()[0]
        cur.close()
        conn.close()

        logger.info("Loaded %d rows into staging.cta_ridership_raw", staged_count)
        return {"staged_row_count": staged_count}
    
    @task
    @task
    def transform():
        hook = PostgresHook(postgres_conn_id="warehouse_default")

        with open(TRANSFORM_SQL_PATH) as f:
            hook.run(f.read())

        conn = hook.get_conn()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM analytics.cta_ridership_daily")
        analytics_count = cur.fetchone()[0]
        cur.close()
        conn.close()

        logger.info("analytics.cta_ridership_daily now has %d rows", analytics_count)
        return {"analytics_row_count": analytics_count}

    @task
    def quality_check():
        hook = PostgresHook(postgres_conn_id="warehouse_default")
        conn = hook.get_conn()
        cur = conn.cursor()

        failures = []

        # 1. Row count sanity: analytics should never have more rows than staging
        cur.execute("SELECT COUNT(*) FROM staging.cta_ridership_raw")
        staging_count = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM analytics.cta_ridership_daily")
        analytics_count = cur.fetchone()[0]
        if analytics_count == 0:
            failures.append("analytics.cta_ridership_daily is empty")
        if analytics_count > staging_count:
            failures.append(
                f"analytics row count ({analytics_count}) exceeds staging ({staging_count})"
            )

        # 2. No nulls in key columns
        cur.execute("""
            SELECT COUNT(*) FROM analytics.cta_ridership_daily
            WHERE station_id IS NULL OR ride_date IS NULL OR rides IS NULL
        """)
        null_count = cur.fetchone()[0]
        if null_count > 0:
            failures.append(f"{null_count} rows have nulls in key columns")

        # 3. No negative ride counts
        cur.execute("SELECT COUNT(*) FROM analytics.cta_ridership_daily WHERE rides < 0")
        negative_count = cur.fetchone()[0]
        if negative_count > 0:
            failures.append(f"{negative_count} rows have negative ride counts")

        # 4. daytype should only ever be A, U, or W
        cur.execute("""
            SELECT COUNT(*) FROM analytics.cta_ridership_daily
            WHERE daytype NOT IN ('A', 'U', 'W')
        """)
        bad_daytype_count = cur.fetchone()[0]
        if bad_daytype_count > 0:
            failures.append(f"{bad_daytype_count} rows have an unexpected daytype value")

        cur.close()
        conn.close()

        if failures:
            raise ValueError("Data quality checks failed: " + "; ".join(failures))

        logger.info("All data quality checks passed. analytics row count: %d", analytics_count)
        return {"analytics_row_count": analytics_count, "checks_passed": True}

    extract() >> load_to_staging() >> transform() >> quality_check()


cta_etl_pipeline()