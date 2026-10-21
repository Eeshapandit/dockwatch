"""Step 11: orchestrate the monthly batch with Airflow.

    extract_month  ->  load_month  ->  dbt_build  ->  export_page

Why an orchestrator instead of a cron job running trips.py?
  * dependencies: dbt only runs if the load succeeded
  * retries: a flaky download is retried automatically, with a delay
  * history: every run, its logs and its duration are in the UI
  * backfills: re-run any past month with one command, safely
    (every step is idempotent, so reruns never double count)

Schedule: the 15th of each month at 06:00 UTC, processing the PREVIOUS
month (Citi Bike publishes a month's file in the first days of the next).
Trigger manually with {"month": "202509"} or {"month": "latest"}.
"""
from datetime import timedelta

import pendulum
from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.operators.bash import BashOperator

DBT = "/opt/dbt-venv/bin/dbt"
REPO = "/opt/dockwatch"


@dag(
    dag_id="dockwatch_trips_monthly",
    schedule="0 6 15 * *",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,          # don't auto run every month since 2025 on first start
    max_active_runs=1,      # two months loading at once would fight over disk and DB
    default_args={"retries": 2, "retry_delay": timedelta(minutes=10)},
    params={"month": Param("", type="string", description="YYYYMM or 'latest'. Empty = previous month.")},
    tags=["dockwatch", "batch"],
)
def dockwatch_trips_monthly():

    @task
    def extract_month(**context):
        from trips import extract_month as extract

        month = context["params"].get("month") or context["data_interval_start"].strftime("%Y%m")
        resolved, path = extract(month)
        return {"month": resolved, "path": str(path)}

    @task
    def load_month(info: dict):
        from trips import load_month as load

        return load(info["path"], info["month"])

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=f"cd {REPO}/dbt && {DBT} build --profiles-dir . --target-path /tmp/dbt-target --log-path /tmp/dbt-logs",
    )

    @task
    def export_page():
        import export_page_data

        export_page_data.main()

    load_month(extract_month()) >> dbt_build >> export_page()


dockwatch_trips_monthly()
