import datetime

import fix_53617  # noqa: F401
from revd.sdk import dag, task, task_group, Asset
import revd.util
from revd.operators import ObjectCopyOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

# If this is a manual upload, the workflow might need a Sensor instead of a download
ENISA_KEV_URL = "https://raw.githubusercontent.com/enisaeu/CNW/refs/heads/main/kev.csv"
ENISA_KEV_STORE = "{{ params.incoming }}/enisa_kev/dbdate={{ ds_nodash }}/enisa_kev.csv"


@task_group
def enisa_kev():
    download = ObjectCopyOperator(
        task_id="download",
        src=ENISA_KEV_URL,
        dst=((ENISA_KEV_STORE,), dict(conn_id="aws_default")),
        replace="skip",
        retries=2,
        retry_delay=datetime.timedelta(minutes=5))
    build = SQLExecuteQueryOperator(
        task_id="build",
        conn_id="{{ params.sql_conn }}",
        split_statements=True,
        sql="sql/enisa_kev.sql")
    add_part = SQLExecuteQueryOperator(
        task_id="add_part",
        conn_id="{{ params.sql_conn }}",
        sql=("ALTER TABLE enisa_kev_archive ADD IF NOT EXISTS"
             " PARTITION (dbdate = '{{ ds_nodash }}')"))
    ds = Asset("enisa_kev")

    @task(outlets=[ds])
    def publish(where, params):
        revd.util.build_latest_view(
            view_name="enisa_kev",
            archive_name="enisa_kev_archive",
            where=where,
            database=params["database"])
        print(f"Publish enisa_kev with {where}")
        yield ds

    [download, build] >> add_part >> publish("dbdate='{{ ds_nodash }}'")


@dag(
    schedule="@daily",
    max_active_runs=1,
    max_active_tasks=1,
    start_date=datetime.datetime(2025, 1, 1),
)
def enisa_kev_ingestion():
    """
    ### ENISA Known Exploited Vulnerabilities (KEV) Ingestion

    This DAG:
    1. Downloads the ENISA KEV CSV (URL needs configuration)
    2. Uploads to S3 partitioned by date
    3. Creates Athena tables for vulnerability analysis
    4. Publishes dataset for downstream security analytics
    """
    enisa_kev()


di = enisa_kev_ingestion()

if __name__ == "__main__":
    di.test()
