from datetime import datetime
from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator

with DAG(
    dag_id="spotify_pipeline",
    description="Extract -> Transform -> Load pipeline for Spotify tracks dataset",
    start_date=datetime(2026, 1, 1),
    schedule=None,        # run manually for now; we can add a schedule later
    catchup=False,
    tags=["spotify", "pipeline"],
) as dag:

    extract = BashOperator(
        task_id="extract_from_kaggle_to_raw",
        bash_command="cd /opt/airflow/scripts && MINIO_ENDPOINT=http://host.docker.internal:9000 python extract_lambda.py",
    )

    transform = BashOperator(
        task_id="transform_raw_to_cleaned",
        bash_command="cd /opt/airflow/scripts && MINIO_ENDPOINT=http://host.docker.internal:9000 python transform_glue.py",
    )

    load = BashOperator(
        task_id="load_cleaned_to_snowflake",
        bash_command="cd /opt/airflow/scripts && MINIO_ENDPOINT=http://host.docker.internal:9000 python load_snowflake.py",
    )

    extract >> transform >> load