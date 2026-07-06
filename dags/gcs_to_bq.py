# Deployed using GitHub Actions CI/CD

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator

from validation import validate_csv
from audit import log_audit_success


with DAG(
    dag_id="gcs_to_big_daily",
    start_date=datetime(2026, 1, 1),
    schedule="45 20 * * *",
    catchup=False,
    tags=["gcp", "gcs", "bigquery"],
) as dag:

    wait_for_file = GCSObjectExistenceSensor(
        task_id="wait_for_file",
        bucket="airflow-gcp-learning-rohith",
        object="inbound/rohith.csv",
        poke_interval=300,
        timeout=3000,
        mode="reschedule",
    )

    validate = PythonOperator(
        task_id="validate_file",
        python_callable=validate_csv,
        op_kwargs={
            "bucket_name": "airflow-gcp-learning-rohith",
            "object_name": "inbound/rohith.csv",
        },
    )

    load_to_bq = GCSToBigQueryOperator(
        task_id="load_to_bigquery",
        bucket="airflow-gcp-learning-rohith",
        source_objects=["inbound/rohith.csv"],
        destination_project_dataset_table="studied-limiter-501113-k0.employee.employee_details",
        source_format="CSV",
        skip_leading_rows=1,
        write_disposition="WRITE_APPEND",
        autodetect=True,
    )

    move_to_archive = GCSToGCSOperator(
        task_id="move_to_archive",
        source_bucket="airflow-gcp-learning-rohith",
        source_object="inbound/rohith.csv",
        destination_bucket="airflow-gcp-learning-rohith",
        destination_object="archive/rohith.csv",
        move_object=True,
    )

    log_audit = PythonOperator(
        task_id="log_audit_success",
        python_callable=log_audit_success,
        op_kwargs={
            "project_id": "studied-limiter-501113-k0",
            "dataset_id": "employee",
            "table_id": "etl_audit",
            "file_name": "rohith.csv",
            "validation_task_id": "validate_file",
        },
    )

    wait_for_file >> validate >> load_to_bq >> move_to_archive >> log_audit