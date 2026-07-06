\# Airflow Composer ETL Project



This project contains an end-to-end Airflow ETL pipeline deployed on Google Cloud Composer.



\## Pipeline Flow



GCS inbound file -> Validate CSV -> Load to BigQuery -> Move to archive -> Audit logging



\## GCP Services Used



\- Cloud Composer / Managed Airflow

\- Cloud Storage

\- BigQuery

\- IAM



\## DAG



\- gcs\_to\_big\_daily

