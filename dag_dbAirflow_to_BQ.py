from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator

GCS_BUCKET = 'linkaja_datalake_development'
GCS_PREFIX = 'hilmy/staging'
POSTGRES_CONNENCTION_ID = 'my_postgres_conn'
GCP_CONNECTION_ID='my_gcp_conn'
PG_SCHEMA='public'
SOURCE_TABLE_NAME = 'dag_run'

BQ_DATASET_ID='linkaja-data-engineering-dev.de_sandbox'
BQ_SCHEMA_ab_user = [
    {"name": "id", "type": "INT64", "mode": "REQUIRED"},
    {"name": "first_name", "type": "STRING", "mode": "NULLABLE"},
    {"name": "last_name", "type": "STRING", "mode": "NULLABLE"},
    {"name": "username", "type": "STRING", "mode": "NULLABLE"},
    {"name": "password", "type": "STRING", "mode": "NULLABLE"},
]
BQ_SCHEMA_dag_run = [
    {"name": "id", "type": "INT64", "mode": "REQUIRED"},
    {"name": "dag_id", "type": "STRING", "mode": "NULLABLE"},
    {"name": "queued_at", "type": "TIMESTAMP", "mode": "NULLABLE"},
    {"name": "execution_date", "type": "TIMESTAMP", "mode": "NULLABLE"},
    {"name": "start_date", "type": "TIMESTAMP", "mode": "NULLABLE"},
    {"name": "end_date", "type": "TIMESTAMP", "mode": "NULLABLE"},
    {"name": "state", "type": "STRING", "mode": "NULLABLE"},
    {"name": "run_id", "type": "STRING", "mode": "NULLABLE"},
    {"name": "creating_job_id", "type": "STRING", "mode": "NULLABLE"},
    {"name": "external_trigger", "type": "BOOL", "mode": "NULLABLE"},
    {"name": "run_type", "type": "STRING", "mode": "NULLABLE"},
    {"name": "conf", "type": "STRING", "mode": "NULLABLE"},
    {"name": "data_interval_start", "type": "TIMESTAMP", "mode": "NULLABLE"},
    {"name": "data_interval_end", "type": "TIMESTAMP", "mode": "NULLABLE"},
    {"name": "last_scheduling_decision", "type": "TIMESTAMP", "mode": "NULLABLE"},
    {"name": "dag_hash", "type": "STRING", "mode": "NULLABLE"},
    {"name": "log_template_id", "type": "INT64", "mode": "NULLABLE"},
    {"name": "updated_at", "type": "TIMESTAMP", "mode": "NULLABLE"},
    {"name": "clear_number", "type": "INT64", "mode": "NULLABLE"},
]

default_args = {
    'owner': 'Hilmy',
    'retries': 1, 
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='load_postgres_to_bq',
    start_date=datetime(2025, 8, 27),
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
) as dag:
    postgre_to_gcs_task = PostgresToGCSOperator(
        task_id='postgre_to_gcs',
        postgres_conn_id=POSTGRES_CONNENCTION_ID,
        gcp_conn_id=GCP_CONNECTION_ID,
        sql=f'''
            SELECT * 
            FROM {PG_SCHEMA}.{SOURCE_TABLE_NAME}
            WHERE DATE(execution_date) = '{{{{ ds }}}}' ''',
        bucket=GCS_BUCKET,
        filename=f'{GCS_PREFIX}/{SOURCE_TABLE_NAME}_{{{{ ts_nodash }}}}.csv',
        export_format='CSV',
        gzip=False
    )

    gcs_to_bigquery_task = GCSToBigQueryOperator(
        task_id='gcs_to_bigquery',
        bucket=GCS_BUCKET,
        source_objects=[f'{GCS_PREFIX}/{SOURCE_TABLE_NAME}_{{{{ ts_nodash }}}}.csv'],
        source_format='CSV',
        destination_project_dataset_table=f'{BQ_DATASET_ID}.{SOURCE_TABLE_NAME}',
        schema_fields=BQ_SCHEMA_dag_run,
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE',
        skip_leading_rows=1,
        gcp_conn_id=GCP_CONNECTION_ID,
    )

    postgre_to_gcs_task >> gcs_to_bigquery_task
