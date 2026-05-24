from airflow import DAG
from datetime import datetime, timedelta

from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.email import EmailOperator


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 5, 16),
    'email': ['anaskhan481500@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2)
}

# S3 Details
s3_bucket = 'airflow-snow-um'
s3_key = 'city-folder/customer_detail.csv'

with DAG(
    dag_id='snowflake_s3_with_email_notification_etl',
    default_args=default_args,
    schedule='@daily',
    catchup=False
) as dag:

    # Check file in S3
    is_file_in_s3_available = S3KeySensor(
    task_id='tsk_is_file_in_s3_available',
    bucket_name='airflow-snow-um',
    bucket_key='city-folder/customer_detail.csv',
    aws_conn_id='aws_s3_conn',
    poke_interval=30,
    timeout=300
)

create_table = SQLExecuteQueryOperator(
    task_id="create_snowflake_table",
    conn_id="conn_id_snowflake",
    sql="""
    CREATE TABLE IF NOT EXISTS customer_info (
        city STRING,
        state STRING,
        census_2020 NUMBER,
        land_area_sq_mile_2020 NUMBER
    );
    """
)


copy_csv_into_snowflake_table = SQLExecuteQueryOperator(
    task_id="tsk_copy_csv_into_snowflake_table",
    conn_id="conn_id_snowflake",
    sql="""
    COPY INTO customer_info
    FROM @snowflake_ext_stage_yml
    FILE_FORMAT = (FORMAT_NAME = 'csv_format')
    ON_ERROR = 'CONTINUE';
    """
)


is_file_in_s3_available >> create_table >> copy_csv_into_snowflake_table    

    