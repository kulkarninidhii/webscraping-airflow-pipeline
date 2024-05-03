from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from datetime import datetime, timedelta
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
import boto3
# from src.pydantic.url_model import validate_and_store
import sys
import os
# Calculate the absolute path to the src directory
# Add the src directory to sys.path

from src.pydantic_project.url_model import validate_and_store
from src.pydantic_project.ExtractionValidation import validate_and_save_csv
from src.PdfExtraction.Extraction import process_all_pdfs

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def branch_function(**kwargs):
    """Decides the starting task based on the trigger source."""
    trigger_source = kwargs.get('dag_run').conf.get('trigger_source', '')
    print(trigger_source)
    if trigger_source == 'button1':
        return 'run_scrapy_playwright'
    if trigger_source == 'button2':
        return 'extract_pdf_task'

    
# def branch_function(**kwargs):
#     """Decides whether to run the webscraping task based on the trigger source."""
#     trigger_source = kwargs.get('dag_run').conf.get('trigger_source', '')
#     if trigger_source == 'streamlit':
#         return 'run_scrapy_playwright'
#     else:
#         return 'skip_webscraping'

def upload_file_to_s3(bucket_name, s3_key, file_path):
    hook = S3Hook(aws_conn_id='aws_default')
    hook.load_file(filename=file_path, bucket_name=bucket_name, replace=True, key=s3_key) 

# def download_pdf_from_s3(bucket_name, s3_key, local_path):
#     hook = S3Hook(aws_conn_id='aws_default')
#     hook.download_file(key=s3_key, bucket_name=bucket_name, local_path=local_path)

def download_pdf_from_s3(bucket_name, s3_key, local_directory):
    """
    Download a PDF file from an S3 bucket to a specified local directory using boto3.

    :param bucket_name: Name of the S3 bucket.
    :param s3_key: S3 key of the PDF file to download.
    :param local_directory: Local directory to save the downloaded PDF.
    """
    # Ensure the local directory exists
    if not os.path.exists(local_directory):
        os.makedirs(local_directory)
    # Construct the local file path
    local_file_path = os.path.join(local_directory, os.path.basename(s3_key))

    s3 = boto3.client('s3', aws_access_key_id='access_key', aws_secret_access_key='secret_access_key')
    s3.download_file(bucket_name, s3_key, local_file_path)
    print(f"Downloaded {s3_key} to {local_file_path}")


def execute_pdf_tasks():
    # Assuming download_pdfs_from_s3 and process_all_pdfs are defined elsewhere in your DAG file
    download_pdf_from_s3('docpool','2024-l1-topics-combined-2.pdf','/opt/airflow/src/dataset/')
    download_pdf_from_s3('docpool','2024-l2-topics-combined-2.pdf','/opt/airflow/src/dataset/')
    download_pdf_from_s3('docpool','2024-l3-topics-combined-2.pdf','/opt/airflow/src/dataset/')
    process_all_pdfs('/opt/airflow/src/dataset/*.pdf')

def validation_tasks():
    # Assuming download_pdfs_from_s3 and process_all_pdfs are defined elsewhere in your DAG file
    validate_and_save_csv('/opt/airflow/src/dataset/final_output.csv','/opt/airflow/src/dataset/Cleanedfinal_output.csv' )
    validate_and_store('/opt/airflow/src/dataset/CFA.json','/opt/airflow/src/dataset/validated_CFA.csv')

def uploads3():
    upload_file_to_s3('validateddocpool','CFA.csv','/opt/airflow/src/dataset/validated_CFA.csv')
    upload_file_to_s3('validateddocpool','PDF.csv','/opt/airflow/src/dataset/Cleanedfinal_output.csv')
      


sql_file_path = '/opt/airflow/src/DBT-Snowflake.sql'

with open(sql_file_path, 'r') as file:
    sql_commands = file.read()


with DAG(
    'consolidated_dag',
    default_args=default_args,
    description='A consolidated dag for ETL',
    schedule=None,
    start_date=datetime(2024, 3, 25),
    catchup=False,
) as dag:

    decide_to_scrape = BranchPythonOperator(
        task_id='decide_to_scrape',
        python_callable=branch_function,
    )

    run_scrapy_playwright = BashOperator(
        task_id='run_scrapy_playwright',
        bash_command='cd /opt/airflow/src/scrapy/Pwspider/spiders && scrapy crawl pwspidey -o /opt/airflow/src/dataset/CFA.json',
    )
    # skip_webscraping = EmptyOperator(
    #     task_id='skip_webscraping',
    # )

    extract_pdf_task = PythonOperator(
        task_id='extract_pdf_task',
        python_callable=execute_pdf_tasks,
                # Add any necessary op_kwargs here
    )

    validate_and_store_task = PythonOperator(
        task_id='validate_and_store',
        python_callable=validation_tasks,         
    )

    join_task= EmptyOperator(
        task_id='join_task',
        trigger_rule=TriggerRule.ONE_SUCCESS,
    )

    upload_to_s3 = PythonOperator(
        task_id='upload_to_s3',
        python_callable=uploads3,
    )

    snowflake_upload = SnowflakeOperator(
    task_id='execute_sql_in_snowflake',
    snowflake_conn_id='snowflake_default',
    sql=sql_commands,
    dag=dag,
    )

    # run_dbt_models = BashOperator(
    # task_id='run_dbt_models',
    # bash_command='dbt run --profiles-dir /opt/airflow/src/dbt_proj/ --project-dir /opt/airflow/src/dbt_proj/',
    # )
    




    decide_to_scrape >> [run_scrapy_playwright, extract_pdf_task] >> join_task >> validate_and_store_task >> upload_to_s3 >> snowflake_upload 
