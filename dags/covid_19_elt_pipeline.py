from airflow import DAG
from airflow.operators.python import PythonOperator
import datetime 
import logging
import sys

import os

sys.path.append('..')
# sys.path.insert(0, '..') # import parent directory
from src import extract_github, load_s3, load_snowflake
from pipeline_utils import log_formatter, config_file_load
from pipeline_utils.log_exception import log_exceptions

# setup logging file and format
log_formatter.setup_logging()

logging.debug("Initiating DAG Args")
default_args = {
    'start_date': datetime.datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5)
}

logging.debug("Starting airflow pipeline.")

s3_loader = load_s3.S3Loader()


snowflake_data_loader = load_snowflake.SnowflakeLoader()
s3 = snowflake_data_loader.create_session()
conn = snowflake_data_loader.connect_to_snowflake()



# Define the DAG
with DAG(
    'covid_19_elt_pipeline',
    default_args=default_args,
    schedule_interval=None
) as dag:
    
    # Task 1: Run script1.py
    task1 = PythonOperator(
        task_id='extract_from_github',
        python_callable=extract_github.DataExtractor.extract_data
    )

    # Task 2: Run script2.py
    task2 = PythonOperator(
        task_id='load_data_into_s3_bucket',
        python_callable=s3_loader.load_data
    )

    # Task 3: Run script3.py
    task3 = PythonOperator(
        task_id='push_data_from_s3_to_snowflake',
        python_callable=snowflake_data_loader.load_to_snowflake_dw,  
        op_kwargs={'s3': s3, 'conn': conn}  # pass function arguments here
    )


    # Define the task dependencies
    task1 >> task2 >> task3

logging.debug("Finished airflow pipeline")
