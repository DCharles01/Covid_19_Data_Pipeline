import datetime
import boto3
import snowflake.connector
from io import BytesIO
import os
import configparser
import logging
# import pytest
# import sys
# sys.path.append(os.path.dirname(os.getcwd()))
import sys

sys.path.append('..')
from pipeline_utils import config_file_load, log_formatter
from pipeline_utils.log_exception import log_exceptions

log_formatter.setup_logging()


class SnowflakeLoader:
    def __init__(self):
        # config file import
        # self.config_path = config_path
        self.config = config_file_load.import_config_file()

        # s3 bucket name
        self.bucket_name = 'covid-19-data-david'
        self.folder_name = 'Covid19-Data/'

        # s3 file name
        self.file_name = f'covid_19_data_{datetime.datetime.now().strftime("%Y%m%d")}.csv'
        self.file_name_path = self.folder_name + self.file_name
        

        # get access and secret key
        self.ACCESS_KEY = self.config.get('aws_access_secret_key', 'ACCESS_KEY')
        self.SECRET_KEY = self.config.get('aws_access_secret_key', 'SECRET_KEY')

        # get snowflake credentials
        self.snowflake_account = self.config.get('snowflake_credentials', 'SNOWFLAKE_ACCOUNT')
        self.snowflake_user = self.config.get('snowflake_credentials', 'SNOWFLAKE_USERNAME')
        self.snowflake_password = self.config.get('snowflake_credentials', 'SNOWFLAKE_PASSWORD')
        self.snowflake_database = self.config.get('snowflake_credentials', 'SNOWFLAKE_DATABASE')
        self.snowflake_warehouse = self.config.get('snowflake_credentials', 'SNOWFLAKE_WAREHOUSE')
        self.snowflake_schema = self.config.get('snowflake_credentials', 'SNOWFLAKE_SCHEMA')
        self.snowflake_table = self.config.get('snowflake_credentials', 'SNOWFLAKE_TABLE')

        
    @log_exceptions
    def create_session(self):
        session = boto3.Session(
            aws_access_key_id=self.ACCESS_KEY,
            aws_secret_access_key=self.SECRET_KEY
        )
        s3 = session.client('s3')

        return s3
    
    @log_exceptions
    def connect_to_snowflake(self):
        

        conn = snowflake.connector.connect(
            account=self.snowflake_account,
            user=self.snowflake_user,
            password=self.snowflake_password,
            database=self.snowflake_database,
            warehouse=self.snowflake_warehouse,
            schema=self.snowflake_schema,

        )
        return conn
    
    @log_exceptions
    def load_to_snowflake_dw(self, s3, conn):
        # s3 = create_session() # create session
        # conn = connect_to_snowflake() # connect to snowflake

        try:
            file_object = s3.get_object(Bucket=self.bucket_name, Key=self.file_name_path)
            file_content = file_object['Body'].read()
            stage = 'MY_S3_STAGE'
            s3_file_path = f's3://{self.bucket_name}/{self.file_name_path}'
            # TODO: load data in staging, landing zone, and processed zone in s3 -> not needed for ELT pipeline, only ELT
            snowflake_query = f"""
            COPY INTO {self.snowflake_table} (DATE, NEIGHBORHOOD_NAME, BOROUGH, MODZCTA, ZIPCODE_LABEL, AGE_GROUP, POP_DENOMINATOR, COUNT_PARTIALLY_CUMULATIVE, COUNT_FULLY_CUMULATIVE, COUNT_1PLUS_CUMULATIVE, COUNT_ADDITIONAL_CUMULATIVE, COUNT_BIVALENT_ADDITIONAL_CUMULATIVE, PERC_PARTIALLY, PERC_FULLY, PERC_1PLUS, PERC_ADDITIONAL, PERC_BIVALENT_ADDITIONAL) FROM 
            (select $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17 from @{stage}) FILES=('{self.file_name}')
            """
            with conn.cursor() as cursor:
                cursor.execute(snowflake_query)
            logging.info(f"{self.file_name} uploaded successfully")
        except Exception as e:
            logging.critical(f"{self.file_name} not uploaded: {str(e)}")


snowflake_data_loader = SnowflakeLoader()
s3 = snowflake_data_loader.create_session()
conn = snowflake_data_loader.connect_to_snowflake()
snowflake_data_loader.load_to_snowflake_dw(s3, conn)



# # Unit tests using pytest

# @pytest.fixture
# def snowflake_loader():
#     config_file_path = os.path.join(os.path.dirname(os.path.dirname('/Users/pythagoras/Data_Engineering_Projects/Covid_19_Data_Pipeline/src/load_snowflake.py')), 'config/config.ini')
#     return SnowflakeLoader(config_file_path)


# def test_load_data_success(snowflake_loader):
#     snowflake_loader.load_data()
#     # TODO: Add assertions to validate the successful data load


# def test_load_data_failure(snowflake_loader):
#     # Modify the config file to use invalid credentials or file path
#     snowflake_loader.load_data()
#     # TODO: Add assertions to validate the failure scenario
