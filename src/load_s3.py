
import pandas as pd
import boto3
from io import StringIO
import configparser
import datetime
import os
import logging
# import sys
# sys.path.append(os.path.dirname(os.getcwd()))
import sys

sys.path.append('..')

from src.extract_github import DataExtractor
from pipeline_utils import config_file_load, log_formatter
from pipeline_utils.log_exception import log_exceptions

log_formatter.setup_logging()


class S3Loader:
    def __init__(self):
        # self.config_path = config_path
        self.config = config_file_load.import_config_file()
        # self.config.read(self.config_path)

        self.ACCESS_KEY = self.config.get('aws_access_secret_key', 'ACCESS_KEY')
        self.SECRET_KEY = self.config.get('aws_access_secret_key', 'SECRET_KEY')

        self.bucket_name = 'covid-19-data-david'
        self.folder_name = 'Covid19-Data/'

    @log_exceptions
    def load_data(self):
        logging.debug("Connecting to S3")
        session = boto3.Session(
            aws_access_key_id=self.ACCESS_KEY,
            aws_secret_access_key=self.SECRET_KEY
        )
        s3 = session.client('s3')

        file_name = f'covid_19_data_{datetime.datetime.now().strftime("%Y%m%d")}.csv'
        file_name_path = self.folder_name + file_name

        covid_data = DataExtractor.extract_data()

        logging.info("Data Successfully loaded into S3")
        s3.put_object(Body=covid_data, Bucket=self.bucket_name, Key=file_name_path)


# Usage example
# config_file_path = os.path.join(os.path.dirname(os.getcwd()), 'config/config.ini')
s3_loader = S3Loader()
s3_loader.load_data()