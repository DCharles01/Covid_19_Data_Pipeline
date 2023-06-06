import logging
import datetime


def setup_logging():
    log_format = '%(asctime)s - %(levelname)s - %(message)s'
    logging.basicConfig(format=log_format, datefmt='%m/%d/%Y %I:%M:%S %p', handlers=[
        logging.FileHandler(f'/opt/airflow/logs/data_pipeline_logs/log_{datetime.datetime.now().strftime("%Y%m%d")}.log'),
        logging.StreamHandler()], encoding='utf-8', level=logging.INFO)