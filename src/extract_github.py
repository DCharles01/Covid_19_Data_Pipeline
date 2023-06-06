from github import Github
from io import StringIO
import configparser
import datetime
import pandas as pd
import os
# import sys
# sys.path.append(os.path.dirname(os.getcwd()))
import logging
from pipeline_utils import config_file_load, log_formatter
from pipeline_utils.log_exception import log_exceptions



# setup logging
log_formatter.setup_logging()

# log_format = '%(asctime)s - %(levelname)s - %(message)s'
# logging.basicConfig(format=log_format, datefmt='%m/%d/%Y %I:%M:%S %p', handlers=[
#                     logging.FileHandler(f'log_{datetime.datetime.now().strftime("%Y%m%d")}.log'),
#                     logging.StreamHandler()], encoding='utf-8', level=logging.INFO)

# logging.info("Loading config file")
# try:
#     # Read the configuration file
#     config = configparser.ConfigParser()
#     config.read(os.path.join(os.path.dirname(os.getcwd()), 'config/config.ini'))
#     logging.info("Config file successfully loaded.")
# except:
#     logging.CRITICAL("Config file not found. Check filepath.")


# def log_exceptions(func):
#     def wrapper(*args, **kwargs):
#         try:
#             return func(*args, **kwargs)
#         except Exception as e:
#             logging.critical(f"Exception occurred in {func.__name__}: {str(e)}")
#     return wrapper




class DataExtractor:
    @classmethod
    @log_exceptions
    def extract_data(cls):
        # Retrieve github token
        # TODO: renew token in June and replace
        logging.info("Retrieving GitHub token...")
        # load config file
        logging.info("Loading Config file")
        config = config_file_load.import_config_file()
        github_token = config.get('github_token', 'GITHUB_TOKEN')

        g = Github(github_token)
        logging.info("Retrieving GitHub token successful...")

        # Specify the repository and file
        repo_name = 'nychealth/covid-vaccine-data'
        file_path = 'people/coverage-by-modzcta-allages.csv'

        try:
            logging.info("Retrieving GitHub files...")
            # Retrieve the file content
            repo = g.get_repo(repo_name)
            file_content = repo.get_contents(file_path).decoded_content.decode()
            logging.info("Github file retrieval successful.")
        except:
            logging.critical(f"Github Repository: www.github.com/{repo_name}/{file_path} does not exist or {file_path.split(sep='/')[1]} does not exist in {repo_name}.")

        extracted_data = pd.read_csv(StringIO(file_content))
        extracted_data_string = extracted_data.to_csv(index=False)

        logging.info(f"{file_path.split(sep='/')[1]} successfully extracted.")
        return extracted_data_string


DataExtractor.extract_data()
