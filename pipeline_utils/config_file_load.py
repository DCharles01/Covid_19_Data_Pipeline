import os
import logging
from .log_formatter import setup_logging
import configparser

# setup logging
setup_logging()


def import_config_file():
    logging.info("Loading config file")
    try:
        # Read the configuration file
        config = configparser.ConfigParser()
        # config.read(os.path.join(os.path.dirname(os.getcwd()), 'config/config.ini'))
        config.read(os.path.join(os.getcwd(), 'config/config.ini'))
        logging.info("Config file successfully loaded.")
    except:
        logging.CRITICAL("Config file not found. Check filepath.")

    return config