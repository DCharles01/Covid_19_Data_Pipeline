from .log_formatter import setup_logging
import logging

# setup logging
setup_logging()

def log_exceptions(func):
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logging.critical(f"Exception occurred in {func.__name__}: {str(e)}")
    return wrapper