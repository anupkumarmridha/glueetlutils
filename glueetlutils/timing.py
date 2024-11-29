from functools import wraps
import time
from glueetlutils.logger import logger  # Use the global logger

def log_time(func):
    """
    Decorator to log the execution time of a function.

    :param func: The function whose execution time is to be measured.
    :return: The wrapped function.
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        logger.info(f"Starting function: {func.__name__}")
        start_time = time.time()
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logger.exception(f"Error in function {func.__name__}: {e}")
            raise
        finally:
            elapsed_time = time.time() - start_time
            logger.info(f"Function {func.__name__} completed in {elapsed_time:.2f} seconds.")
    return wrapper
