import time
import json
import boto3
from botocore.exceptions import ClientError

# Logger setup (ensure you have a logger configured)
import logging
logger = logging.getLogger(__name__)

class CommonUtils:
    @staticmethod
    def log_time(func):
        """Decorator to measure and log the execution time of functions."""
        def wrapper(*args, **kwargs):
            logger.info(f"Starting function: {func.__name__}")
            start_time = time.time()
            try:
                result = func(*args, **kwargs)
                return result
            except Exception as e:
                logger.error(f"An error occurred in function {func.__name__}: {e}")
                raise e  # Re-raise to propagate the exception
            finally:
                elapsed_time = time.time() - start_time
                logger.info(f"Function {func.__name__} completed in {elapsed_time:.2f} seconds.")
        return wrapper

    @staticmethod
    @log_time
    def get_secret(secret_name, region_name):
        """
        Retrieve secrets from AWS Secrets Manager.

        :param secret_name: The name of the secret in Secrets Manager.
        :param region_name: The AWS region of the Secrets Manager.
        :return: The secret as a dictionary.
        """
        logger.info("Loading secrets from Secrets Manager...")
        session = boto3.session.Session()
        client = session.client(service_name='secretsmanager', region_name=region_name)

        try:
            get_secret_value_response = client.get_secret_value(SecretId=secret_name)
            secret = get_secret_value_response.get('SecretString')
            return json.loads(secret)

        except ClientError as e:
            logger.error(f"An error occurred while loading secrets: {e}")
            raise e
