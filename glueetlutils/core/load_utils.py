from functools import wraps
import time
import random
from botocore.exceptions import ClientError
from glueetlutils.logger import logger
from glueetlutils.timing import log_time


class LoadUtils:
    def __init__(self, glue_context):
        self.glue_context = glue_context

    @log_time
    def load_data_from_s3(self, s3_path, transformation_ctx):
        """
        Load data from an S3 location into an AWS Glue DynamicFrame.

        This function uses the AWS Glue context to load data from a specified S3 path.
        It supports reading CSV files and optionally uses Glue job bookmarks to manage incremental loads.

        Args:
            s3_path (str): The S3 path to the folder or file containing the data. Example: "s3://bucket-name/folder-name/".
            transformation_ctx (str): A unique string to identify the transformation context for AWS Glue operations.

        Returns:
            DynamicFrame: An AWS Glue DynamicFrame containing the loaded data.
            None: If the data is empty (0 rows).

        Raises:
            Exception: For S3 connection issues, data loading errors, or Glue internal errors.
        """
        try:
            logger.info(f"Loading data from S3 path: {s3_path}...")
            
            input_data = self.glue_context.create_dynamic_frame.from_options(
                connection_type="s3",
                connection_options={
                    "paths": [s3_path],
                    "recurse": True,
                    "jobBookmarkKeys": ["filename"],
                    "jobBookmarkKeysSortOrder": "asc",
                },
                transformation_ctx=transformation_ctx,
                format="csv",
                format_options={
                    "withHeader": True,
                    "separator": ",",
                    "quoteChar": '"',
                },
            )

            record_count = input_data.count()
            logger.info(f"------S3 INPUT DATA SIZE: {record_count} rows------")
            if record_count == 0:
                logger.warning("The input data is empty.")
                return None

            logger.info("Data loaded and mapped successfully.")
            return input_data

        except Exception as e:
            logger.exception(f"Error loading data from S3: {e}")
            raise

    @staticmethod
    def _retry_with_backoff(max_attempts=3, initial_delay=1, max_delay=10, exponential_base=2):
        """
        A decorator that implements exponential backoff retry logic.

        Args:
            max_attempts (int): Maximum number of retry attempts.
            initial_delay (int): Initial delay between retries in seconds.
            max_delay (int): Maximum delay between retries in seconds.
            exponential_base (int): Base for exponential backoff calculation.

        Returns:
            function: Decorated function with retry logic.
        """
        def decorator(func):
            @wraps(func)
            def wrapper(*args, **kwargs):
                last_exception = None

                for attempt in range(max_attempts):
                    try:
                        return func(*args, **kwargs)
                    except Exception as e:
                        last_exception = e
                        if not LoadUtils.is_retryable_error(e):
                            logger.error(f"Non-retryable error occurred: {str(e)}")
                            raise e

                        if attempt == max_attempts - 1:
                            logger.error(f"Max retry attempts ({max_attempts}) reached. Last error: {str(e)}")
                            raise e

                        delay = min(initial_delay * (exponential_base ** attempt) + random.uniform(0, 1), max_delay)
                        logger.warning(
                            f"Attempt {attempt + 1}/{max_attempts} failed: {str(e)}. Retrying in {delay:.2f} seconds..."
                        )
                        time.sleep(delay)

                if last_exception:
                    raise last_exception

            return wrapper
        return decorator

    @staticmethod
    def is_retryable_error(exception):
        """
        Determine if an error is retryable based on its type and message.

        Args:
            exception (Exception): The caught exception.

        Returns:
            bool: True if the error is retryable, False otherwise.
        """
        retryable_messages = [
            "The connection attempt failed",
            "Connection reset by peer",
            "Connection timed out",
            "deadlock detected",
            "operational error",
            "connection is closed",
            "ConnectionError",
            "OperationalError",
            "InternalServiceError",
            "ServiceUnavailable",
            "Too many requests",
            "ThrottlingException",
            "ProvisionedThroughputExceededException",
        ]

        error_message = str(exception).lower()
        is_retryable = any(msg.lower() in error_message for msg in retryable_messages)

        if isinstance(exception, ClientError):
            error_code = exception.response['Error']['Code']
            is_retryable = error_code in [
                "ThrottlingException",
                "ProvisionedThroughputExceededException",
                "RequestLimitExceeded",
                "InternalServiceError",
                "ServiceUnavailable",
            ]

        return is_retryable

    @_retry_with_backoff(max_attempts=3, initial_delay=2, max_delay=15, exponential_base=2)
    @log_time
    def write_data_to_redshift(self, connection_options, dyf, table_name):
        """
        Write an AWS Glue DynamicFrame to a Redshift table.

        Args:
            connection_options (dict): Connection parameters for Redshift.
            dyf (DynamicFrame): The AWS Glue DynamicFrame to be written to Redshift.
            table_name (str): The name of the target Redshift table in the "public" schema.

        Raises:
            ValueError: If required input values (e.g., `dyf` or `table_name`) are invalid.
            Exception: For connection errors, permission issues, or Glue internal errors.
        """
        try:
            logger.info(f"Writing to Redshift table: {table_name}...")
            merged_connection_options = {
                **connection_options,
                "dbtable": f"public.{table_name}",
            }

            self.glue_context.write_dynamic_frame.from_options(
                frame=dyf,
                connection_type="redshift",
                connection_options=merged_connection_options,
                transformation_ctx=f"Redshift_{table_name}",
            )

            logger.info(f"Data successfully written to Redshift table: {table_name}")

        except ValueError as ve:
            logger.error(f"ValueError: {str(ve)}")
            raise

        except Exception as e:
            logger.exception(f"Error writing to Redshift table {table_name}: {str(e)}")
            raise

    @log_time
    def load_data_from_redshift(self, connection_options, table_name):
        """
        Load data from a Redshift table into an AWS Glue DynamicFrame.

        Args:
            connection_options (dict): Connection parameters for Redshift.
            table_name (str): The name of the target Redshift table in the "public" schema.

        Returns:
            DynamicFrame: The data as a DynamicFrame.

        Raises:
            Exception: For connection errors, permission issues, or Glue internal errors.
        """
        try:
            logger.info(f"Loading data from Redshift table: {table_name}...")
            merged_connection_options = {
                **connection_options,
                "dbtable": f"public.{table_name}",
            }

            data_from_redshift = self.glue_context.create_dynamic_frame.from_options(
                connection_type="redshift",
                connection_options=merged_connection_options,
            )

            logger.info(f"Data successfully loaded from Redshift table: {table_name}")
            return data_from_redshift

        except Exception as e:
            logger.exception(f"Error loading data from Redshift table {table_name}: {e}")
            raise
