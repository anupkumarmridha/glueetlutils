from functools import wraps
import time
from botocore.exceptions import ClientError


class LoadUtils:
    def __init__(self, glue_context):
        self.glue_context = glue_context

    def load_data_from_s3(glueContext,s3_path, transformation_ctx):

        """Load data from an S3 location into an AWS Glue DynamicFrame.

        This function uses the AWS Glue context to load data from a specified S3 path.
        It supports reading CSV files and optionally uses Glue job bookmarks to manage incremental loads.

        Args:
            glueContext (GlueContext): The AWS Glue context used to interact with Glue services.
            s3_path (str): The S3 path to the folder or file containing the data. Example: "s3://bucket-name/folder-name/".
            transformation_ctx (str): A unique string to identify the transformation context for AWS Glue operations.

        Returns:
            DynamicFrame: An AWS Glue DynamicFrame containing the loaded data.
                        - If the data is successfully loaded, it returns a DynamicFrame.
                        - If the data is empty (0 rows), it returns `None`.

        Raises:
            Exception: If there is an issue with the S3 connection, data loading, or any internal Glue error,
                    the function raises an Exception with the error details.
        """
        try:
            print("Loading data from S3...")
            
            input_data = glueContext.create_dynamic_frame.from_options(
                connection_type="s3",
                connection_options={
                    "paths": [s3_path],
                    "recurse": True,
                    "jobBookmarkKeys": ["filename"],
                    "jobBookmarkKeysSortOrder": "asc"
                },
                transformation_ctx=transformation_ctx,
                format="csv",
                format_options={
                    "withHeader": True,
                    "separator": ",",
                    "quoteChar": '"'
                }
            )

            print("------S3 INPUT DATA SIZE----------" + str(input_data.count()))
            if input_data.count() == 0:
                print("The input data is empty.")
                return None

            print("Data loaded and mapped successfully.")
            return input_data
        except Exception as e:
            print(f"Error loading data from S3: {e}")
            raise

    def _retry_with_backoff(max_attempts=3, initial_delay=1, max_delay=10, exponential_base=2):
        """
        A decorator that implements exponential backoff retry logic for Redshift operations.
        
        Args:
            max_attempts (int): Maximum number of retry attempts
            initial_delay (int): Initial delay between retries in seconds
            max_delay (int): Maximum delay between retries in seconds
            exponential_base (int): Base for exponential backoff calculation
        
        Returns:
            function: Decorated function with retry logic
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
                        
                        # Check if the error is retryable
                        if not is_retryable_error(e):
                            print(f"Non-retryable error occurred: {str(e)}")
                            raise e
                        
                        # If this was the last attempt, raise the exception
                        if attempt == max_attempts - 1:
                            print(f"Max retry attempts ({max_attempts}) reached. Last error: {str(e)}")
                            raise e
                        
                        # Calculate delay with exponential backoff and jitter
                        delay = min(initial_delay * (exponential_base ** attempt) + random.uniform(0, 1), max_delay)
                        
                        print(
                            f"Attempt {attempt + 1}/{max_attempts} failed: {str(e)}. "
                            f"Retrying in {delay:.2f} seconds..."
                        )
                        
                        time.sleep(delay)
                
                if last_exception:
                    raise last_exception
                    
            return wrapper
        return decorator


    def is_retryable_error(exception):
        """
        Determine if an error is retryable based on its type and message.
        
        Args:
            exception (Exception): The caught exception
        
        Returns:
            bool: True if the error is retryable, False otherwise
        """
        # List of retryable error messages or patterns
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
            "ProvisionedThroughputExceededException"
        ]
        
        error_message = str(exception).lower()
        
        # Check if the error message contains any retryable patterns
        is_retryable = any(msg.lower() in error_message for msg in retryable_messages)
        
        # Check for specific exception types
        if isinstance(exception, ClientError):
            error_code = exception.response['Error']['Code']
            is_retryable = error_code in [
                'ThrottlingException',
                'ProvisionedThroughputExceededException',
                'RequestLimitExceeded',
                'InternalServiceError',
                'ServiceUnavailable'
            ]
        
        return is_retryable


    @_retry_with_backoff(max_attempts=3, initial_delay=2, max_delay=15, exponential_base=2)
    def write_data_to_redshift(glueContext, connection_options, dyf, table_name):
        """Write an AWS Glue DynamicFrame to a Redshift table with Snappy compression.

        This function writes the provided DynamicFrame (`dyf`) to the specified Redshift table. 
        It dynamically updates the connection options to include the target table name and supports Snappy compression.

        Args:
            glueContext (GlueContext): The AWS Glue context used to interact with Glue services.
            connection_options (dict): A dictionary containing connection parameters for Redshift.
                                    Example: {"url": "<jdbc_url>", "user": "username", "password": "password"}
            dyf (DynamicFrame): The AWS Glue DynamicFrame to be written to Redshift.
            table_name (str): The name of the target Redshift table in the "public" schema.

        Returns:
            None: This function writes data to Redshift and does not return a value.

        Raises:
            ValueError: If any required input value (e.g., `dyf` or `table_name`) is `None` or invalid.
            Exception: For general issues, such as connection errors, Redshift permissions, or internal Glue errors.
    """
        try:
            print(f"Writing {table_name} to Redshift...")
            
            
            merged_connection_options = {
                **connection_options,  # Unpack original Redshift options
                "dbtable": f"public.{table_name}"  # Specify the table name dynamically
            
            }
            
            # Write the DynamicFrame with Snappy compression
            glueContext.write_dynamic_frame.from_options(
                frame=dyf,
                connection_type="redshift",
                connection_options=merged_connection_options,
                transformation_ctx=f"Redshift_{table_name}"
            )
            
            print(f"{table_name} written successfully.")
        
        except ValueError as ve:
            # Handle missing input values (e.g., None)
            print(f"ValueError: {str(ve)}")
            raise  # Reraise the exception after logging it
        
        except Exception as e:
            # Handle general errors (e.g., connection issues, permission issues)
            print(f"Error writing {table_name} to Redshift: {str(e)}")
            raise  # Reraise the exception after logging it


    def load_data_from_redshift(glue_context, connection_options, table_name):
        """
        Load data from a Redshift table into an AWS Glue DynamicFrame.

        This function fetches data from the specified Redshift table using the provided connection options 
        and loads it into an AWS Glue DynamicFrame.

        Args:
            glue_context (GlueContext): The AWS Glue context used to interact with Glue services.
            connection_options (dict): A dictionary containing connection parameters for Redshift.
                                    Example: {"url": "<jdbc_url>", "user": "username", "password": "password"}
            table_name (str): The name of the target Redshift table in the "public" schema.

        Returns:
            DynamicFrame: An AWS Glue DynamicFrame containing the data from the specified Redshift table.

        Raises:
            Exception: If an error occurs while connecting to Redshift, fetching data, or during any internal Glue operation.

        """
        try:
            print(f"Loading {table_name} data from Redshift...")
            
            merged_connection_options = {
                **connection_options,  # Unpack original Redshift options
                "dbtable": f"public.{table_name}"  # Specify the table name dynamically
            
            }
            
            data_from_redshift = glue_context.create_dynamic_frame.from_options(
                connection_type="redshift",
                connection_options=merged_connection_options
            )
            return data_from_redshift
        except Exception as e:
            print(f"Error loading payment method table: {e}")
            raise

