import time
import json
import boto3
from botocore.exceptions import ClientError
from glueetlutils.logger import logger
from glueetlutils.timing import log_time


class CommonUtils:
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
