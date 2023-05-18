import boto3
import os
import logging
from dotenv import dotenv_values
from botocore.exceptions import ClientError

config = dotenv_values(".env")


class S3Client:
    def __init__(self):
        self.bucket_name = config["BUCKET_NAME"]
        self.client = boto3.client(
                's3',
                aws_access_key_id=config['AWS_ACCESS_KEY_ID'],
                aws_secret_access_key=config['AWS_SECRET_ACCESS_KEY'],
                region_name=config['REGION_NAME']
                )

    def upload_file(self, file_path: str, object_name=None):
        if object_name is None:
            object_name = os.path.basename(file_path)
        try:
            response = self.client.upload_file(
                    file_path,
                    self.bucket_name,
                    object_name)
        except ClientError as e:
            logging.error(e)
            return False
        return response
