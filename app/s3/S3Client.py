import boto3
import os
import logging
from dotenv import dotenv_values
from botocore.exceptions import ClientError
from app.errors import UploadingFileError, DownloadFileError, \
        FileNotFoundError, DeletingFileError, UpdatingFileError

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

    def _file_exists_in_bucket(self, filename):
        try:
            self.client.head_object(Bucket=self.bucket_name, Key=filename)
            return True
        except ClientError:
            return False

    def upload_file(self, file, filename):
        try:
            self.client.upload_fileobj(
                    file,
                    self.bucket_name,
                    filename
                    )
        except ClientError as e:
            logging.error(e)
            raise UploadingFileError

    def download_file(self, file_path: str):
        file_name = os.path.basename(file_path)
        if not self._file_exists_in_bucket(file_name):
            raise FileNotFoundError
        try:
            with open(file_path, 'wb') as file:
                self.client.download_fileobj(
                        self.bucket_name,
                        file_name,
                        file
                        )
        except ClientError as e:
            logging.error(e)
            raise DownloadFileError

    def delete_file(self, filename):
        if not self._file_exists_in_bucket(filename):
            raise FileNotFoundError
        try:
            self.client.delete_object(Bucket=self.bucket_name, Key=filename)
        except ClientError as e:
            logging.error(e)
            raise DeletingFileError

    def update_file(self, file, filename):
        if not self._file_exists_in_bucket(filename):
            raise FileNotFoundError
        try:
            self.upload_file(file, filename)
        except UpdatingFileError:
            raise UpdatingFileError
