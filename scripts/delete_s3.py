import logging

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


class S3_Client:
    def __init__(self):
        """
        Initializes the S3_Client with default S3 client and resource.
        """
        self.client = boto3.client("s3")
        self.s3 = boto3.resource("s3")

    def create_bucket(self, bucket_name):
        self.client.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={
                "LocationConstraint": self.client.meta.region_name
            },
        )
        logger.info(f"✅ Bucket {bucket_name} created successfully. ✅")

    def upload_file(self, bucket_name, file_name, object_name=None):
        """
        Uploads a file to the specified S3 bucket.
        Arguments:
            bucket_name: The name of the bucket
            file_name: The path to the file to upload
            object_name: The name of the object in S3 (optional)
        """
        if object_name is None:
            object_name = file_name

        try:
            self.client.upload_file(file_name, bucket_name, object_name)
            logger.info(f"✅ File {file_name} uploaded to {bucket_name}/{object_name}.")
        except ClientError as e:
            logger.error(f"❌ Failed to upload file: {e}")

    def delete_bucket_prefix(self, bucket_name, prefix):
        """
        Deletes all objects under the specified prefix in the S3 bucket.
        Arguments:
            bucket_name: The name of the bucket
            prefix: The prefix path to delete (e.g., 'cps_dummy/happy_path/')
        """
        logger.info(
            f"🗑️  Deleting all contents under prefix {prefix} in {bucket_name}..."
        )

        try:
            # Ensure prefix ends with '/' for consistency
            if not prefix.endswith("/"):
                prefix += "/"

            # Use paginator to handle large numbers of objects efficiently
            paginator = self.client.get_paginator("list_objects_v2")

            for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
                if "Contents" in page:
                    objects_to_delete = [
                        {"Key": obj["Key"]} for obj in page["Contents"]
                    ]

                    if objects_to_delete:
                        self.client.delete_objects(
                            Bucket=bucket_name, Delete={"Objects": objects_to_delete}
                        )
                        logger.debug(f"Deleted {len(objects_to_delete)} objects")

            logger.info(f"✅ Successfully deleted all contents under prefix {prefix}")

        except ClientError as e:
            logger.error(f"❌ Failed to delete prefix contents: {e}")
            raise
