import argparse

import boto3
from botocore.exceptions import ClientError

"""
This script moves the contents of a source S3 bucket to a destination S3 bucket,
deletes all versions and delete markers in the source bucket, and finally deletes
the source bucket.

Usage:
    python s3_archiver.py source-bucket-name destination-bucket-name

Requirements:
    - AWS CLI configured with appropriate permissions.
    - boto3 library installed (pip install boto3).
"""


class S3Archiver:

    def __init__(self):
        """
        Initializes the S3Archiver with default S3 client and resource.
        """
        self.client = boto3.client("s3")
        self.s3 = boto3.resource("s3")

    def _delete_all_versions_and_markers(self, bucket_name):
        """
        Deletes all versions and delete markers of all objects in the specified bucket.
        Arguments:
            bucket_name: The name of the bucket
        """
        print(f"🗑️  Deleting all versions and delete markers from {bucket_name}...")
        bucket = self.s3.Bucket(bucket_name)
        versions = bucket.object_versions.all()

        for version in versions:
            try:
                print(
                    f"🗑️  Deleting version {version.id} of object {version.object_key} from bucket {bucket_name}..."
                )
                version.delete()
            except ClientError as e:
                print(
                    f"❌ Error deleting version {version.id} of object {version.object_key}: {e}"
                )

    def _delete_bucket(self, bucket_name):
        """
        Deletes a single bucket after ensuring it is empty.
        Arguments:
            bucket_name: The name of the bucket to be deleted
        """
        print(f"🧹 Ensuring bucket {bucket_name} is empty and deleting it...")
        bucket = self.s3.Bucket(bucket_name)

        try:
            # Ensure the bucket is empty by deleting any remaining objects
            bucket.objects.all().delete()
            bucket.object_versions.all().delete()
        except ClientError as e:
            print(f"❌ Error ensuring bucket {bucket_name} is empty: {e}")

        try:
            bucket.delete()
            print(f"✅ Bucket {bucket_name} has been deleted.")
        except ClientError as e:
            print(f"❌ Error deleting bucket {bucket_name}: {e}")

    def delete_bucket_contents(self, bucket_name):
        """
        Deletes all objects and versions in the specified S3 bucket.
        Arguments:
            bucket_name: The name of the bucket to be emptied
        """
        print(f"🗑️  Deleting all contents of bucket {bucket_name}...")
        self._delete_all_versions_and_markers(bucket_name)
        self._delete_bucket(bucket_name)
        print(f"✅ All contents of bucket {bucket_name} have been deleted.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Move S3 bucket contents and delete the source bucket."
    )

    parser.add_argument("source_bucket", help="The name of the source S3 bucket.")

    args = parser.parse_args()
    # Extract bucket name if S3 URI is provided
    source_bucket = args.source_bucket
    if source_bucket.startswith("s3://"):
        source_bucket = source_bucket.replace("s3://", "").split("/")[0]

    archiver = S3Archiver()
    archiver.delete_bucket_contents(source_bucket)
