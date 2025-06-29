from typing import List

import boto3
import pytest
from moto import mock_s3

from src.aws_utils.s3 import S3Uri, get_files_glob_from_s3, parse_s3_uri, s3_join


@mock_s3
def test_get_files_glob_from_s3_with_moto():
    # Setup fake S3
    s3 = boto3.resource("s3")
    bucket = s3.Bucket("test-bucket")
    bucket.create(CreateBucketConfiguration={"LocationConstraint": "eu-west-1"})

    bucket.put_object(Key="data/file1.7z", Body=b"data")
    bucket.put_object(Key="data/file2.7z", Body=b"data")
    bucket.put_object(Key="data/readme.txt", Body=b"docs")

    result = get_files_glob_from_s3("test-bucket", "data/", ".7z")
    actual_keys = [obj.key for obj in result]
    expected_keys = ["data/file1.7z", "data/file2.7z"]

    assert actual_keys == expected_keys


@pytest.mark.parametrize(
    "s3_uri, expected",
    [
        ("s3://my-bucket/file.csv", S3Uri(bucket="my-bucket", key="file.csv")),
        (
            "s3://bucket-name/folder/subfolder/file.txt",
            S3Uri("bucket-name", "folder/subfolder/file.txt"),
        ),
        ("s3://bucket-name/", S3Uri("bucket-name", "")),
        ("s3://bucket-name/nested/folder/", S3Uri("bucket-name", "nested/folder/")),
        (
            "s3://bucket-name/with spaces and %20encoded",
            S3Uri("bucket-name", "with spaces and %20encoded"),
        ),
    ],
)
def test_parse_s3_uri_valid_inputs(s3_uri: str, expected: str):
    assert parse_s3_uri(s3_uri) == expected


@pytest.mark.parametrize(
    "invalid_uri",
    [
        "http://my-bucket/path/to/file.csv",
        "gs://my-bucket/file.csv",
        "/local/path/to/file.csv",
        "bucket-name/key",
        "ftp://bucket/file.csv",
        "file://bucket-name/file.csv",
    ],
)
def test_parse_s3_uri_invalid_scheme_raises(invalid_uri: str):
    with pytest.raises(ValueError, match="Expected S3 URI"):
        parse_s3_uri(invalid_uri)


@pytest.mark.parametrize(
    "parts,expected",
    [
        (["folder", "file.csv"], "folder/file.csv"),
        (["folder/", "/file.csv"], "folder/file.csv"),
        (["a/", "/b/", "c.csv"], "a/b/c.csv"),
        (["", "folder", "", "file.csv"], "folder/file.csv"),
        (["", "", ""], ""),
        (["folder", None, "file.csv"], "folder/file.csv"),
        ([None, "only.csv"], "only.csv"),
    ],
)
def test_s3_join(parts: List[str], expected: str):
    assert s3_join(*parts) == expected
