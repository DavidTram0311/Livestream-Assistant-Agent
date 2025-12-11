from pyspark.sql import SparkSession
from dotenv import load_dotenv
import os, sys
import boto3

class SparkS3Connector:
    def __init__(self, app_name="spark-s3-connector", extra_packages=None):
        load_dotenv()
        self.app_name = app_name
        self.extra_packages = extra_packages
        self.aws_bucket_name = os.getenv("AWS_BUCKET_NAME")
        self.aws_profile_name = os.getenv("AWS_PROFILE_NAME", "default")
        self.aws_region = os.getenv("AWS_REGION", "ap-northeast-1")
        self.s3_prefix = os.getenv("S3_PREFIX", "amazon_product_data_2014/raw")
        self.packages = [
            "org.apache.hadoop:hadoop-aws:3.3.2",
            "com.amazonaws:aws-java-sdk-bundle:1.11.1026",
        ]
        if self.extra_packages:
            self.packages.extend(self.extra_packages)

    def _get_aws_credentials(self):
        """Internal method to fetch SSO/Profile credentials via Boto3."""
        try:
            session = boto3.Session(profile_name=self.aws_profile_name)
            credentials = session.get_credentials()
            if not credentials:
                raise Exception(f"No credentials found for profile {self.aws_profile_name}")
            frozen_credentials = credentials.get_frozen_credentials()
            return frozen_credentials
        except Exception as e:
            raise Exception(f"Failed to get AWS credentials for profile {self.aws_profile_name}: {e}")

    def get_spark_session(self):
        """Build and return a SparkSession configured for S3 access."""
        print(f"--- Initializing Spark Session ({self.app_name}) ---")
        frozen_credentials = self._get_aws_credentials()

        packages = ",".join(self.packages)

        builder = (
            SparkSession.builder.appName(self.app_name)
            .config("spark.jars.packages", packages)
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.access.key", frozen_credentials.access_key)
            .config("spark.hadoop.fs.s3a.secret.key", frozen_credentials.secret_key)
            .config("spark.hadoop.fs.s3a.endpoint", f"s3.{self.aws_region}.amazonaws.com")
        )

        if frozen_credentials.token:
            builder = builder.config("spark.hadoop.fs.s3a.session.token", frozen_credentials.token) \
                .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider")

        print(f"--- Spark Session initialized ---")
        return builder.getOrCreate()

    def __enter__(self):
        """Called when entering a with block."""
        return self.get_spark_session()

    def __exit__(self, exc_type, exc_value, traceback):
        """Called when exiting a with block."""
        if self.spark:
            print("---- Auto-stopping Spark Session ----")
            self.spark.stop()
            self.spark = None
            print(f"--- Spark Session stopped ---")
        else:
            print("---- Spark Session not found ----")
        return False

    def get_s3_path(self, path):
        """Return the full S3 path for a given relative path."""
        return f"s3a://{self.aws_bucket_name}/{self.s3_prefix}/{path}"