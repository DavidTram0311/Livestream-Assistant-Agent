from pyspark.sql import SparkSession
from dotenv import load_dotenv
import os, sys
import boto3
from spark_connector import SparkS3Connector

load_dotenv()

aws_bucket_name = os.getenv("AWS_BUCKET_NAME")
aws_profile_name = os.getenv("AWS_PROFILE_NAME", "default")
aws_region = os.getenv("AWS_REGION", "ap-northeast-1")
s3_prefix = os.getenv("S3_PREFIX", "amazon_product_data_2014/raw")

if not aws_bucket_name:
    sys.exit("Missing AWS_BUCKET_NAME")

# Retrieve credentials via boto3 (handles SSO and other profiles)
try:
    session = boto3.Session(profile_name=aws_profile_name)
    credentials = session.get_credentials()
    if not credentials:
        sys.exit(f"No credentials found for profile {aws_profile_name}")
    frozen_credentials = credentials.get_frozen_credentials()
except Exception as e:
    sys.exit(f"Failed to get AWS credentials for profile {aws_profile_name}: {e}")

# Build Spark Session
builder = (
    SparkSession.builder.appName("s3-read")
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.2,com.amazonaws:aws-java-sdk-bundle:1.11.1026")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.access.key", frozen_credentials.access_key)
    .config("spark.hadoop.fs.s3a.secret.key", frozen_credentials.secret_key)
    .config("spark.hadoop.fs.s3a.endpoint", f"s3.{aws_region}.amazonaws.com")
)

if frozen_credentials.token:
    builder = builder.config("spark.hadoop.fs.s3a.session.token", frozen_credentials.token) \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider")

spark = builder.getOrCreate()

print(f"Reading from s3a://{aws_bucket_name}/{s3_prefix}/meta/meta_Cell_Phones_and_Accessories.jsonl")
df = spark.read.json(f"s3a://{aws_bucket_name}/{s3_prefix}/meta/meta_Cell_Phones_and_Accessories.jsonl")
df.show(5)

from pyspark.sql import functions as F

# This shows the distribution of rows across your 10 partitions
df.withColumn("partition_id", F.spark_partition_id()) \
.groupBy("partition_id") \
.count() \
.show()

spark.stop()
