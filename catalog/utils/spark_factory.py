from functools import lru_cache

from pyspark.sql import SparkSession
import boto3

@lru_cache(10)
def _load_aws_credentials():
    session = boto3.Session()
    credentials = session.get_credentials()
    if not (credentials.access_key or credentials.secret_key):
        raise AttributeError("Could not find AWS credentials.Please set up aws credentials, using "
                             "aws configure")
    return credentials


@lru_cache(10)
def spark_conf(master="local", application_name="DistributeThis"):
    credentials = _load_aws_credentials()
    spark = SparkSession.builder \
        .master(master) \
        .appName(application_name) \
        .config("spark.driver.memory", "6g") \
        .config("spark.jars.packages",
                "org.apache.hadoop:hadoop-aws:2.7.3,"
                "org.postgresql:postgresql:42.1.1"
                ) \
        .getOrCreate()
    spark._jsc.hadoopConfiguration().set("fs.s3n.awsAccessKeyId", credentials.access_key)
    spark._jsc.hadoopConfiguration().set("fs.s3n.awsSecretAccessKey",
                                         credentials.secret_key)
    return spark
