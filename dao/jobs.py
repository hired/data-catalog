import json

from pyspark.sql.types import ArrayType, StringType, StructType, StructField, IntegerType

from dao.postgres_dataloader import read
from pyspark.sql.functions import udf, from_json


def get():
    jobs_df = read("jobs")
    return jobs_df


if __name__ == "__main__":
    get()
