from catalog.dao import read
from catalog.utils.spark_factory import spark_conf


def get(concept_name):
    spark = spark_conf()
    return spark.read.json(read(concept_name).rdd.map(lambda x: x.payload))

