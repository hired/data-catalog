from catalog.dao.postgres_dataloader import read
from catalog.utils.spark_factory import spark_conf


def get(concept_name, spark=None):
    if not spark:
        spark = spark_conf()
    return spark.read.json(read(concept_name, spark).rdd.map(lambda x: x.payload))
