import json
import os

import catalog.dao.concepts
from catalog.utils.spark_factory import spark_conf

DATA_CATALOG_SINK = 's3n://hired-etl/data_catalog'


def create_catalog(spark=None):
    if not spark:
        spark = spark_conf()
    concepts = ["activities"]
    # concepts = ["offers", "employers", "jobs", "activities", "candidates"]
    for concept in concepts:
        df = catalog.dao.concepts.get(concept, spark)
        df.write.mode("overwrite").parquet(DATA_CATALOG_SINK + "/" + concept)

