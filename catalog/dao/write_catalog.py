import catalog.dao.concepts
from catalog.utils.spark_factory import spark_conf

spark = spark_conf()

DATA_CATALOG_SINK = 's3n://hired-etl-staging/data_catalog'
concepts = ["offers", "employers", "jobs", "activities", "candidates"]
for concept in concepts:
    df = catalog.dao.concepts.get(concept)
    df.write.mode("overwrite").parquet(DATA_CATALOG_SINK + "/" + concept)
