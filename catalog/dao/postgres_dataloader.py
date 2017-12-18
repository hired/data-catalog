import os

from catalog.dao.data_sources import get
from catalog.utils.spark_factory import spark_conf

current_dir = os.path.dirname(__file__)


def read(concept, spark):
    with open(os.path.join(current_dir, "../queries/{}.sql".format(concept))) as sql:
        query = sql.read()
        return _read(query, url=None, spark=spark)


def _read(query, url=None, spark=None):
    if url is None:
        url = get("follower")
    if not spark:
        spark = spark_conf()
    df = spark.read.jdbc(url=url, table=query,
                         properties={'driver': 'org.postgresql.Driver'})

    return df
