import os

from catalog.dao.data_sources import get
from catalog.utils.spark_factory import spark_conf

spark = spark_conf()

current_dir = os.path.dirname(__file__)


def read(concept):
    with open(os.path.join(current_dir, "../queries/{}.sql".format(concept))) as sql:
        query = sql.read()
        return _read(query)


def _read(query, url=None):
    if url is None:
        url = get("follower")
    df = spark.read.jdbc(url=url, table=query,
                         properties={'driver': 'org.postgresql.Driver'})

    return df
