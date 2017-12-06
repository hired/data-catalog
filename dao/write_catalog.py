from dao.jobs import get

jobs_df = get()
DATA_CATALOG_SINK = 's3n://hired-etl-staging/data_catalog'
jobs_df.write.mode("overwrite").parquet(DATA_CATALOG_SINK + "/jobs")
