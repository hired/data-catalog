from catalog.dao.write_catalog import create_catalog


def write_catalog(spark_context, sql_context, params):
    if 'working_dir' not in params:
        raise Exception(
            "working_dir must be passed in params in order to run attribute generator")

    create_catalog(spark_context)
