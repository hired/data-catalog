from collections import namedtuple

from catalog.dao.credentials_loader import load_credentials

data_source = namedtuple("data_source",
                         ["host", "db_name", "port", "username", "password", "options"],
                         verbose=False)

# ***REMOVED***:***REMOVED***
_data_sources = {
    "local": {"payload": {
        "host": "localhost",
        "port": 5432,
        "db_name": "hired_dev",
        "username": "postgres",
        "password": "HIRED",
        "options": {}},
        "credential_loader_key": None},
    "follower": {"payload": {
        "host": None,
        "port": None,
        "db_name": None,
        "username": None,
        "password": None,
        "options": {"sslmode": "required"}},
        "credential_loader_key": "follower_db"}
}


def _general_data_source_url(this_data_source: data_source):
    jdbc_url = "***REMOVED***://{0}:{1}/{2}?user={3}&password={4}".format(
        this_data_source.host, this_data_source.port, this_data_source.db_name,
        this_data_source.username, this_data_source.password)
    if this_data_source.options:
        return jdbc_url + "&" + "&".join(
            ["{}={}".format(key, value) for key, value in this_data_source.options.items()])
    return jdbc_url


def get(source_name: str):
    if source_name in _data_sources:
        sources = _data_sources[source_name]
        payload = sources["payload"]
        if sources["credential_loader_key"]:
            extra_credentials = load_credentials("database", sources["credential_loader_key"])
            named_parameters = {named_key: extra_credentials.get(named_key, None) for named_key in
                                data_source._fields}
            named_parameters["options"] = {named_key: extra_credentials.get(named_key, None) for
                                           named_key in
                                           extra_credentials if
                                           named_key not in data_source._fields}
            payload.update(named_parameters)
        return _general_data_source_url(data_source(**payload))
    raise AttributeError("Invalid key, Available data sources are {} "
                         .format(_data_sources.keys()))
