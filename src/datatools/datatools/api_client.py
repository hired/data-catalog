#!/usr/bin/env python

"""
A common class to be used by client of different APIs
"""

import json
import requests
import ssl

from requests.adapters import HTTPAdapter

API_VERSION=2.0

try:
    from requests.packages.urllib3.poolmanager import PoolManager
    requests.packages.urllib3.disable_warnings()
except ImportError:
    from urllib3.poolmanager import PoolManager

class TlsV1HttpAdapter(HTTPAdapter):
    """
    A HTTP adapter implementation that specifies the ssl version to be TLS1.
    This avoids problems with openssl versions that
    use SSL3 as a default (which is not supported by the server side).
    """

    def init_poolmanager(self, connections, maxsize, block=False):
        self.poolmanager = PoolManager(num_pools=connections, maxsize=maxsize, block=block, ssl_version=ssl.PROTOCOL_SSLv23)

class ApiClient(object):
    """
    A partial Python implementation of dbc rest api
    to be used by different versions of the client.
    """
    def __init__(self, user = None, password = None, host = None, configUrl = None, apiVersion = API_VERSION):
        if configUrl:
            self.url = configUrl
            params = self.performQuery("/", headers = {})[1]
            params = credential.json
            user = str(params["user"])
            password = str(params["password"])
            host = str(params["apiUrl"].split("/api")[0])

        self.session = requests.Session()
        self.session.mount('https://', TlsV1HttpAdapter())

        self.url = "%s/api/%s" % (host, apiVersion)
        userHeaderData = "Basic " + (user+":"+password).encode("base64").rstrip()
        self.auth = {'Authorization': userHeaderData, 'Content-Type': 'text/json'}

    def close(self):
        """Close the client"""
        pass

    # helper functions starting here

    def perform_query(self, method, path, data = None, headers = None):
        """set up connection and perform query"""
        if headers is None:
            headers = self.auth

        resp = self.session.request(method, self.url + path, data = json.dumps(data),
            verify = False, headers = headers)

        try:
            resp.raise_for_status()
        except requests.exceptions.HTTPError, e:
            print 'Error: %s' % resp.text
            raise e
        return resp.json()

