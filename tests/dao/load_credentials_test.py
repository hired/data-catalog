from dao.credentials_loader import load_credentials
import unittest
import os


def load_test_credentials_file():
    return os.path.abspath(os.path.join(os.path.dirname(__file__), "test_credentials_file.yaml"))


class LoadCredentialsTest(unittest.TestCase):
    def test_get_valid_database_credential(self):
        db_credentials = load_credentials(credential_type="database",
                                          name="test_db",
                                          credential_file=load_test_credentials_file())
        assert (db_credentials == {'dbname': 'test_this',
                                   'host': 'localhost',
                                   'port': 80, 'user': 'dbuser',
                                   'password': 'dbpassword',
                                   'sslmode': 'require'})
