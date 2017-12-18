from functools import lru_cache
import os
from io import BytesIO

import yaml
from lastpass import Vault, LastPassIncorrectGoogleAuthenticatorCodeError, \
    LastPassIncorrectYubikeyPasswordError

try:
    LPASS_USERNAME = os.environ["lastpass_username"]
    LPASS_PASSWORD = os.environ["lastpass_password"]
except KeyError:
    print("Lastpass environment is not set")


@lru_cache(1)
def load_lpass_vault(device_id="data_catalog"):
    try:
        # First try without a multifactor password
        return Vault.open_remote(LPASS_USERNAME, LPASS_PASSWORD, None, device_id)
    except LastPassIncorrectGoogleAuthenticatorCodeError as e:
        # Get the code
        multifactor_password = input('Enter Google Authenticator code:')
        # And now retry with the code
        return Vault.open_remote(LPASS_USERNAME, LPASS_PASSWORD, multifactor_password, device_id)
    except LastPassIncorrectYubikeyPasswordError as e:
        # Get the code
        multifactor_password = input('Enter Yubikey password:')
        # And now retry with the code
        return Vault.open_remote(LPASS_USERNAME, LPASS_PASSWORD, multifactor_password, device_id)


@lru_cache(1)
def get_credential_from_vault():
    vault = load_lpass_vault()
    data_credentials = [acc for acc in vault.accounts if 'data-credentials' in str(acc.name)]
    try:
        if len(data_credentials):
            return data_credentials[0].notes.decode("utf-8")
        else:
            return None
    except AttributeError:
        return None


@lru_cache(1)
def get_credentials_from_s3():
    import boto3
    import botocore
    client = boto3.client("s3")
    try:
        obj = client.get_object(Bucket='hired-etl', Key='data_credentials.yaml')
        return BytesIO(obj['Body'].read())
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            print("The credential file does not exist in s3")
        else:
            raise


def load_credentials(credential_type, name, credential_file=None):
    if credential_file:
        with _load_credentials_file(credential_file) as file:
            return yaml.load(file).get(credential_type, {}).get(name, None)
    else:
        credential_from_vault = get_credentials_from_s3()
        return yaml.load(credential_from_vault).get(credential_type, {}).get(name, None)


@lru_cache(1)
def _load_credentials_file(credential_file):
    return open(credential_file)
