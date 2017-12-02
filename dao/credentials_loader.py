from functools import lru_cache
import os

import yaml
from lastpass import Vault, LastPassIncorrectGoogleAuthenticatorCodeError, \
    LastPassIncorrectYubikeyPasswordError

LPASS_USERNAME = os.environ["lastpass_username"]
LPASS_PASSWORD = os.environ["lastpass_password"]

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


def load_credentials(credential_type, name, credential_file=None):
    if credential_file:
        with _load_credentials_file(credential_file) as file:
            return yaml.load(file).get(credential_type, {}).get(name, None)
    else:
        credential_from_vault = get_credential_from_vault()
        return yaml.load(credential_from_vault).get(credential_type, {}).get(name, None)


@lru_cache(1)
def _load_credentials_file(credential_file):
    return open(credential_file)
