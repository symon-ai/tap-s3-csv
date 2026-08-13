from enum import Enum

ROLE_CONFIG_KEYS = ('account_id', 'role_name', 'external_id')
ACCESS_KEY_REQUIRED_KEYS = ('aws_access_key_id', 'aws_secret_access_key')
ACCESS_KEY_OPTIONAL_KEYS = ('aws_session_token',)
ACCESS_KEY_CONFIG_KEYS = ACCESS_KEY_REQUIRED_KEYS + ACCESS_KEY_OPTIONAL_KEYS

REQUIRED_CONFIG_KEYS_DEFAULT = ['bucket']
REQUIRED_CONFIG_KEYS_ROLE = ['bucket', *ROLE_CONFIG_KEYS]
REQUIRED_CONFIG_KEYS_ACCESS_KEY = ['bucket', *ACCESS_KEY_REQUIRED_KEYS]


class AwsAuthMode(Enum):
    DEFAULT = 'default'  # Use ambient worker credentials without assuming a customer role.
    ROLE = 'role'
    ACCESS_KEY = 'access_key'


def _configured_keys(config, keys):
    return {
        key for key in keys
        if config.get(key) not in (None, '')
    }


def _missing_keys(config, required_keys):
    return [
        key for key in required_keys
        if config.get(key) in (None, '')
    ]


def _validate_required_keys(config, required_keys, auth_label):
    missing_keys = _missing_keys(config, required_keys)
    if missing_keys:
        raise ValueError(
            f'Incomplete {auth_label} config. Missing required keys: '
            f'{", ".join(missing_keys)}'
        )


def validate_auth_config(config):
    role_keys = _configured_keys(config, ROLE_CONFIG_KEYS)
    access_key_keys = _configured_keys(config, ACCESS_KEY_CONFIG_KEYS)

    if role_keys and access_key_keys:
        raise ValueError(
            'AWS authentication config must use either role assumption '
            '(account_id, role_name, external_id) or access key credentials '
            '(aws_access_key_id, aws_secret_access_key), not both.'
        )

    if role_keys:
        required_keys, auth_label = ROLE_CONFIG_KEYS, 'role assumption'
    elif access_key_keys:
        required_keys, auth_label = ACCESS_KEY_REQUIRED_KEYS, 'access key'
    elif any(key in config for key in ROLE_CONFIG_KEYS):
        required_keys, auth_label = ROLE_CONFIG_KEYS, 'role assumption'
    elif any(key in config for key in ACCESS_KEY_REQUIRED_KEYS):
        required_keys, auth_label = ACCESS_KEY_REQUIRED_KEYS, 'access key'
    else:
        return

    _validate_required_keys(config, required_keys, auth_label)


def detect_auth_mode(config):
    """Validate config and detect the selected AWS authentication mode."""
    validate_auth_config(config)

    if _configured_keys(config, ROLE_CONFIG_KEYS):
        return AwsAuthMode.ROLE

    if _configured_keys(config, ACCESS_KEY_REQUIRED_KEYS):
        return AwsAuthMode.ACCESS_KEY

    return AwsAuthMode.DEFAULT


def get_required_config_keys(auth_mode):
    if auth_mode == AwsAuthMode.ROLE:
        return REQUIRED_CONFIG_KEYS_ROLE
    if auth_mode == AwsAuthMode.ACCESS_KEY:
        return REQUIRED_CONFIG_KEYS_ACCESS_KEY
    return REQUIRED_CONFIG_KEYS_DEFAULT


def is_external_auth(auth_mode):
    return auth_mode in (AwsAuthMode.ROLE, AwsAuthMode.ACCESS_KEY)
