from enum import Enum

COMMON_REQUIRED_CONFIG_KEYS = ('bucket',)
ROLE_REQUIRED_CONFIG_KEYS = ('account_id', 'role_name', 'external_id')
ACCESS_KEY_REQUIRED_CONFIG_KEYS = ('aws_access_key_id', 'aws_secret_access_key')
ACCESS_KEY_OPTIONAL_CONFIG_KEYS = ('aws_session_token',)


class AwsAuthMode(Enum):
    DEFAULT = 'default'  # Use ambient worker credentials without assuming a customer role.
    ROLE = 'role'
    ACCESS_KEY = 'access_key'


def detect_auth_mode(config):
    """Validate config and detect the selected AWS authentication mode."""
    role_has_values = any(
        config.get(key) not in (None, '')
        for key in ROLE_REQUIRED_CONFIG_KEYS
    )
    access_key_has_values = any(
        config.get(key) not in (None, '')
        for key in (
            ACCESS_KEY_REQUIRED_CONFIG_KEYS
            + ACCESS_KEY_OPTIONAL_CONFIG_KEYS
        )
    )

    if role_has_values and access_key_has_values:
        raise ValueError(
            'AWS authentication config must use either role assumption '
            '(account_id, role_name, external_id) or access key credentials '
            '(aws_access_key_id, aws_secret_access_key), not both.'
        )

    if role_has_values:
        auth_mode = AwsAuthMode.ROLE
        required_keys = ROLE_REQUIRED_CONFIG_KEYS
    elif access_key_has_values:
        auth_mode = AwsAuthMode.ACCESS_KEY
        required_keys = ACCESS_KEY_REQUIRED_CONFIG_KEYS
    elif any(key in config for key in ROLE_REQUIRED_CONFIG_KEYS):
        auth_mode = AwsAuthMode.ROLE
        required_keys = ROLE_REQUIRED_CONFIG_KEYS
    elif any(key in config for key in ACCESS_KEY_REQUIRED_CONFIG_KEYS):
        auth_mode = AwsAuthMode.ACCESS_KEY
        required_keys = ACCESS_KEY_REQUIRED_CONFIG_KEYS
    else:
        return AwsAuthMode.DEFAULT

    missing_keys = [
        key for key in required_keys
        if config.get(key) in (None, '')
    ]
    if missing_keys:
        auth_label = (
            'role assumption'
            if auth_mode == AwsAuthMode.ROLE
            else 'access key'
        )
        raise ValueError(
            f'Incomplete {auth_label} config. '
            f'Missing required keys: {", ".join(missing_keys)}'
        )

    return auth_mode


def get_required_config_keys(auth_mode):
    auth_required_keys = {
        AwsAuthMode.ROLE: ROLE_REQUIRED_CONFIG_KEYS,
        AwsAuthMode.ACCESS_KEY: ACCESS_KEY_REQUIRED_CONFIG_KEYS,
    }.get(auth_mode, ())
    return [*COMMON_REQUIRED_CONFIG_KEYS, *auth_required_keys]
