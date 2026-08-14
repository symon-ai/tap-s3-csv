from enum import Enum

COMMON_REQUIRED_CONFIG_KEYS = ('bucket',)
ROLE_REQUIRED_CONFIG_KEYS = ('account_id', 'role_name', 'external_id')
ACCESS_KEY_REQUIRED_CONFIG_KEYS = ('aws_access_key_id', 'aws_secret_access_key')
ACCESS_KEY_OPTIONAL_CONFIG_KEYS = ('aws_session_token',)


class AwsAuthMode(Enum):
    DEFAULT = 'default'  # Use ambient worker credentials without assuming a customer role.
    ROLE = 'role'
    ACCESS_KEY = 'access_key'


def _has_non_empty_value(config, keys):
    return any(config.get(key) not in (None, '') for key in keys)


def _has_any_key(config, keys):
    return any(key in config for key in keys)


def _missing_keys(config, keys):
    return [key for key in keys if config.get(key) in (None, '')]


def validate_auth_config(config):
    role_has_values = _has_non_empty_value(config, ROLE_REQUIRED_CONFIG_KEYS)
    access_key_has_values = _has_non_empty_value(
        config,
        ACCESS_KEY_REQUIRED_CONFIG_KEYS + ACCESS_KEY_OPTIONAL_CONFIG_KEYS,
    )

    if role_has_values and access_key_has_values:
        raise ValueError(
            'AWS authentication config must use either role assumption '
            '(account_id, role_name, external_id) or access key credentials '
            '(aws_access_key_id, aws_secret_access_key), not both.'
        )

    if role_has_values:
        required_keys = ROLE_REQUIRED_CONFIG_KEYS
        auth_label = 'role assumption'
    elif access_key_has_values:
        required_keys = ACCESS_KEY_REQUIRED_CONFIG_KEYS
        auth_label = 'access key'
    elif _has_any_key(config, ROLE_REQUIRED_CONFIG_KEYS):
        required_keys = ROLE_REQUIRED_CONFIG_KEYS
        auth_label = 'role assumption'
    elif _has_any_key(config, ACCESS_KEY_REQUIRED_CONFIG_KEYS):
        required_keys = ACCESS_KEY_REQUIRED_CONFIG_KEYS
        auth_label = 'access key'
    else:
        return

    missing_keys = _missing_keys(config, required_keys)
    if missing_keys:
        raise ValueError(
            f'Incomplete {auth_label} config. '
            f'Missing required keys: {", ".join(missing_keys)}'
        )


def get_auth_mode(config):
    """Validate config and return the selected AWS authentication mode."""
    validate_auth_config(config)

    if _has_non_empty_value(config, ROLE_REQUIRED_CONFIG_KEYS):
        return AwsAuthMode.ROLE
    if _has_non_empty_value(config, ACCESS_KEY_REQUIRED_CONFIG_KEYS):
        return AwsAuthMode.ACCESS_KEY
    return AwsAuthMode.DEFAULT


def get_required_config_keys(auth_mode):
    auth_required_keys = {
        AwsAuthMode.ROLE: ROLE_REQUIRED_CONFIG_KEYS,
        AwsAuthMode.ACCESS_KEY: ACCESS_KEY_REQUIRED_CONFIG_KEYS,
    }.get(auth_mode, ())
    return [*COMMON_REQUIRED_CONFIG_KEYS, *auth_required_keys]
