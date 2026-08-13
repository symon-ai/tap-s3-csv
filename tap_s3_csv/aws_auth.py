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


def detect_auth_mode(config):
    """Detect AWS auth mode from config keys. Mutually exclusive role vs access key."""
    role_keys_present = [
        config.get(key) not in (None, '') for key in ROLE_CONFIG_KEYS
    ]
    access_key_keys_present = {
        key: config.get(key) not in (None, '') for key in ACCESS_KEY_CONFIG_KEYS
    }

    has_any_role_key = any(role_keys_present)
    has_any_access_key_key = any(access_key_keys_present.values())
    has_role_config = any(key in config for key in ROLE_CONFIG_KEYS)
    has_access_key_config = any(key in config for key in ACCESS_KEY_CONFIG_KEYS)

    if has_any_role_key and has_any_access_key_key:
        raise ValueError(
            'AWS authentication config must use either role assumption '
            '(account_id, role_name, external_id) or access key credentials '
            '(aws_access_key_id, aws_secret_access_key), not both.'
        )

    if has_any_role_key or (has_role_config and not has_any_access_key_key):
        if not all(role_keys_present):
            missing_keys = [
                key for key, present in zip(ROLE_CONFIG_KEYS, role_keys_present)
                if not present
            ]
            raise ValueError(
                'Incomplete role assumption config. Missing required keys: '
                f'{", ".join(missing_keys)}'
            )
        return AwsAuthMode.ROLE

    if has_any_access_key_key or has_access_key_config:
        missing_keys = [
            key for key in ACCESS_KEY_REQUIRED_KEYS
            if not access_key_keys_present[key]
        ]
        if missing_keys:
            raise ValueError(
                'Incomplete access key config. Missing required keys: '
                f'{", ".join(missing_keys)}'
            )
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
