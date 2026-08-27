COMMON_REQUIRED_CONFIG_KEYS = ('bucket',)
ROLE_REQUIRED_CONFIG_KEYS = ('account_id', 'role_name', 'external_id')
ACCESS_KEY_REQUIRED_CONFIG_KEYS = ('aws_access_key_id', 'aws_secret_access_key')
ACCESS_KEY_OPTIONAL_CONFIG_KEYS = ('aws_session_token',)

AUTH_METHOD_ROLE = 'awsRoleAssumption'
AUTH_METHOD_ACCESS_KEY = 's3Credentials'
AUTH_CONFIG = {
    AUTH_METHOD_ROLE: (ROLE_REQUIRED_CONFIG_KEYS, 'role assumption'),
    AUTH_METHOD_ACCESS_KEY: (ACCESS_KEY_REQUIRED_CONFIG_KEYS, 'access key'),
}


def _has_non_empty_value(config, keys):
    return any(config.get(key) not in (None, '') for key in keys)


def _missing_keys(config, keys):
    return [key for key in keys if config.get(key) in (None, '')]


def _get_auth_config(auth_method):
    if auth_method is None:
        return (), None
    try:
        return AUTH_CONFIG[auth_method]
    except KeyError as error:
        raise ValueError(
            f'Unsupported auth_method {auth_method!r}. '
            f'Must be {AUTH_METHOD_ACCESS_KEY!r} or {AUTH_METHOD_ROLE!r}.'
        ) from error


def resolve_auth_method(config):
    """Validate config and return its auth method, or None for internal S3."""
    auth_method = config.get('auth_method')
    if auth_method is None:
        role_has_values = _has_non_empty_value(
            config, ROLE_REQUIRED_CONFIG_KEYS)
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

        if role_has_values or any(
                key in config for key in ROLE_REQUIRED_CONFIG_KEYS):
            auth_method = AUTH_METHOD_ROLE
        elif access_key_has_values or any(
                key in config for key in ACCESS_KEY_REQUIRED_CONFIG_KEYS):
            auth_method = AUTH_METHOD_ACCESS_KEY
        else:
            return None

    required_keys, auth_label = _get_auth_config(auth_method)
    missing_keys = _missing_keys(config, required_keys)
    if missing_keys:
        raise ValueError(
            f'Incomplete {auth_label} config. '
            f'Missing required keys: {", ".join(missing_keys)}'
        )
    return auth_method


def get_required_config_keys(auth_method):
    auth_required_keys, _ = _get_auth_config(auth_method)
    return [*COMMON_REQUIRED_CONFIG_KEYS, *auth_required_keys]
