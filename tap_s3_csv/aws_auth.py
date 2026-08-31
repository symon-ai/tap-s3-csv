COMMON_REQUIRED_CONFIG_KEYS = ('bucket',)
ROLE_REQUIRED_CONFIG_KEYS = ('account_id', 'role_name', 'external_id')
ACCESS_KEY_REQUIRED_CONFIG_KEYS = ('aws_access_key_id', 'aws_secret_access_key')
ACCESS_KEY_OPTIONAL_CONFIG_KEYS = ('aws_session_token',)

AUTH_METHOD_ROLE = 'awsRoleAssumption'
AUTH_METHOD_ACCESS_KEY = 's3Credentials'


def _has_non_empty_value(config, keys):
    return any(config.get(key) not in (None, '') for key in keys)


def _has_any_key(config, keys):
    return any(key in config for key in keys)


def _missing_keys(config, keys):
    return [key for key in keys if config.get(key) in (None, '')]


def _get_auth_requirements(auth_method):
    if auth_method == AUTH_METHOD_ROLE:
        return ROLE_REQUIRED_CONFIG_KEYS, 'role assumption'
    if auth_method == AUTH_METHOD_ACCESS_KEY:
        return ACCESS_KEY_REQUIRED_CONFIG_KEYS, 'access key'
    return (), None


def resolve_auth_method(config):
    """Return the configured auth method, or None for internal S3."""
    auth_method = config.get('auth_method')
    if auth_method is None and 'external_id' in config:
        return AUTH_METHOD_ROLE
    if auth_method is None:
        return None
    if auth_method not in (AUTH_METHOD_ROLE, AUTH_METHOD_ACCESS_KEY):
        raise ValueError(
            f'Unsupported auth_method {auth_method!r}. '
            f'Must be {AUTH_METHOD_ACCESS_KEY!r} or {AUTH_METHOD_ROLE!r}.'
        )
    return auth_method


def validate_auth_config(config, auth_method):
    required_keys, auth_label = _get_auth_requirements(auth_method)
    if auth_method is None:
        if _has_non_empty_value(
                config, ROLE_REQUIRED_CONFIG_KEYS
                + ACCESS_KEY_REQUIRED_CONFIG_KEYS
                + ACCESS_KEY_OPTIONAL_CONFIG_KEYS
        ) or _has_any_key(
                config,
                ROLE_REQUIRED_CONFIG_KEYS + ACCESS_KEY_REQUIRED_CONFIG_KEYS
        ):
            raise ValueError(
                'auth_method is required when AWS authentication credentials are provided.'
            )
        return

    missing_keys = _missing_keys(config, required_keys)
    if missing_keys:
        raise ValueError(
            f'Incomplete {auth_label} config. '
            f'Missing required keys: {", ".join(missing_keys)}'
        )


def get_required_config_keys(auth_method):
    auth_required_keys, _ = _get_auth_requirements(auth_method)
    return [*COMMON_REQUIRED_CONFIG_KEYS, *auth_required_keys]
