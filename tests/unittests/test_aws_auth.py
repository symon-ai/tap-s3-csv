import json
import unittest
from unittest import mock

from botocore.exceptions import ClientError

from tap_s3_csv import aws_auth
from tap_s3_csv.aws_auth import AwsAuthMode, AUTH_METHOD_ACCESS_KEY, AUTH_METHOD_ROLE
import tap_s3_csv
from tap_s3_csv import s3
from tap_s3_csv.symon_exception import SymonException


def _sample_tables():
    return [{
        'table_name': 'my_table',
        'search_pattern': '.*\\.csv',
        'key_properties': [],
    }]


def _make_parse_args_side_effect(config):
    def parse_args(required_keys):
        missing = [key for key in required_keys if key not in config]
        if missing:
            raise SystemExit(f'Missing required config keys: {missing}')
        return mock.Mock(
            config=config,
            discover=True,
            properties=None,
            state={},
        )
    return parse_args


class TestAwsAuthDetection(unittest.TestCase):

    def test_default_mode_when_no_auth_keys(self):
        config = {'bucket': 'my-bucket'}
        self.assertEqual(aws_auth.get_auth_mode(config), AwsAuthMode.DEFAULT)

    def test_role_mode_when_all_role_keys_present(self):
        config = {
            'bucket': 'my-bucket',
            'account_id': '111222333444',
            'role_name': 'my-role',
            'external_id': 'external-id',
        }
        self.assertEqual(aws_auth.get_auth_mode(config), AwsAuthMode.ROLE)

    def test_access_key_mode_when_required_keys_present(self):
        config = {
            'bucket': 'my-bucket',
            'aws_access_key_id': 'AKIAEXAMPLE',
            'aws_secret_access_key': 'secret',
        }
        self.assertEqual(aws_auth.get_auth_mode(config), AwsAuthMode.ACCESS_KEY)

    def test_access_key_mode_with_session_token(self):
        config = {
            'bucket': 'my-bucket',
            'aws_access_key_id': 'AKIAEXAMPLE',
            'aws_secret_access_key': 'secret',
            'aws_session_token': 'token',
        }
        self.assertEqual(aws_auth.get_auth_mode(config), AwsAuthMode.ACCESS_KEY)

    def test_rejects_mixed_role_and_access_key_config(self):
        config = {
            'bucket': 'my-bucket',
            'account_id': '111222333444',
            'role_name': 'my-role',
            'external_id': 'external-id',
            'aws_access_key_id': 'AKIAEXAMPLE',
            'aws_secret_access_key': 'secret',
        }
        with self.assertRaisesRegex(ValueError, 'not both'):
            aws_auth.get_auth_mode(config)

    def test_rejects_role_config_with_session_token(self):
        config = {
            'bucket': 'my-bucket',
            'account_id': '111222333444',
            'role_name': 'my-role',
            'external_id': 'external-id',
            'aws_session_token': 'token',
        }
        with self.assertRaisesRegex(ValueError, 'not both'):
            aws_auth.validate_auth_config(config)

    def test_rejects_incomplete_role_config(self):
        config = {
            'bucket': 'my-bucket',
            'external_id': 'external-id',
        }
        with self.assertRaisesRegex(ValueError, 'Incomplete role assumption config'):
            aws_auth.get_auth_mode(config)

    def test_rejects_incomplete_access_key_config(self):
        config = {
            'bucket': 'my-bucket',
            'aws_access_key_id': 'AKIAEXAMPLE',
        }
        with self.assertRaisesRegex(ValueError, 'Incomplete access key config'):
            aws_auth.get_auth_mode(config)

    def test_rejects_session_token_without_access_key_credentials(self):
        config = {
            'bucket': 'my-bucket',
            'aws_session_token': 'token-only',
        }
        with self.assertRaisesRegex(ValueError, 'Incomplete access key config'):
            aws_auth.get_auth_mode(config)

    def test_ignores_empty_optional_session_token(self):
        config = {
            'bucket': 'my-bucket',
            'aws_session_token': '',
        }
        self.assertEqual(aws_auth.get_auth_mode(config), AwsAuthMode.DEFAULT)

    def test_treats_empty_strings_as_missing(self):
        config = {
            'bucket': 'my-bucket',
            'aws_access_key_id': 'AKIAEXAMPLE',
            'aws_secret_access_key': '',
        }
        with self.assertRaisesRegex(ValueError, 'Incomplete access key config'):
            aws_auth.get_auth_mode(config)

    def test_rejects_blank_role_config(self):
        config = {
            'bucket': 'my-bucket',
            'account_id': '',
            'role_name': '',
            'external_id': '',
        }
        with self.assertRaisesRegex(ValueError, 'Incomplete role assumption config'):
            aws_auth.get_auth_mode(config)

    def test_get_required_config_keys(self):
        self.assertEqual(
            aws_auth.get_required_config_keys(AwsAuthMode.DEFAULT),
            ['bucket'],
        )
        self.assertEqual(
            aws_auth.get_required_config_keys(AwsAuthMode.ROLE),
            ['bucket', 'account_id', 'role_name', 'external_id'],
        )
        self.assertEqual(
            aws_auth.get_required_config_keys(AwsAuthMode.ACCESS_KEY),
            ['bucket', 'aws_access_key_id', 'aws_secret_access_key'],
        )


class TestExplicitAuthMethod(unittest.TestCase):

    def test_access_key_auth_method_matches_s3_credentials_type(self):
        self.assertEqual(AUTH_METHOD_ACCESS_KEY, 's3Credentials')

    def test_explicit_access_key_wins_over_legacy_role_fields(self):
        config = {
            'auth_method': AUTH_METHOD_ACCESS_KEY,
            'aws_access_key_id': 'AKIAEXAMPLE',
            'aws_secret_access_key': 'secret',
            'external_id': 'legacy-external-id',
        }
        self.assertEqual(aws_auth.get_auth_mode(config), AwsAuthMode.ACCESS_KEY)

    def test_explicit_role_wins_over_access_key_fields(self):
        config = {
            'auth_method': AUTH_METHOD_ROLE,
            'account_id': '111222333444',
            'role_name': 'my-role',
            'external_id': 'external-id',
            'aws_access_key_id': 'AKIAEXAMPLE',
            'aws_secret_access_key': 'secret',
        }
        self.assertEqual(aws_auth.get_auth_mode(config), AwsAuthMode.ROLE)

    def test_explicit_access_key_requires_credentials(self):
        config = {
            'auth_method': AUTH_METHOD_ACCESS_KEY,
            'external_id': 'legacy-external-id',
        }
        with self.assertRaisesRegex(ValueError, 'Incomplete access key config'):
            aws_auth.get_auth_mode(config)

    def test_explicit_role_requires_role_credentials(self):
        config = {
            'auth_method': AUTH_METHOD_ROLE,
            'external_id': 'external-id',
        }
        with self.assertRaisesRegex(ValueError, 'Incomplete role assumption config'):
            aws_auth.get_auth_mode(config)

    def test_invalid_auth_method_raises(self):
        config = {'auth_method': 'invalid'}
        with self.assertRaises(ValueError):
            aws_auth.get_auth_mode(config)


class TestAuthRouting(unittest.TestCase):

    @mock.patch('tap_s3_csv.do_discover')
    @mock.patch('tap_s3_csv.dialect.detect_tables_dialect')
    @mock.patch('tap_s3_csv.s3.list_files_in_bucket')
    @mock.patch('tap_s3_csv.s3.setup_external_source_with_aws_role_assumption')
    @mock.patch('tap_s3_csv.s3.setup_external_source_with_aws_access_key')
    @mock.patch('singer.utils.parse_args')
    def test_default_uses_ambient_credentials(
            self, mock_parse_args, mock_setup_access_key, mock_setup_role,
            mock_list_files, mock_detect_dialect, mock_do_discover):
        config = {
            'bucket': 'my-bucket',
            'tables': _sample_tables(),
        }
        mock_parse_args.side_effect = _make_parse_args_side_effect(config)
        mock_list_files.return_value = iter([{'Key': 'file.csv'}])

        tap_s3_csv.main()

        mock_setup_access_key.assert_not_called()
        mock_setup_role.assert_not_called()
        mock_list_files.assert_called_once_with('my-bucket')
        mock_detect_dialect.assert_called_once_with(config)
        mock_do_discover.assert_called_once()

    @mock.patch('tap_s3_csv.do_discover')
    @mock.patch('tap_s3_csv.s3.setup_external_source_with_aws_role_assumption')
    @mock.patch('tap_s3_csv.s3.setup_external_source_with_aws_access_key')
    @mock.patch('singer.utils.parse_args')
    def test_field_based_role_config_uses_role_client(
            self, mock_parse_args, mock_setup_access_key, mock_setup_role,
            mock_do_discover):
        config = {
            'bucket': 'my-bucket',
            'account_id': '111222333444',
            'role_name': 'my-role',
            'external_id': 'external-id',
            'tables': _sample_tables(),
        }
        mock_parse_args.side_effect = _make_parse_args_side_effect(config)

        tap_s3_csv.main()

        mock_setup_access_key.assert_not_called()
        mock_setup_role.assert_called_once_with(config)
        mock_do_discover.assert_called_once()

    @mock.patch('tap_s3_csv.do_discover')
    @mock.patch('tap_s3_csv.s3.setup_external_source_with_aws_role_assumption')
    @mock.patch('tap_s3_csv.s3.setup_external_source_with_aws_access_key')
    @mock.patch('singer.utils.parse_args')
    def test_explicit_role_assumption_uses_role_client(
            self, mock_parse_args, mock_setup_access_key, mock_setup_role,
            mock_do_discover):
        config = {
            'bucket': 'my-bucket',
            'auth_method': AUTH_METHOD_ROLE,
            'account_id': '111222333444',
            'role_name': 'my-role',
            'external_id': 'external-id',
            'tables': _sample_tables(),
        }
        mock_parse_args.side_effect = _make_parse_args_side_effect(config)

        tap_s3_csv.main()

        mock_setup_access_key.assert_not_called()
        mock_setup_role.assert_called_once_with(config)
        mock_do_discover.assert_called_once()

    @mock.patch('tap_s3_csv.do_discover')
    @mock.patch('tap_s3_csv.s3.setup_external_source_with_aws_role_assumption')
    @mock.patch('tap_s3_csv.s3.setup_external_source_with_aws_access_key')
    @mock.patch('singer.utils.parse_args')
    def test_explicit_access_key_routing(
            self, mock_parse_args, mock_setup_access_key, mock_setup_role,
            mock_do_discover):
        config = {
            'bucket': 'my-bucket',
            'auth_method': AUTH_METHOD_ACCESS_KEY,
            'aws_access_key_id': 'AKIAEXAMPLE',
            'aws_secret_access_key': 'secret',
            'tables': _sample_tables(),
        }
        mock_parse_args.side_effect = _make_parse_args_side_effect(config)

        tap_s3_csv.main()

        mock_setup_role.assert_not_called()
        mock_setup_access_key.assert_called_once_with(config)
        mock_do_discover.assert_called_once()

    @mock.patch('tap_s3_csv.do_discover')
    @mock.patch('tap_s3_csv.s3.setup_external_source_with_aws_role_assumption')
    @mock.patch('tap_s3_csv.s3.setup_external_source_with_aws_access_key')
    @mock.patch('singer.utils.parse_args')
    def test_field_based_access_key_config_uses_access_key_client(
            self, mock_parse_args, mock_setup_access_key, mock_setup_role,
            mock_do_discover):
        config = {
            'bucket': 'my-bucket',
            'aws_access_key_id': 'AKIAEXAMPLE',
            'aws_secret_access_key': 'secret',
            'tables': _sample_tables(),
        }
        mock_parse_args.side_effect = _make_parse_args_side_effect(config)

        tap_s3_csv.main()

        mock_setup_role.assert_not_called()
        mock_setup_access_key.assert_called_once_with(config)
        mock_do_discover.assert_called_once()


class TestSetupExternalSourceWithAwsAccessKey(unittest.TestCase):

    @mock.patch('tap_s3_csv.s3.boto3.setup_default_session')
    def test_sets_up_session_with_required_credentials(self, mock_setup_session):
        config = {
            'aws_access_key_id': 'AKIAEXAMPLE',
            'aws_secret_access_key': 'secret',
        }
        s3.setup_external_source_with_aws_access_key(config)
        mock_setup_session.assert_called_once_with(
            aws_access_key_id='AKIAEXAMPLE',
            aws_secret_access_key='secret',
        )

    @mock.patch('tap_s3_csv.s3.boto3.setup_default_session')
    def test_sets_up_session_with_session_token(self, mock_setup_session):
        config = {
            'aws_access_key_id': 'AKIAEXAMPLE',
            'aws_secret_access_key': 'secret',
            'aws_session_token': 'token',
        }
        s3.setup_external_source_with_aws_access_key(config)
        mock_setup_session.assert_called_once_with(
            aws_access_key_id='AKIAEXAMPLE',
            aws_secret_access_key='secret',
            aws_session_token='token',
        )


class TestSetupExternalSourceWithAwsRoleAssumption(unittest.TestCase):

    @mock.patch('tap_s3_csv.s3.boto3.setup_default_session')
    @mock.patch('tap_s3_csv.s3.AssumeRoleCredentialFetcher')
    @mock.patch('tap_s3_csv.s3.Session')
    def test_assumes_customer_role(self, mock_session, mock_fetcher, mock_setup_session):
        config = {
            'account_id': '111222333444',
            'role_name': 'my-role',
            'external_id': 'external-id',
        }
        mock_session.return_value.create_client = mock.Mock()
        mock_session.return_value.get_credentials = mock.Mock(return_value='creds')

        s3.setup_external_source_with_aws_role_assumption(config)

        mock_fetcher.assert_called_once_with(
            mock_session.return_value.create_client,
            'creds',
            'arn:aws:iam::111222333444:role/my-role',
            extra_args={
                'DurationSeconds': 3600,
                'RoleSessionName': 'TapS3CSV',
                'ExternalId': 'external-id',
            },
            cache=mock.ANY,
        )
        mock_setup_session.assert_called_once()


class TestBuildSymonExceptionFromClientError(unittest.TestCase):

    def _client_error(self, code, message='denied'):
        return ClientError(
            {'Error': {'Code': code, 'Message': message}},
            'ListObjectsV2',
        )

    def test_maps_access_denied_to_symon_exception(self):
        error = self._client_error('AccessDenied')
        result = s3.build_symon_exception_from_client_error(error, 'my-bucket')
        self.assertIsInstance(result, SymonException)
        self.assertEqual(result.code, 'amazonS3.accessDeniedError')
        self.assertIn('my-bucket', str(result))

    def test_maps_access_denied_variants_to_symon_exception(self):
        for aws_code in ('AccessDenied', 'AccessDeniedException', 'KMSAccessDeniedException'):
            with self.subTest(aws_code=aws_code):
                error = self._client_error(aws_code)
                result = s3.build_symon_exception_from_client_error(error, 'my-bucket')
                self.assertIsInstance(result, SymonException)
                self.assertEqual(result.code, 'amazonS3.accessDeniedError')

    def test_access_denied_without_bucket_omits_bucket_in_message(self):
        error = self._client_error('AccessDenied')
        result = s3.build_symon_exception_from_client_error(error)
        self.assertEqual(result.code, 'amazonS3.accessDeniedError')
        self.assertNotIn('"', str(result))

    def test_returns_other_client_errors_unchanged(self):
        for aws_code in ('NoSuchBucket', 'NotFound', 'ExpiredToken', 'SlowDown'):
            with self.subTest(aws_code=aws_code):
                error = self._client_error(aws_code)
                result = s3.build_symon_exception_from_client_error(error, 'my-bucket')
                self.assertIs(result, error)


class TestS3ClientErrorTranslationBoundaries(unittest.TestCase):

    def _client_error(self, code, message='raw aws message'):
        return ClientError(
            {'Error': {'Code': code, 'Message': message}},
            'ListObjectsV2',
        )

    @mock.patch('tap_s3_csv.s3.boto3.client')
    def test_list_files_in_bucket_leaves_client_error_for_retry(self, mock_boto_client):
        aws_error = self._client_error('AccessDenied')
        mock_boto_client.return_value.get_paginator.return_value.paginate.side_effect = aws_error

        with self.assertRaises(ClientError) as ctx:
            list(s3.list_files_in_bucket('customer-bucket'))

        self.assertIs(ctx.exception, aws_error)

    @mock.patch('backoff._sync.time.sleep')
    @mock.patch('tap_s3_csv.s3.boto3.resource')
    def test_get_file_handle_retries_transient_slow_down(
            self, mock_boto_resource, mock_sleep):
        aws_error = self._client_error('SlowDown')
        get_object = (
            mock_boto_resource.return_value.Bucket.return_value.Object.return_value.get
        )
        get_object.side_effect = aws_error

        with self.assertRaises(ClientError):
            s3.get_file_handle({'bucket': 'bucket'}, 'missing.csv')

        self.assertEqual(get_object.call_count, 5)

    @mock.patch('backoff._sync.time.sleep')
    @mock.patch('tap_s3_csv.s3.boto3.resource')
    def test_get_file_handle_leaves_client_error_untranslated(
            self, mock_boto_resource, mock_sleep):
        aws_error = self._client_error('NoSuchBucket')
        mock_boto_resource.return_value.Bucket.return_value.Object.return_value.get.side_effect = aws_error
        config = {'bucket': 'customer-bucket'}

        with self.assertRaises(ClientError) as ctx:
            s3.get_file_handle(config, 'missing.csv')

        self.assertIs(ctx.exception, aws_error)

    @mock.patch('tap_s3_csv.do_discover')
    @mock.patch('tap_s3_csv.s3.setup_external_source_with_aws_access_key')
    @mock.patch('singer.utils.parse_args')
    def test_external_source_translates_access_denied_at_main_boundary(
            self, mock_parse_args, mock_setup_access_key, mock_do_discover):
        config = {
            'bucket': 'customer-bucket',
            'auth_method': AUTH_METHOD_ACCESS_KEY,
            'aws_access_key_id': 'AKIAEXAMPLE',
            'aws_secret_access_key': 'secret',
            'tables': _sample_tables(),
        }
        mock_parse_args.side_effect = _make_parse_args_side_effect(config)
        aws_error = self._client_error('AccessDenied')
        mock_do_discover.side_effect = aws_error

        with self.assertRaises(SymonException) as ctx:
            tap_s3_csv.main()

        self.assertEqual(ctx.exception.code, 'amazonS3.accessDeniedError')
        self.assertIn('customer-bucket', str(ctx.exception))
        self.assertIs(ctx.exception.__cause__, aws_error)

    @mock.patch('tap_s3_csv.do_discover')
    @mock.patch('tap_s3_csv.s3.setup_external_source_with_aws_access_key')
    @mock.patch('singer.utils.parse_args')
    def test_external_source_leaves_non_access_denied_client_error_untranslated(
            self, mock_parse_args, mock_setup_access_key, mock_do_discover):
        config = {
            'bucket': 'customer-bucket',
            'auth_method': AUTH_METHOD_ACCESS_KEY,
            'aws_access_key_id': 'AKIAEXAMPLE',
            'aws_secret_access_key': 'secret',
            'tables': _sample_tables(),
        }
        mock_parse_args.side_effect = _make_parse_args_side_effect(config)
        aws_error = self._client_error('NoSuchBucket')
        mock_do_discover.side_effect = aws_error

        with self.assertRaises(ClientError) as ctx:
            tap_s3_csv.main()

        self.assertIs(ctx.exception, aws_error)

    @mock.patch('tap_s3_csv.do_discover')
    @mock.patch('tap_s3_csv.s3.setup_external_source_with_aws_access_key')
    @mock.patch('singer.utils.parse_args')
    def test_external_source_does_not_translate_wrapped_client_error(
            self, mock_parse_args, mock_setup_access_key, mock_do_discover):
        config = {
            'bucket': 'customer-bucket',
            'auth_method': AUTH_METHOD_ACCESS_KEY,
            'aws_access_key_id': 'AKIAEXAMPLE',
            'aws_secret_access_key': 'secret',
            'tables': _sample_tables(),
        }
        mock_parse_args.side_effect = _make_parse_args_side_effect(config)
        aws_error = self._client_error('AccessDenied')
        wrapped = RuntimeError('wrapper')
        wrapped.__cause__ = aws_error
        mock_do_discover.side_effect = wrapped

        with self.assertRaises(RuntimeError) as ctx:
            tap_s3_csv.main()

        self.assertIs(ctx.exception, wrapped)

    @mock.patch('tap_s3_csv.do_discover')
    @mock.patch('tap_s3_csv.s3.setup_external_source_with_aws_access_key')
    @mock.patch('singer.utils.parse_args')
    def test_external_source_preserves_file_not_found_error(
            self, mock_parse_args, mock_setup_access_key, mock_do_discover):
        config = {
            'bucket': 'customer-bucket',
            'auth_method': AUTH_METHOD_ACCESS_KEY,
            'aws_access_key_id': 'AKIAEXAMPLE',
            'aws_secret_access_key': 'secret',
            'tables': _sample_tables(),
        }
        mock_parse_args.side_effect = _make_parse_args_side_effect(config)
        file_not_found = SymonException(
            'No files matched the configured key.',
            'amazonS3.FileNotFound',
        )
        mock_do_discover.side_effect = file_not_found

        with self.assertRaises(SymonException) as ctx:
            tap_s3_csv.main()

        self.assertIs(ctx.exception, file_not_found)
        self.assertEqual(ctx.exception.code, 'amazonS3.FileNotFound')

    @mock.patch('tap_s3_csv.do_discover')
    @mock.patch('tap_s3_csv.dialect.detect_tables_dialect')
    @mock.patch('tap_s3_csv.s3.list_files_in_bucket')
    @mock.patch('singer.utils.parse_args')
    def test_internal_source_preserves_client_error_at_main_boundary(
            self, mock_parse_args, mock_list_files, mock_detect_dialect,
            mock_do_discover):
        config = {
            'bucket': 'internal-bucket',
            'tables': _sample_tables(),
        }
        mock_parse_args.side_effect = _make_parse_args_side_effect(config)
        aws_error = self._client_error('AccessDenied')
        mock_list_files.return_value = iter([{'Key': 'file.csv'}])
        mock_do_discover.side_effect = aws_error

        with self.assertRaises(ClientError) as ctx:
            tap_s3_csv.main()

        self.assertIs(ctx.exception, aws_error)
        mock_detect_dialect.assert_called_once_with(config)

    @mock.patch('tap_s3_csv.LOGGER')
    @mock.patch('tap_s3_csv.do_discover')
    @mock.patch('tap_s3_csv.s3.setup_external_source_with_aws_access_key')
    @mock.patch('singer.utils.parse_args')
    def test_tap_error_transport_emits_access_denied_code(
            self, mock_parse_args, mock_setup_access_key,
            mock_do_discover, mock_logger):
        config = {
            'bucket': 'customer-bucket',
            'auth_method': AUTH_METHOD_ACCESS_KEY,
            'aws_access_key_id': 'AKIAEXAMPLE',
            'aws_secret_access_key': 'secret',
            'tables': _sample_tables(),
        }
        mock_parse_args.side_effect = _make_parse_args_side_effect(config)
        mock_do_discover.side_effect = SymonException(
            'Unable to access bucket "customer-bucket". Ensure the policy associated with this connection in your AWS '
            'account grants the appropriate permissions.',
            'amazonS3.accessDeniedError',
        )

        with self.assertRaises(SymonException) as ctx:
            tap_s3_csv.main()

        self.assertEqual(ctx.exception.code, 'amazonS3.accessDeniedError')
        logged_payloads = [
            call.args[0]
            for call in mock_logger.info.call_args_list
            if call.args and call.args[0].startswith(tap_s3_csv.ERROR_START_MARKER)
        ]
        self.assertEqual(len(logged_payloads), 1)
        error_info = json.loads(
            logged_payloads[0][len(tap_s3_csv.ERROR_START_MARKER):-len(tap_s3_csv.ERROR_END_MARKER)]
        )
        self.assertEqual(error_info['code'], 'amazonS3.accessDeniedError')
