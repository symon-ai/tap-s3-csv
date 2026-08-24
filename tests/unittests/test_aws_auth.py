import unittest
from unittest import mock

from botocore.exceptions import ClientError

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


class TestResolveAuthMethod(unittest.TestCase):

    def test_explicit_auth_method_wins(self):
        config = {
            'auth_method': 'awsAccessKey',
            'external_id': 'legacy-external-id',
        }
        self.assertEqual(tap_s3_csv._resolve_auth_method(config), 'awsAccessKey')

    def test_legacy_role_when_auth_method_absent_and_external_id_present(self):
        config = {'external_id': 'legacy-external-id'}
        self.assertEqual(tap_s3_csv._resolve_auth_method(config), 'awsRoleAssumption')

    def test_no_auth_preserves_internal_behavior(self):
        config = {'bucket': 'my-bucket'}
        self.assertIsNone(tap_s3_csv._resolve_auth_method(config))

    def test_access_key_fields_without_auth_method_stay_internal(self):
        config = {
            'aws_access_key_id': 'AKIAEXAMPLE',
            'aws_secret_access_key': 'secret',
        }
        self.assertIsNone(tap_s3_csv._resolve_auth_method(config))


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
    def test_legacy_external_id_fallback_uses_role_client(
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
            'auth_method': 'awsRoleAssumption',
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
            'auth_method': 'awsAccessKey',
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
    @mock.patch('tap_s3_csv.dialect.detect_tables_dialect')
    @mock.patch('tap_s3_csv.s3.list_files_in_bucket')
    @mock.patch('tap_s3_csv.s3.setup_external_source_with_aws_role_assumption')
    @mock.patch('tap_s3_csv.s3.setup_external_source_with_aws_access_key')
    @mock.patch('singer.utils.parse_args')
    def test_access_key_fields_without_auth_method_use_default_path(
            self, mock_parse_args, mock_setup_access_key, mock_setup_role,
            mock_list_files, mock_detect_dialect, mock_do_discover):
        config = {
            'bucket': 'my-bucket',
            'aws_access_key_id': 'AKIAEXAMPLE',
            'aws_secret_access_key': 'secret',
            'tables': _sample_tables(),
        }
        mock_parse_args.side_effect = _make_parse_args_side_effect(config)
        mock_list_files.return_value = iter([{'Key': 'file.csv'}])

        tap_s3_csv.main()

        mock_setup_access_key.assert_not_called()
        mock_setup_role.assert_not_called()
        mock_detect_dialect.assert_called_once_with(config)
        mock_do_discover.assert_called_once()


class TestSetupAwsAccessKeyClient(unittest.TestCase):

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


class TestSetupAwsClient(unittest.TestCase):

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

    GENERIC_MESSAGE = 'Amazon S3 request failed.'

    def _client_error(self, code, message='raw aws message', http_status_code=None):
        response = {
            'Error': {'Code': code, 'Message': message},
        }
        if http_status_code is not None:
            response['ResponseMetadata'] = {'HTTPStatusCode': http_status_code}
        return ClientError(
            response,
            'ListObjectsV2',
        )

    def _assert_mapped(self, aws_code, singer_code, bucket='my-bucket', aws_message='raw aws message'):
        error = self._client_error(aws_code, aws_message)
        result = s3.build_symon_exception_from_client_error(error, bucket)
        self.assertIsInstance(result, SymonException)
        self.assertEqual(result.code, singer_code)
        self.assertEqual(str(result), self.GENERIC_MESSAGE)
        self.assertNotIn(aws_message, str(result))
        self.assertNotIn(aws_code, str(result))

    def test_maps_expired_token(self):
        self._assert_mapped('ExpiredToken', 'amazonS3.expiredTokenError')

    def test_maps_invalid_credentials(self):
        for aws_code in ('InvalidToken', 'InvalidAccessKeyId', 'SignatureDoesNotMatch'):
            with self.subTest(aws_code=aws_code):
                self._assert_mapped(aws_code, 'amazonS3.invalidCredentialsError')

    def test_maps_access_denied_variants(self):
        for aws_code in ('AccessDenied', 'AccessDeniedException', 'KMSAccessDeniedException'):
            with self.subTest(aws_code=aws_code):
                self._assert_mapped(aws_code, 'amazonS3.requestFailed')

    def test_maps_bucket_not_found(self):
        for aws_code in ('NoSuchBucket', 'NotFound'):
            with self.subTest(aws_code=aws_code):
                self._assert_mapped(aws_code, 'amazonS3.requestFailed')

    def test_maps_incorrect_region(self):
        for aws_code in (
            'PermanentRedirect',
            'AuthorizationHeaderMalformed',
            'IllegalLocationConstraintException',
        ):
            with self.subTest(aws_code=aws_code):
                self._assert_mapped(aws_code, 'amazonS3.requestFailed')

    def test_maps_unnamed_http_errors(self):
        for http_status_code in (301, 403, 404):
            with self.subTest(http_status_code=http_status_code):
                error = self._client_error(str(http_status_code), http_status_code=http_status_code)
                result = s3.build_symon_exception_from_client_error(error, 'my-bucket')
                self.assertIsInstance(result, SymonException)
                self.assertEqual(result.code, 'amazonS3.requestFailed')
                self.assertEqual(str(result), self.GENERIC_MESSAGE)

    def test_maps_other_client_errors_to_the_safe_fallback(self):
        self._assert_mapped('SlowDown', 'amazonS3.requestFailed')

    @mock.patch('tap_s3_csv.s3.LOGGER')
    def test_logs_original_aws_error_without_exposing_it(self, mock_logger):
        error = self._client_error('ExpiredToken', 'The provided token has expired.')
        s3.build_symon_exception_from_client_error(error, 'my-bucket')

        mock_logger.error.assert_called_once_with(
            'Amazon S3 ClientError (%s): %s',
            'ExpiredToken',
            'The provided token has expired.',
        )


class TestS3ClientErrorTranslationBoundaries(unittest.TestCase):

    def tearDown(self):
        s3.set_translate_s3_client_errors(False)

    def _client_error(self, code, message='raw aws message'):
        return ClientError(
            {'Error': {'Code': code, 'Message': message}},
            'ListObjectsV2',
        )

    @mock.patch('tap_s3_csv.s3.boto3.client')
    def test_list_files_in_bucket_translates_when_enabled(self, mock_boto_client):
        aws_error = self._client_error('AccessDenied')
        mock_boto_client.return_value.get_paginator.return_value.paginate.side_effect = aws_error

        s3.set_translate_s3_client_errors(True)
        with self.assertRaises(SymonException) as ctx:
            list(s3.list_files_in_bucket('customer-bucket'))

        self.assertEqual(ctx.exception.code, 'amazonS3.requestFailed')
        self.assertEqual(str(ctx.exception), 'Amazon S3 request failed.')
        self.assertIs(ctx.exception.__cause__, aws_error)

    @mock.patch('tap_s3_csv.s3.boto3.client')
    def test_list_files_in_bucket_preserves_client_error_when_disabled(self, mock_boto_client):
        aws_error = self._client_error('AccessDenied')
        mock_boto_client.return_value.get_paginator.return_value.paginate.side_effect = aws_error

        with self.assertRaises(ClientError) as ctx:
            list(s3.list_files_in_bucket('internal-bucket'))

        self.assertIs(ctx.exception, aws_error)

    @mock.patch('tap_s3_csv.s3.boto3.resource')
    def test_get_file_handle_translates_when_enabled(self, mock_boto_resource):
        aws_error = self._client_error('NoSuchBucket')
        mock_boto_resource.return_value.Bucket.return_value.Object.return_value.get.side_effect = aws_error
        config = {'bucket': 'customer-bucket'}

        s3.set_translate_s3_client_errors(True)
        with self.assertRaises(SymonException) as ctx:
            s3.get_file_handle(config, 'missing.csv')

        self.assertEqual(ctx.exception.code, 'amazonS3.requestFailed')
        self.assertIs(ctx.exception.__cause__, aws_error)
