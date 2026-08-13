import unittest
from unittest import mock

from botocore.exceptions import ClientError

from tap_s3_csv import aws_auth
from tap_s3_csv.aws_auth import AwsAuthMode
from tap_s3_csv import s3
from tap_s3_csv.symon_exception import SymonException


class TestAwsAuthDetection(unittest.TestCase):

    def test_default_mode_when_no_auth_keys(self):
        config = {'bucket': 'my-bucket'}
        self.assertEqual(aws_auth.detect_auth_mode(config), AwsAuthMode.DEFAULT)

    def test_role_mode_when_all_role_keys_present(self):
        config = {
            'bucket': 'my-bucket',
            'account_id': '111222333444',
            'role_name': 'my-role',
            'external_id': 'external-id',
        }
        self.assertEqual(aws_auth.detect_auth_mode(config), AwsAuthMode.ROLE)

    def test_access_key_mode_when_required_keys_present(self):
        config = {
            'bucket': 'my-bucket',
            'aws_access_key_id': 'AKIAEXAMPLE',
            'aws_secret_access_key': 'secret',
        }
        self.assertEqual(aws_auth.detect_auth_mode(config), AwsAuthMode.ACCESS_KEY)

    def test_access_key_mode_with_session_token(self):
        config = {
            'bucket': 'my-bucket',
            'aws_access_key_id': 'AKIAEXAMPLE',
            'aws_secret_access_key': 'secret',
            'aws_session_token': 'token',
        }
        self.assertEqual(aws_auth.detect_auth_mode(config), AwsAuthMode.ACCESS_KEY)

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
            aws_auth.detect_auth_mode(config)

    def test_rejects_role_config_with_session_token(self):
        config = {
            'bucket': 'my-bucket',
            'account_id': '111222333444',
            'role_name': 'my-role',
            'external_id': 'external-id',
            'aws_session_token': 'token',
        }
        with self.assertRaisesRegex(ValueError, 'not both'):
            aws_auth.detect_auth_mode(config)

    def test_rejects_incomplete_role_config(self):
        config = {
            'bucket': 'my-bucket',
            'external_id': 'external-id',
        }
        with self.assertRaisesRegex(ValueError, 'Incomplete role assumption config'):
            aws_auth.detect_auth_mode(config)

    def test_rejects_incomplete_access_key_config(self):
        config = {
            'bucket': 'my-bucket',
            'aws_access_key_id': 'AKIAEXAMPLE',
        }
        with self.assertRaisesRegex(ValueError, 'Incomplete access key config'):
            aws_auth.detect_auth_mode(config)

    def test_rejects_session_token_without_access_key_credentials(self):
        config = {
            'bucket': 'my-bucket',
            'aws_session_token': 'token-only',
        }
        with self.assertRaisesRegex(ValueError, 'Incomplete access key config'):
            aws_auth.detect_auth_mode(config)

    def test_treats_empty_strings_as_missing(self):
        config = {
            'bucket': 'my-bucket',
            'aws_access_key_id': 'AKIAEXAMPLE',
            'aws_secret_access_key': '',
        }
        with self.assertRaisesRegex(ValueError, 'Incomplete access key config'):
            aws_auth.detect_auth_mode(config)

    def test_rejects_blank_role_config(self):
        config = {
            'bucket': 'my-bucket',
            'account_id': '',
            'role_name': '',
            'external_id': '',
        }
        with self.assertRaisesRegex(ValueError, 'Incomplete role assumption config'):
            aws_auth.detect_auth_mode(config)

    def test_is_external_auth(self):
        self.assertFalse(aws_auth.is_external_auth(AwsAuthMode.DEFAULT))
        self.assertTrue(aws_auth.is_external_auth(AwsAuthMode.ROLE))
        self.assertTrue(aws_auth.is_external_auth(AwsAuthMode.ACCESS_KEY))

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


class TestSetupAwsAccessKeyClient(unittest.TestCase):

    @mock.patch('tap_s3_csv.s3.boto3.setup_default_session')
    def test_sets_up_session_with_required_credentials(self, mock_setup_session):
        config = {
            'aws_access_key_id': 'AKIAEXAMPLE',
            'aws_secret_access_key': 'secret',
        }
        s3.setup_aws_access_key_client(config)
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
        s3.setup_aws_access_key_client(config)
        mock_setup_session.assert_called_once_with(
            aws_access_key_id='AKIAEXAMPLE',
            aws_secret_access_key='secret',
            aws_session_token='token',
        )


class TestBuildSymonExceptionFromClientError(unittest.TestCase):

    def _client_error(self, code):
        return ClientError(
            {'Error': {'Code': code, 'Message': 'denied'}},
            'ListObjectsV2',
        )

    def test_maps_access_denied_to_symon_exception(self):
        error = self._client_error('AccessDenied')
        result = s3.build_symon_exception_from_client_error(error, 'my-bucket')
        self.assertIsInstance(result, SymonException)
        self.assertEqual(result.code, 'amazonS3.accessDeniedError')
        self.assertIn('my-bucket', str(result))

    def test_returns_other_client_errors_unchanged(self):
        error = self._client_error('NoSuchBucket')
        result = s3.build_symon_exception_from_client_error(error, 'my-bucket')
        self.assertIs(result, error)
