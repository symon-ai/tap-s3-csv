import unittest
from unittest import mock

import tap_s3_csv

TABLES = [{
    'table_name': 'my_table',
    'search_pattern': '.*\\.csv',
    'key_properties': [],
}]

INTERNAL_CONFIG = {
    'bucket': 'internal-bucket',
    'tables': TABLES,
}

ROLE_CONFIG = {
    'bucket': 'customer-bucket',
    'auth_method': tap_s3_csv.AUTH_METHOD_ROLE,
    'account_id': '111222333444',
    'role_name': 'customer-role',
    'external_id': 'external-id',
    'tables': TABLES,
}

ACCESS_KEY_CONFIG = {
    'bucket': 'customer-bucket',
    'auth_method': tap_s3_csv.AUTH_METHOD_ACCESS_KEY,
    'aws_access_key_id': 'AKIAEXAMPLE',
    'aws_secret_access_key': 'secret',
    'tables': TABLES,
}


def _args(config):
    return mock.Mock(
        config=config,
        discover=True,
        properties=None,
        state={},
    )


class TestAuthRouting(unittest.TestCase):

    @mock.patch('tap_s3_csv.do_discover')
    @mock.patch('tap_s3_csv.dialect.detect_tables_dialect')
    @mock.patch('tap_s3_csv.s3.list_files_in_bucket')
    @mock.patch('tap_s3_csv.s3.setup_external_source_with_aws_access_key')
    @mock.patch('tap_s3_csv.s3.setup_external_source_with_aws_role_assumption')
    @mock.patch('singer.utils.parse_args')
    def test_no_auth_method_uses_ambient_credentials(
            self, mock_parse_args, mock_setup_role, mock_setup_access_key,
            mock_list_files, mock_detect_dialect, mock_do_discover):
        mock_parse_args.return_value = _args(INTERNAL_CONFIG)
        mock_list_files.return_value = iter(())

        tap_s3_csv.main()

        mock_setup_role.assert_not_called()
        mock_setup_access_key.assert_not_called()
        mock_do_discover.assert_called()

    @mock.patch('tap_s3_csv.do_discover')
    @mock.patch('tap_s3_csv.dialect.detect_tables_dialect')
    @mock.patch('tap_s3_csv.s3.list_files_in_bucket')
    @mock.patch('tap_s3_csv.s3.setup_external_source_with_aws_access_key')
    @mock.patch('tap_s3_csv.s3.setup_external_source_with_aws_role_assumption')
    @mock.patch('singer.utils.parse_args')
    def test_credentials_without_auth_method_use_ambient_credentials(
            self, mock_parse_args, mock_setup_role, mock_setup_access_key,
            mock_list_files, mock_detect_dialect, mock_do_discover):
        config = {
            **INTERNAL_CONFIG,
            'account_id': '111222333444',
            'role_name': 'customer-role',
            'external_id': 'external-id',
            'aws_access_key_id': 'AKIAEXAMPLE',
            'aws_secret_access_key': 'secret',
        }
        mock_parse_args.return_value = _args(config)
        mock_list_files.return_value = iter(())

        tap_s3_csv.main()

        mock_setup_role.assert_not_called()
        mock_setup_access_key.assert_not_called()
        mock_do_discover.assert_called()

    @mock.patch('tap_s3_csv.do_discover')
    @mock.patch('tap_s3_csv.s3.setup_external_source_with_aws_access_key')
    @mock.patch('tap_s3_csv.s3.setup_external_source_with_aws_role_assumption')
    @mock.patch('singer.utils.parse_args')
    def test_explicit_role_auth_selects_role_authentication(
            self, mock_parse_args, mock_setup_role, mock_setup_access_key,
            mock_do_discover):
        mock_parse_args.return_value = _args(ROLE_CONFIG)

        tap_s3_csv.main()

        mock_setup_role.assert_called()
        mock_setup_access_key.assert_not_called()
        mock_do_discover.assert_called()

    @mock.patch('tap_s3_csv.do_discover')
    @mock.patch('tap_s3_csv.s3.setup_external_source_with_aws_access_key')
    @mock.patch('tap_s3_csv.s3.setup_external_source_with_aws_role_assumption')
    @mock.patch('singer.utils.parse_args')
    def test_explicit_access_key_auth_selects_access_key_authentication(
            self, mock_parse_args, mock_setup_role, mock_setup_access_key,
            mock_do_discover):
        mock_parse_args.return_value = _args(ACCESS_KEY_CONFIG)

        tap_s3_csv.main()

        mock_setup_access_key.assert_called()
        mock_setup_role.assert_not_called()
        mock_do_discover.assert_called()

    @mock.patch('tap_s3_csv.do_discover')
    @mock.patch('tap_s3_csv.s3.setup_external_source_with_aws_access_key')
    @mock.patch('tap_s3_csv.s3.setup_external_source_with_aws_role_assumption')
    @mock.patch('singer.utils.parse_args')
    def test_unknown_auth_method_is_rejected(
            self, mock_parse_args, mock_setup_role, mock_setup_access_key,
            mock_do_discover):
        config = {
            **INTERNAL_CONFIG,
            'auth_method': 'invalidAuthMethod',
        }
        mock_parse_args.return_value = _args(config)

        with self.assertRaisesRegex(ValueError, 'valid S3 auth method'):
            tap_s3_csv.main()

        mock_setup_role.assert_not_called()
        mock_setup_access_key.assert_not_called()
        mock_do_discover.assert_not_called()
