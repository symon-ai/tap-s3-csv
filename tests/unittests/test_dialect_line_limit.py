import unittest
from unittest import mock

import tap_s3_csv


class TestDialectLineLimit(unittest.TestCase):
    @mock.patch('tap_s3_csv.do_discover')
    @mock.patch('tap_s3_csv.s3.get_file_handle')
    @mock.patch('tap_s3_csv.s3.get_input_files_for_table')
    @mock.patch('tap_s3_csv.s3.list_files_in_bucket')
    @mock.patch('singer.utils.parse_args')
    def test_internal_csv_physical_line_boundaries(
            self, parse_args, list_files, input_files, get_file_handle, do_discover):
        list_files.return_value = iter(())
        input_files.return_value = [{'key': 'rules.csv'}]

        mib = 1024 ** 2
        cases = (
            (None, mib - 1, True),
            (None, mib, False),
            (None, mib + 1, False),
            (False, mib, False),
            (True, mib - 1, True),
            (True, mib, True),
            (True, mib + 1, True),
            (True, 2 * mib, True),
            (True, 2 * mib + 1, False),
        )
        for opt_in, line_size, accepted in cases:
            with self.subTest(opt_in=opt_in, line_size=line_size):
                table = {
                    'table_name': 'rule_assignments',
                    'search_pattern': r'.*\.csv',
                    'key_properties': [],
                    'encoding': 'utf-8',
                    'delimiter': '',
                    'quotechar': '"',
                }
                config = {'bucket': 'internal-bucket', 'tables': [table]}
                if opt_in is not None:
                    config['allow_2mb_csv_lines'] = opt_in
                parse_args.return_value = mock.Mock(
                    config=config, discover=True, properties=None, state={})
                prefix = b'value,'
                row = prefix + b'x' * (line_size - len(prefix))
                get_file_handle.return_value.iter_lines.return_value = iter((b'name,value', row))

                if accepted:
                    tap_s3_csv.main()
                    self.assertEqual(config['tables'][0]['delimiter'], ',')
                else:
                    with self.assertRaisesRegex(Exception, '^Too many bytes in one line$'):
                        tap_s3_csv.main()
