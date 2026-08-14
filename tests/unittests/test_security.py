import gzip
import io
import json
import os
import tempfile
import unittest
from unittest import mock

import tap_s3_csv
from tap_s3_csv import dialect, utils


class TestSecurityRemediation(unittest.TestCase):

    def test_rejects_path_in_gzip_header_filename(self):
        payload = io.BytesIO()
        with gzip.GzipFile(filename='safe.csv', mode='wb', fileobj=payload) as gz_file:
            gz_file.write(b'column\nvalue\n')
        unsafe_payload = payload.getvalue().replace(b'safe.csv\x00', b'../escaped.csv\x00', 1)

        with self.assertRaisesRegex(ValueError, 'unsafe file name'):
            utils.get_file_name_from_gzfile(fileobj=io.BytesIO(unsafe_payload))

    def test_writes_to_absolute_working_directory_error_path(self):
        with tempfile.TemporaryDirectory() as temp_working_dir:
            error_path = os.path.join(temp_working_dir, tap_s3_csv.ERROR_FILE_NAME)
            tap_s3_csv._write_error_file(error_path, {'message': 'failed'})
            with open(error_path, encoding='utf-8') as error_file:
                self.assertEqual({'message': 'failed'}, json.load(error_file))

    def test_rejects_error_path_traversal(self):
        with tempfile.TemporaryDirectory() as temp_directory:
            temp_working_dir = os.path.join(temp_directory, 'working')
            os.mkdir(temp_working_dir)
            outside_path = os.path.join(temp_directory, tap_s3_csv.ERROR_FILE_NAME)
            traversal_path = os.path.join(
                temp_working_dir, '..', tap_s3_csv.ERROR_FILE_NAME)
            with self.assertRaisesRegex(ValueError, 'Invalid error_file_path'):
                tap_s3_csv._write_error_file(traversal_path, {})
            self.assertFalse(os.path.exists(outside_path))

    def test_rejects_error_path_symlink_escape(self):
        with tempfile.TemporaryDirectory() as temp_directory:
            temp_working_dir = os.path.join(temp_directory, 'working')
            outside_dir = os.path.join(temp_directory, 'outside')
            os.mkdir(temp_working_dir)
            os.mkdir(outside_dir)
            symlink_dir = os.path.join(temp_working_dir, 'linked')
            os.symlink(outside_dir, symlink_dir)
            outside_path = os.path.join(outside_dir, tap_s3_csv.ERROR_FILE_NAME)

            with self.assertRaisesRegex(ValueError, 'Invalid error_file_path'):
                tap_s3_csv._write_error_file(
                    os.path.join(symlink_dir, tap_s3_csv.ERROR_FILE_NAME), {})
            self.assertFalse(os.path.exists(outside_path))

    @mock.patch('tap_s3_csv.dialect.chardet.UniversalDetector')
    @mock.patch('tap_s3_csv.dialect.preprocess.PreprocessStream')
    @mock.patch('tap_s3_csv.dialect.s3.get_file_handle')
    def test_encoding_samples_are_deterministic(self, get_file_handle, preprocess_stream, detector):
        lines = [b'header'] + [b'plain'] * 499
        get_file_handle.return_value = io.BytesIO(b'')
        preprocess_stream.return_value.iter_lines.return_value = iter(lines)
        detector.return_value.result = {'encoding': 'utf-8', 'confidence': 1.0}
        detector.return_value.done = False

        table = {'delimiter': ',', 'quotechar': '"'}
        dialect.detect_dialect({}, {'key': 'sample.csv'}, table)

        fed_lines = [call.args[0] for call in detector.return_value.feed.call_args_list]
        self.assertEqual(400, len(fed_lines))
        self.assertEqual(b'plain', fed_lines[-1])


if __name__ == '__main__':
    unittest.main()
