import gzip
import io
import json
import os
import tempfile
import unittest
from unittest import mock

import tap_s3_csv
from tap_s3_csv import dialect, s3, sync, utils


def unsafe_gzip_payload():
    payload = io.BytesIO()
    with gzip.GzipFile(filename='safe.csv', mode='wb', fileobj=payload) as gz_file:
        gz_file.write(b'column\nvalue\n')
    return payload.getvalue().replace(b'safe.csv\x00', b'../escaped.csv\x00', 1)


class TestSecurityRemediation(unittest.TestCase):

    def test_rejects_path_in_gzip_header_filename(self):
        with self.assertRaisesRegex(ValueError, 'unsafe file name'):
            utils.get_file_name_from_gzfile(fileobj=io.BytesIO(unsafe_gzip_payload()))

    def test_sampling_rejects_path_in_gzip_header_filename(self):
        with self.assertRaisesRegex(ValueError, 'unsafe file name'):
            s3.sampling_gz_file({}, 'unsafe.gz', io.BytesIO(unsafe_gzip_payload()), 5)

    def test_sync_rejects_path_in_gzip_header_filename(self):
        with self.assertRaisesRegex(ValueError, 'unsafe file name'):
            sync.sync_gz_file({}, 'unsafe.gz', {}, {}, io.BytesIO(unsafe_gzip_payload()))

    def test_writes_actual_app_relative_error_path_under_root(self):
        with tempfile.TemporaryDirectory() as trusted_root:
            relative_dir = os.path.join('export', 'org', 'task')
            os.makedirs(os.path.join(trusted_root, relative_dir))
            relative_path = os.path.join(relative_dir, tap_s3_csv.ERROR_FILE_NAME)

            tap_s3_csv._write_error_file(
                relative_path, trusted_root, {'message': 'failed'})

            with open(os.path.join(trusted_root, relative_path), encoding='utf-8') as error_file:
                self.assertEqual({'message': 'failed'}, json.load(error_file))

    def test_writes_absolute_error_path_under_root(self):
        with tempfile.TemporaryDirectory() as trusted_root:
            error_path = os.path.join(trusted_root, tap_s3_csv.ERROR_FILE_NAME)
            tap_s3_csv._write_error_file(
                error_path, trusted_root, {'message': 'failed'})
            with open(error_path, encoding='utf-8') as error_file:
                self.assertEqual({'message': 'failed'}, json.load(error_file))

    def test_rejects_outside_sibling_error_path(self):
        with tempfile.TemporaryDirectory() as temp_directory:
            trusted_root = os.path.join(temp_directory, 'trusted')
            outside_dir = os.path.join(temp_directory, 'outside')
            os.mkdir(trusted_root)
            os.mkdir(outside_dir)
            outside_path = os.path.join(outside_dir, tap_s3_csv.ERROR_FILE_NAME)

            with self.assertRaisesRegex(ValueError, 'Invalid error_file_path'):
                tap_s3_csv._write_error_file(outside_path, trusted_root, {})
            self.assertFalse(os.path.exists(outside_path))

    def test_rejects_error_path_traversal(self):
        with tempfile.TemporaryDirectory() as temp_directory:
            trusted_root = os.path.join(temp_directory, 'trusted')
            os.mkdir(trusted_root)
            outside_path = os.path.join(temp_directory, tap_s3_csv.ERROR_FILE_NAME)
            traversal_path = os.path.join('working', '..', '..', tap_s3_csv.ERROR_FILE_NAME)
            with self.assertRaisesRegex(ValueError, 'Invalid error_file_path'):
                tap_s3_csv._write_error_file(traversal_path, trusted_root, {})
            self.assertFalse(os.path.exists(outside_path))

    def test_rejects_error_path_symlink_escape(self):
        with tempfile.TemporaryDirectory() as temp_directory:
            trusted_root = os.path.join(temp_directory, 'trusted')
            outside_dir = os.path.join(temp_directory, 'outside')
            os.mkdir(trusted_root)
            os.mkdir(outside_dir)
            symlink_dir = os.path.join(trusted_root, 'linked')
            os.symlink(outside_dir, symlink_dir)
            outside_path = os.path.join(outside_dir, tap_s3_csv.ERROR_FILE_NAME)

            with self.assertRaisesRegex(ValueError, 'Invalid error_file_path'):
                tap_s3_csv._write_error_file(
                    os.path.join('linked', tap_s3_csv.ERROR_FILE_NAME), trusted_root, {})
            self.assertFalse(os.path.exists(outside_path))

    @mock.patch('tap_s3_csv.LOGGER.warning')
    def test_logs_failed_error_file_write(self, warning):
        with tempfile.TemporaryDirectory() as trusted_root:
            tap_s3_csv._try_write_error_file(
                os.path.join('..', tap_s3_csv.ERROR_FILE_NAME), trusted_root, {})

        warning.assert_called_once()
        self.assertIn('Failed to write tap error file', warning.call_args.args[0])

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
