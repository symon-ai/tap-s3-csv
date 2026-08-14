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

    def test_writes_only_to_expected_error_filename(self):
        with tempfile.TemporaryDirectory(dir='.') as directory:
            error_path = os.path.relpath(os.path.join(directory, tap_s3_csv.ERROR_FILE_NAME))
            tap_s3_csv._write_error_file(error_path, {'message': 'failed'})
            with open(error_path, encoding='utf-8') as error_file:
                self.assertEqual({'message': 'failed'}, json.load(error_file))

            with self.assertRaisesRegex(ValueError, 'Invalid error_file_path'):
                tap_s3_csv._write_error_file(os.path.join(directory, 'arbitrary.json'), {})
            with self.assertRaisesRegex(ValueError, 'Invalid error_file_path'):
                tap_s3_csv._write_error_file('/tmp/tapError.json', {})

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
