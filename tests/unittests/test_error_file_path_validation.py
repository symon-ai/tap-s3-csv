import os
import unittest

from tap_s3_csv import _resolve_confined_error_file_path


class TestErrorFilePathValidation(unittest.TestCase):
    """WP-33422: error_file_path is user-supplied config passed to open() for
    writing. It must be validated/confined to prevent path traversal /
    arbitrary file write (CWE-73)."""

    def setUp(self):
        self.base_dir = os.path.realpath(os.getcwd())

    def test_accepts_normal_relative_path(self):
        result = _resolve_confined_error_file_path('error.json', base_dir=self.base_dir)
        self.assertEqual(result, os.path.join(self.base_dir, 'error.json'))

    def test_accepts_relative_subdirectory_path(self):
        result = _resolve_confined_error_file_path('sub/error.json', base_dir=self.base_dir)
        self.assertEqual(result, os.path.join(self.base_dir, 'sub', 'error.json'))

    def test_rejects_parent_directory_traversal(self):
        result = _resolve_confined_error_file_path('../../etc/passwd', base_dir=self.base_dir)
        self.assertIsNone(result)

    def test_rejects_absolute_path(self):
        result = _resolve_confined_error_file_path('/etc/passwd', base_dir=self.base_dir)
        self.assertIsNone(result)

    def test_rejects_none_and_empty(self):
        self.assertIsNone(_resolve_confined_error_file_path(None, base_dir=self.base_dir))
        self.assertIsNone(_resolve_confined_error_file_path('', base_dir=self.base_dir))

    def test_rejects_non_string(self):
        self.assertIsNone(_resolve_confined_error_file_path(123, base_dir=self.base_dir))


if __name__ == '__main__':
    unittest.main()
