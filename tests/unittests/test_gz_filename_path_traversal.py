import io
import unittest
from unittest import mock

from tap_s3_csv import s3
from tap_s3_csv import utils


class TestSanitizeGzFileName(unittest.TestCase):
    """WP-33417 / CWE-73: the original filename embedded in a gzip header is
    user-supplied and must be sanitized before being used to build a path."""

    def test_normal_filename_is_preserved(self):
        self.assertEqual("data.csv", utils.sanitize_gz_file_name("data.csv"))

    def test_posix_traversal_is_neutralized(self):
        self.assertEqual(
            "passwd", utils.sanitize_gz_file_name("../../../../etc/passwd"))

    def test_windows_traversal_is_neutralized(self):
        self.assertEqual(
            "system.ini",
            utils.sanitize_gz_file_name("..\\..\\windows\\system.ini"))

    def test_absolute_path_is_reduced_to_basename(self):
        self.assertEqual(
            "secret.csv", utils.sanitize_gz_file_name("/var/lib/secret.csv"))

    def test_bare_traversal_token_is_rejected(self):
        self.assertEqual("", utils.sanitize_gz_file_name(".."))

    def test_empty_input_is_passed_through(self):
        self.assertEqual(None, utils.sanitize_gz_file_name(None))


class TestSamplingGzFilePathTraversal(unittest.TestCase):
    """The path handed downstream from sampling_gz_file must not contain the
    attacker-supplied traversal segments."""

    @mock.patch("tap_s3_csv.s3.sample_file")
    @mock.patch("tap_s3_csv.utils.get_file_name_from_gzfile")
    def test_traversal_filename_is_sanitized_before_building_path(
            self, mocked_gz_file_name, mocked_sample_file):
        mocked_gz_file_name.return_value = "../../../../etc/passwd.csv"
        mocked_sample_file.return_value = []

        table_spec = {}
        s3_path = "unittest_compressed_files/sample.gz"
        # minimal valid gzip stream so gzip.GzipFile(...) does not raise
        import gzip
        raw = io.BytesIO()
        with gzip.GzipFile(fileobj=raw, mode="wb") as gz:
            gz.write(b"col\n1\n")
        file_handle = io.BytesIO(raw.getvalue())

        s3.sampling_gz_file(table_spec, s3_path, file_handle, 5)

        # sample_file is invoked with the constructed downstream path as its
        # second positional argument.
        self.assertTrue(mocked_sample_file.called)
        built_path = mocked_sample_file.call_args[0][1]
        self.assertEqual(
            "unittest_compressed_files/sample.gz/passwd.csv", built_path)
        self.assertNotIn("..", built_path)


if __name__ == "__main__":
    unittest.main()
