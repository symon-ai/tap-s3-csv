import unittest

from tap_s3_csv import utils


class TestSanitizeGzFileName(unittest.TestCase):
    """WP-32445 (CWE-73): the gzip-embedded original filename is attacker-controlled
    and must be reduced to a safe basename before it is used to build a path.
    """

    def test_plain_name_is_preserved(self):
        self.assertEqual(utils.sanitize_gz_file_name("data.csv"), "data.csv")

    def test_relative_traversal_is_stripped_to_basename(self):
        # ../../etc/passwd must not be allowed to escape the intended location.
        self.assertEqual(
            utils.sanitize_gz_file_name("../../etc/passwd"), "passwd")

    def test_absolute_path_is_stripped_to_basename(self):
        self.assertEqual(
            utils.sanitize_gz_file_name("/abs/evil.csv"), "evil.csv")

    def test_leading_slash_is_stripped(self):
        self.assertEqual(utils.sanitize_gz_file_name("/data.csv"), "data.csv")

    def test_embedded_separator_is_stripped(self):
        self.assertEqual(
            utils.sanitize_gz_file_name("nested/dir/data.csv"), "data.csv")

    def test_windows_style_separator_is_stripped(self):
        self.assertEqual(
            utils.sanitize_gz_file_name("..\\..\\windows\\system32\\cmd"), "cmd")

    def test_pure_traversal_component_is_rejected(self):
        self.assertIsNone(utils.sanitize_gz_file_name("../.."))
        self.assertIsNone(utils.sanitize_gz_file_name(".."))

    def test_empty_and_none_are_rejected(self):
        self.assertIsNone(utils.sanitize_gz_file_name(""))
        self.assertIsNone(utils.sanitize_gz_file_name(None))

    def test_only_separators_are_rejected(self):
        self.assertIsNone(utils.sanitize_gz_file_name("/"))
        self.assertIsNone(utils.sanitize_gz_file_name("///"))

    def test_sanitized_name_has_no_separators(self):
        # Property the callers rely on: the result is always a bare component.
        for tainted in ["../../etc/passwd", "/abs/evil", "a/b/c.csv",
                        "..\\..\\x"]:
            result = utils.sanitize_gz_file_name(tainted)
            self.assertIsNotNone(result)
            self.assertNotIn("/", result)
            self.assertNotIn("\\", result)


if __name__ == "__main__":
    unittest.main()
