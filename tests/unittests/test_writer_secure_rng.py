import importlib.util
import os
import secrets
import unittest


def _load_writer():
    path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.realpath(__file__))),
        "resources",
        "tap-s3-csv",
        "writer.py",
    )
    spec = importlib.util.spec_from_file_location("writer_fixture", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class TestWriterSecureRng(unittest.TestCase):
    """WP-33377: the CSV fixture generator must use a cryptographically-secure
    RNG (CWE-331) while still producing well-formed test data."""

    def setUp(self):
        self.writer = _load_writer()

    def test_writer_uses_secure_system_random(self):
        # Regression: fails if the generator falls back to the insecure
        # standard `random` module instead of secrets.SystemRandom.
        self.assertIsInstance(self.writer.random, secrets.SystemRandom)

    def test_bigint_fixture_is_well_formed(self):
        data = self.writer.test_bigint_valid_range()
        self.assertEqual(len(data), 1)
        self.assertEqual(len(data[0]), 101)
        for value in data[0][1:]:
            self.assertIsInstance(value, int)

    def test_date_time_fixture_is_well_formed(self):
        columns = self.writer.test_date_time_iso_format()
        self.assertEqual(len(columns), 4)
        # header row + 100 generated rows per column
        for column in columns:
            self.assertEqual(len(column), 101)


if __name__ == "__main__":
    unittest.main()
