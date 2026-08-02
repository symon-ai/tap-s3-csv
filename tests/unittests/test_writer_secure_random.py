import os
import random
import unittest
import importlib.util

# The CSV fixture generator lives in a hyphenated (non-package) directory,
# so load it directly by path.
WRITER_PATH = os.path.join(
    os.path.dirname(os.path.dirname(__file__)),
    "resources", "tap-s3-csv", "writer.py",
)


def _load_writer():
    spec = importlib.util.spec_from_file_location("tap_s3_csv_fixture_writer", WRITER_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class TestWriterSecureRandom(unittest.TestCase):
    """WP-33340: CWE-331 remediation for the tap-s3-csv fixture generator."""

    def test_module_random_is_system_random(self):
        # Regression for CWE-331: the fixture generator must draw from a
        # cryptographically secure (os.urandom-backed) source, not the
        # default insufficient-entropy Mersenne-Twister global.
        writer = _load_writer()
        self.assertIsInstance(writer.random, random.SystemRandom)

    def test_percentage_generator_still_produces_valid_output(self):
        # Behaviour must be preserved: the flagged generator still returns
        # the same shape (2 columns: positive/negative percentages) of data.
        writer = _load_writer()
        result = writer.test_number_formats_percentages()
        self.assertEqual(len(result), 2)
        # header row preserved on each column
        self.assertEqual(result[0][0], "positive percentages")
        self.assertEqual(result[1][0], "negative percentages")
        # 1 header + 100 generated rows
        self.assertEqual(len(result[0]), 101)
        self.assertTrue(all(str(v).endswith("%") for v in result[0][1:]))


if __name__ == "__main__":
    unittest.main()
