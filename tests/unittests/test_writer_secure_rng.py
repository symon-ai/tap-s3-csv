import os
import random
import importlib.util
import unittest

# The fixture module lives in a hyphenated directory that is not importable
# as a normal package, so load it by path via importlib.
WRITER_PATH = os.path.join(
    os.path.dirname(__file__),
    os.pardir,
    "resources",
    "tap-s3-csv",
    "writer.py",
)


def _load_writer():
    spec = importlib.util.spec_from_file_location("tap_s3_csv_test_writer", WRITER_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class TestWriterSecureRng(unittest.TestCase):
    """WP-33326: writer.py fixture generator must use a CSPRNG (CWE-331)."""

    def test_writer_random_is_system_random(self):
        writer = _load_writer()
        # random.SystemRandom is os.urandom-backed; the insecure default
        # random.Random (Mersenne Twister) must no longer be used.
        self.assertIsInstance(writer.random, random.SystemRandom)

    def test_fixture_generation_still_works(self):
        writer = _load_writer()
        result = writer.test_number_formats_commas()
        # Behaviour preserved: two columns (positive/negative) plus a header row.
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0][0], "positive numbers with commas")
        self.assertEqual(result[1][0], "negative numbers with commas")


if __name__ == "__main__":
    unittest.main()
