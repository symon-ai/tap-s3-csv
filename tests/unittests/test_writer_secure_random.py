import importlib.util
import os
import random
import unittest

# The CSV fixture generator lives under a hyphenated resources path, so it is
# not importable as a normal package module. Load it directly by file path.
_WRITER_PATH = os.path.join(
    os.path.dirname(__file__), os.pardir, "resources", "tap-s3-csv", "writer.py"
)


def _load_writer():
    spec = importlib.util.spec_from_file_location("tap_s3_csv_fixture_writer", _WRITER_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class TestWriterSecureRandom(unittest.TestCase):
    """WP-33348: CWE-331 remediation for tests/resources/tap-s3-csv/writer.py.

    The fixture generator must draw from a cryptographically strong PRNG
    (SystemRandom, backed by os.urandom) rather than the default
    non-cryptographic Mersenne Twister generator.
    """

    def test_writer_uses_system_random(self):
        writer = _load_writer()
        self.assertIsInstance(writer.random, random.SystemRandom)

    def test_generated_data_shape_preserved(self):
        # SystemRandom exposes the same call surface, so the generators still
        # produce data of the expected shape/ranges.
        writer = _load_writer()
        row = writer.test_bigint_valid_range()
        self.assertEqual(len(row), 1)
        self.assertEqual(row[0][0], "bigint_signed")
        self.assertEqual(len(row[0]), 101)


if __name__ == "__main__":
    unittest.main()
