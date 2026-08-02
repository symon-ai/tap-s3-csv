import importlib.util
import os
import random
import unittest

# The CSV test-fixture generator lives under a hyphenated resources directory,
# so it cannot be imported by module name; load it directly from its file path.
WRITER_PATH = os.path.join(
    os.path.dirname(__file__),
    os.pardir,
    "resources",
    "tap-s3-csv",
    "writer.py",
)


def _load_writer():
    spec = importlib.util.spec_from_file_location("tap_s3_csv_fixture_writer", WRITER_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class TestWriterSecureRng(unittest.TestCase):
    """WP-33334: writer.py must draw fixture data from a cryptographically
    secure RNG (os.urandom-backed) rather than the insufficient-entropy
    default PRNG (CWE-331)."""

    def test_writer_random_is_system_random(self):
        writer = _load_writer()
        self.assertIsInstance(writer.random, random.SystemRandom)

    def test_fixture_generation_still_works(self):
        writer = _load_writer()
        result = writer.test_number_formats_plus_minus_signs()
        # Two columns (positive / negative) plus header rows, all rows populated.
        self.assertEqual(len(result), 2)
        for column in result:
            self.assertTrue(all(cell is not None for cell in column))


if __name__ == "__main__":
    unittest.main()
