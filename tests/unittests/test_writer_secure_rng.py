import importlib.util
import os
import random
import unittest

# writer.py lives in a hyphenated resources directory, so it is not importable
# as a normal package module; load it by path instead.
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

    # WP-33317: fixture generator must draw from a CSPRNG (CWE-331), not the
    # default insecure Mersenne-Twister PRNG.
    def test_writer_uses_system_random(self):
        writer = _load_writer()
        self.assertIsInstance(writer.random, random.SystemRandom)

    def test_fixture_generation_still_produces_valid_output(self):
        writer = _load_writer()
        rows = writer.test_bigint_valid_range()
        self.assertEqual(rows[0][0], "bigint_signed")
        self.assertEqual(len(rows[0]), 101)


if __name__ == "__main__":
    unittest.main()
