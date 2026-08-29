import importlib.util
import os
import random
import unittest


def _load_writer_module():
    """Load the test-fixture generator resources/tap-s3-csv/writer.py by path."""
    writer_path = os.path.join(
        os.path.dirname(os.path.dirname(__file__)),
        "resources", "tap-s3-csv", "writer.py")
    spec = importlib.util.spec_from_file_location("tap_s3_csv_writer", writer_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class TestWriterSecureRandom(unittest.TestCase):
    """
    WP-33304 regression test for CWE-331 (Insufficient Entropy).

    The writer.py CSV test-fixture generator must draw randomness from a
    cryptographically-secure source (random.SystemRandom, backed by
    os.urandom) rather than the module-level Mersenne Twister PRNG, while
    keeping identical generated-data semantics (ranges / call signatures).
    """

    def setUp(self):
        self.writer = _load_writer_module()

    def test_uses_system_random_source(self):
        # The shared generator instance must be a CSPRNG, not the module PRNG.
        self.assertTrue(hasattr(self.writer, "_secure_random"))
        self.assertIsInstance(self.writer._secure_random, random.SystemRandom)

    def test_unsigned_bigint_range_preserved(self):
        row = self.writer.test_unsigned_bigint_valid_range()[0]
        self.assertEqual(row[0], "bigint_unsigned")
        generated = row[1:99]
        self.assertEqual(len(generated), 98)
        for value in generated:
            self.assertGreaterEqual(value, 2 ** 63)
            self.assertLessEqual(value, 2 ** 64)

    def test_signed_bigint_range_preserved(self):
        row = self.writer.test_bigint_valid_range()[0]
        self.assertEqual(row[0], "bigint_signed")
        generated = row[1:96]
        self.assertEqual(len(generated), 95)
        for value in generated:
            self.assertGreaterEqual(value, -2 ** 63)
            self.assertLessEqual(value, 2 ** 63 - 1)


if __name__ == "__main__":
    unittest.main()
