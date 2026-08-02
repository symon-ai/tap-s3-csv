import os
import random
import unittest
import importlib.util

# writer.py lives under tests/resources/tap-s3-csv/ (a hyphenated, non-package
# path), so load it by file path rather than a normal import.
_WRITER_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "resources", "tap-s3-csv", "writer.py")


def _load_writer():
    spec = importlib.util.spec_from_file_location("tap_s3_csv_test_writer", _WRITER_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class TestWriterSecureRng(unittest.TestCase):
    """WP-33368: guard the CWE-331 remediation from silently regressing.

    The test-fixture data generator must use an os.urandom-backed secure RNG
    (random.SystemRandom) instead of the default deterministic PRNG.
    """

    def test_writer_uses_secure_rng(self):
        writer = _load_writer()
        self.assertIsInstance(
            writer.random,
            random.SystemRandom,
            "writer.random must be a random.SystemRandom (CWE-331 remediation)")

    def test_secure_rng_preserves_value_ranges(self):
        writer = _load_writer()
        # Behavior-preserving: randint/uniform still return in-range values.
        for _ in range(50):
            self.assertTrue(0 <= writer.random.randint(0, 59) <= 59)
            val = writer.random.uniform(-10, 10)
            self.assertTrue(-10 <= val <= 10)


if __name__ == "__main__":
    unittest.main()
