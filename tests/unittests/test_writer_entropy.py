import importlib.util
import os
import random
import unittest

# The fixture generator lives under a hyphenated resource path that is not a
# regular importable package, so load it directly from its file path.
WRITER_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "resources", "tap-s3-csv", "writer.py")


def _load_writer():
    spec = importlib.util.spec_from_file_location("tap_s3_csv_writer", WRITER_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class TestWriterEntropy(unittest.TestCase):
    """Regression test for WP-33353 (Veracode CWE-331, Insufficient Entropy).

    The fixture generator must draw its random values from a cryptographically
    strong source (random.SystemRandom, backed by os.urandom) rather than the
    default Mersenne-Twister ``random`` module, while keeping the same
    randint/uniform/random API and value ranges.
    """

    def test_generator_uses_system_random(self):
        writer = _load_writer()
        # Before the fix ``writer.random`` was the stdlib ``random`` module;
        # after the fix it is a SystemRandom instance.
        self.assertIsInstance(writer.random, random.SystemRandom)

    def test_value_ranges_preserved(self):
        writer = _load_writer()
        for _ in range(200):
            self.assertIn(writer.random.randint(0, 59), range(0, 60))
            self.assertIn(writer.random.randint(-23, 23), range(-23, 24))
            self.assertTrue(0.0 <= writer.random.random() < 1.0)


if __name__ == "__main__":
    unittest.main()
