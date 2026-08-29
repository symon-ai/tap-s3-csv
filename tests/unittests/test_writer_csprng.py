import importlib.util
import os
import random
import unittest


class TestWriterUsesCSPRNG(unittest.TestCase):
    """Regression for CWE-331 (WP-33332): the CSV test-data generator must
    draw randomness from a cryptographically-secure RNG (SystemRandom /
    os.urandom), not the default Mersenne-Twister PRNG."""

    def _load_writer(self):
        writer_path = os.path.join(
            os.path.dirname(__file__), os.pardir, "resources", "tap-s3-csv", "writer.py"
        )
        spec = importlib.util.spec_from_file_location("_tap_s3_csv_writer", writer_path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module

    def test_writer_random_is_system_random(self):
        module = self._load_writer()
        self.assertIsInstance(module.random, random.SystemRandom)

    def test_writer_random_call_surface_preserved(self):
        module = self._load_writer()
        self.assertIsInstance(module.random.random(), float)
        self.assertIn(module.random.randint(1, 1), (1,))
        self.assertIsInstance(module.random.uniform(0.0, 1.0), float)


if __name__ == "__main__":
    unittest.main()
