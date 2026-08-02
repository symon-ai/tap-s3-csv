import importlib.util
import os
import random
import unittest

# Load the standalone CSV fixture generator by path (its directory name contains
# a dash so it cannot be imported as a normal package).
_WRITER_PATH = os.path.join(
    os.path.dirname(__file__), "..", "resources", "tap-s3-csv", "writer.py"
)


def _load_writer():
    spec = importlib.util.spec_from_file_location("_writer_under_test", _WRITER_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class TestWriterSecureRng(unittest.TestCase):
    """WP-33319: writer.py must use a cryptographically secure RNG (CWE-331)."""

    def setUp(self):
        self.writer = _load_writer()

    def test_uses_system_random_not_mersenne_twister(self):
        # Regression for CWE-331: the module-level RNG must be os.urandom-backed
        # (random.SystemRandom), not the default Mersenne-Twister-backed random.
        self.assertIsInstance(self.writer._rng, random.SystemRandom)

    def test_float_double_normal_range_shape_and_range_preserved(self):
        rows = self.writer.test_float_double_normal_range()
        # single column: header + 100 generated values
        self.assertEqual(len(rows), 1)
        values = rows[0][1:]
        self.assertEqual(rows[0][0], "float with precision")
        self.assertEqual(len(values), 100)
        # first 25 come from random() -> [0.0, 1.0)
        self.assertTrue(all(0.0 <= v < 1.0 for v in values[0:25]))
        # values 51-75 come from -random() -> (-1.0, 0.0]
        self.assertTrue(all(-1.0 < v <= 0.0 for v in values[50:75]))


if __name__ == "__main__":
    unittest.main()
