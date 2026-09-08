import importlib.util
import os
import random
import unittest

# The synthetic CSV test-data generator lives at a hyphenated path that is not
# importable as a normal module, so load it by file location.
WRITER_PATH = os.path.join(
    os.path.dirname(__file__), "..", "resources", "tap-s3-csv", "writer.py"
)


def _load_writer():
    spec = importlib.util.spec_from_file_location("s3_csv_writer", WRITER_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class TestWriterUsesCsprng(unittest.TestCase):
    """Regression for CWE-331: the generator must source randomness from a
    cryptographically secure RNG (random.SystemRandom / os.urandom) rather than
    the insecure Mersenne-Twister module-level random API."""

    def test_module_exposes_systemrandom_instance(self):
        writer = _load_writer()
        # Fails without the fix: _rng does not exist on the pre-fix module.
        self.assertTrue(hasattr(writer, "_rng"))
        self.assertIsInstance(writer._rng, random.SystemRandom)

    def test_generated_data_semantics_preserved(self):
        writer = _load_writer()
        # Same method surface -> ranges and counts must still hold.
        cols = writer.test_primary_key_unique_values_and_nullable_integers()
        # header + 100 rows -> 4 columns each of length 101
        self.assertEqual(len(cols), 4)
        for column in cols:
            self.assertEqual(len(column), 101)


if __name__ == "__main__":
    unittest.main()
