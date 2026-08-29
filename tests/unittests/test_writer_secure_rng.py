import os
import re
import random
import unittest
import importlib.util

# The test-data generator lives under tests/resources and is not importable as a
# package, so load it directly by file path.
WRITER_PATH = os.path.join(
    os.path.dirname(os.path.dirname(__file__)),
    "resources", "tap-s3-csv", "writer.py")


def _load_writer():
    spec = importlib.util.spec_from_file_location("tap_s3_csv_writer", WRITER_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class TestWriterSecureRng(unittest.TestCase):

    # WP-33394 (CWE-331): the fixture generator must draw randomness from a
    # cryptographically-secure source, not the default Mersenne-Twister PRNG.
    def test_module_uses_system_random(self):
        writer = _load_writer()
        self.assertIsInstance(writer._rng, random.SystemRandom)

    # WP-33394 (CWE-331): guard against reintroducing the insecure module-level
    # random.randint / random.random / random.uniform calls that Veracode flags
    # (this is the assertion that fails without the fix).
    def test_no_insecure_prng_calls_in_source(self):
        with open(WRITER_PATH) as source_file:
            source = source_file.read()
        insecure = re.findall(r"\brandom\.(?:randint|random|uniform)\(", source)
        self.assertEqual(insecure, [])

    # WP-33394: behavior is preserved — values still fall in their declared ranges.
    def test_generated_values_stay_in_range(self):
        writer = _load_writer()
        signed = writer.test_bigint_valid_range()[0][1:96]
        self.assertEqual(len(signed), 95)
        for value in signed:
            self.assertIsInstance(value, int)
            self.assertTrue(-2**63 <= value <= 2**63 - 1)


if __name__ == "__main__":
    unittest.main()
