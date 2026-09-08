import importlib.util
import os
import random
import re
import unittest

# Path to the CSV test-fixture generator flagged by Veracode CWE-331.
WRITER_PATH = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "..", "resources", "tap-s3-csv", "writer.py")
)


def _load_writer():
    spec = importlib.util.spec_from_file_location("tap_s3_csv_fixture_writer", WRITER_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class TestWriterSecureRng(unittest.TestCase):
    """WP-33385: remediate CWE-331 insecure RNG in the tap-s3-csv fixture writer."""

    def test_module_uses_cryptographically_strong_rng(self):
        # The fixture writer must route its PRNG calls through a CSPRNG-backed
        # source (random.SystemRandom) rather than the insecure module-level MT.
        writer = _load_writer()
        self.assertIsInstance(writer._rng, random.SystemRandom)

    def test_no_insecure_module_level_prng_calls(self):
        # Guards against re-introducing insecure random.<fn> fixture-value calls
        # that Veracode's CWE-331 rule flags. Fails against the pre-fix source.
        with open(WRITER_PATH, "r") as handle:
            source = handle.read()
        insecure = re.findall(r"\brandom\.(randint|random|uniform)\b", source)
        self.assertEqual(insecure, [])

    def test_fixture_data_contract_preserved(self):
        # The remediation must not change the shape/ranges of generated data.
        writer = _load_writer()
        columns = writer.test_primary_key_unique_values_and_nullable_integers()
        # 4 columns: key, nullable integer, non-nullable integer, description.
        self.assertEqual(len(columns), 4)
        # Header row + 100 data rows -> 101 entries per column.
        self.assertTrue(all(len(col) == 101 for col in columns))
        keys = list(columns[0][1:])
        self.assertEqual(keys, list(range(1, 101)))
        non_nullable = columns[2][1:]
        for key, value in zip(keys, non_nullable):
            if key == 12:
                self.assertIsNone(value)
            else:
                self.assertTrue(1 <= value <= 1000)


if __name__ == "__main__":
    unittest.main()
