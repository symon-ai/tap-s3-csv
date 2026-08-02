import os
import re
import random
import unittest
import importlib.util


WRITER_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "resources", "tap-s3-csv", "writer.py")


def _load_writer():
    spec = importlib.util.spec_from_file_location("_tap_s3_csv_writer", WRITER_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class TestWriterSecureRng(unittest.TestCase):
    """WP-33325: CWE-331 - the CSV test-data generator must use a
    cryptographically secure RNG, not the non-cryptographic module-level
    `random` functions."""

    def test_uses_system_random(self):
        module = _load_writer()
        self.assertIsInstance(
            module._rng, random.SystemRandom,
            "writer must generate data through random.SystemRandom() (CWE-331)")

    def test_no_insecure_module_level_random_calls(self):
        with open(WRITER_PATH, encoding="utf-8") as handle:
            source = handle.read()
        insecure = re.findall(r"\brandom\.(random|randint|uniform)\(", source)
        self.assertEqual(
            insecure, [],
            "insecure non-cryptographic random.* call sites remain (CWE-331): "
            f"{insecure}")

    def test_generator_still_produces_data(self):
        module = _load_writer()
        rows = module.test_number_formats_commas()
        # header row + 100 generated rows, transposed into 2 columns
        self.assertEqual(len(rows), 2)
        self.assertEqual(len(rows[0]), 101)


if __name__ == "__main__":
    unittest.main()
