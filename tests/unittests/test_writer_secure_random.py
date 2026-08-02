import importlib.util
import os
import random
import re
import unittest

# The CSV test-fixture generator lives outside the importable package, under
# tests/resources/tap-s3-csv/writer.py. Load it directly by path.
_WRITER_PATH = os.path.join(
    os.path.dirname(__file__), os.pardir, "resources", "tap-s3-csv", "writer.py"
)
_WRITER_PATH = os.path.abspath(_WRITER_PATH)


def _load_writer():
    spec = importlib.util.spec_from_file_location("_wp_writer", _WRITER_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class TestWriterSecureRandom(unittest.TestCase):
    """WP-33313: the fixture generator must use a cryptographically-secure PRNG
    (CWE-331), not the non-secure module-level ``random`` functions."""

    def test_no_insecure_module_level_prng_calls(self):
        with open(_WRITER_PATH, "r", encoding="utf-8") as handle:
            source = handle.read()
        insecure = re.findall(r"\brandom\.(?:random|randint|uniform)\s*\(", source)
        self.assertEqual(
            insecure,
            [],
            "writer.py must not call non-secure random.random/randint/uniform "
            "(CWE-331); use the SystemRandom instance instead",
        )

    def test_uses_system_random_generator(self):
        module = _load_writer()
        self.assertIsInstance(module._SYS_RANDOM, random.SystemRandom)

    def test_generator_output_shape_preserved(self):
        module = _load_writer()
        rows = module.test_float_double_representable_range()
        self.assertEqual(len(rows), 2)
        self.assertEqual(len(rows[0]), 101)


if __name__ == "__main__":
    unittest.main()
