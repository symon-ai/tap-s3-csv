import importlib.util
import os
import random
import unittest

# The fixture module lives under a hyphenated directory, so it cannot be
# imported with a normal `import` statement; load it by file path instead.
WRITER_PATH = os.path.join(
    os.path.dirname(__file__),
    os.pardir,
    "resources",
    "tap-s3-csv",
    "writer.py",
)


def _load_writer():
    spec = importlib.util.spec_from_file_location("_tap_s3_csv_fixture_writer", WRITER_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class TestWriterSecureRandom(unittest.TestCase):

    def test_writer_uses_system_random(self):
        # CWE-331 regression: the fixture module must use an os.urandom-backed
        # CSPRNG, not the default insufficient-entropy PRNG. Fails before the
        # `random = random.SystemRandom()` rebind, passes after.
        writer = _load_writer()
        self.assertIsInstance(writer.random, random.SystemRandom)

    def test_fixture_generation_shape_unchanged(self):
        # Sanity-check that swapping in SystemRandom keeps fixture output shape.
        writer = _load_writer()
        result = writer.test_multiple_file_with_pk_part_a()
        # Two columns: "key" and "value".
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0][0], "key")
        self.assertEqual(result[1][0], "value")
        # Header row + 19 generated rows (range(1, 20)) => 20 entries per column.
        self.assertEqual(len(result[0]), 20)
        self.assertEqual(len(result[1]), 20)


if __name__ == "__main__":
    unittest.main()
