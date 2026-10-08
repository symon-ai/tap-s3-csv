import os
import random
import importlib.util
import unittest


def _load_writer():
    writer_path = os.path.join(
        os.path.dirname(os.path.dirname(__file__)),
        "resources", "tap-s3-csv", "writer.py")
    spec = importlib.util.spec_from_file_location("writer_fixture", writer_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class TestWriterSecureRng(unittest.TestCase):
    """WP-33398: remediate CWE-331 in the CSV test-fixture generator.

    The generator must draw random values from a cryptographically secure
    source (random.SystemRandom / os.urandom) rather than the insecure default
    Mersenne Twister generator, while keeping the exact same value ranges and
    data shapes.
    """

    def test_uses_cryptographically_secure_rng(self):
        writer = _load_writer()
        self.assertTrue(hasattr(writer, "_rng"), "writer must route RNG through _rng")
        self.assertIsInstance(
            writer._rng, random.SystemRandom,
            "writer._rng must be a cryptographically secure random.SystemRandom")

    def test_value_ranges_and_shapes_preserved(self):
        writer = _load_writer()

        key_col, value_col = writer.test_multiple_file_with_pk_part_c()
        self.assertEqual(key_col[0], "key")
        self.assertEqual(value_col[0], "value")
        self.assertEqual(list(key_col[1:]), list(range(61, 80)))
        self.assertTrue(all(1 <= v <= 1000 for v in value_col[1:]))

        bigint = writer.test_bigint_valid_range()
        self.assertEqual(bigint[0][0], "bigint_signed")
        self.assertEqual(len(bigint[0]), 101)


if __name__ == "__main__":
    unittest.main()
