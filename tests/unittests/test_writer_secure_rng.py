import os
import random
import unittest
import importlib.util


def _load_writer_module():
    """Load the hyphenated-path test-fixture generator writer.py by file path."""
    writer_path = os.path.join(
        os.path.dirname(__file__),
        os.pardir,
        "resources",
        "tap-s3-csv",
        "writer.py",
    )
    spec = importlib.util.spec_from_file_location("tap_s3_csv_writer_fixture", writer_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class TestWriterSecureRng(unittest.TestCase):
    """WP-33408: remediate CWE-331 (insufficient entropy) in the writer.py test-fixture generator."""

    def test_writer_uses_cryptographically_secure_rng(self):
        '''
            The module-level `random` name must be reseated to a SystemRandom
            (os.urandom-backed CSPRNG) so every random.* fixture call draws
            from sufficient entropy. Fails on the insecure default PRNG.
        '''
        writer = _load_writer_module()
        self.assertIsInstance(writer.random, random.SystemRandom)

    def test_fixture_generation_still_works(self):
        '''
            Reseating `random` must not break fixture generation. Exercise a
            deterministic generator to confirm the module still produces data.
        '''
        writer = _load_writer_module()
        data = writer.test_string_data_by_columns()
        # 26 columns produced by transposing the 100x26 grid.
        self.assertEqual(len(data), 26)
        # Each column has one value per row.
        self.assertEqual(len(data[0]), 100)


if __name__ == "__main__":
    unittest.main()
