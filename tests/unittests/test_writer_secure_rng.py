import os
import random
import unittest
import importlib.util


def _load_writer_module():
    """Load the test-fixture generator tests/resources/tap-s3-csv/writer.py by
    path, since its directory is hyphenated and not an importable package."""
    writer_path = os.path.join(
        os.path.dirname(__file__),
        os.pardir,
        "resources",
        "tap-s3-csv",
        "writer.py",
    )
    writer_path = os.path.abspath(writer_path)
    spec = importlib.util.spec_from_file_location("_tap_s3_csv_writer", writer_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class TestWriterSecureRng(unittest.TestCase):
    """WP-33336 / CWE-331: writer.py must generate its fixture data from a
    cryptographically secure RNG, not the insufficient-entropy default PRNG."""

    def test_writer_uses_cryptographically_secure_rng(self):
        writer = _load_writer_module()
        # Fails without the fix: `random` is the stdlib module (insecure PRNG).
        # Passes with the fix: `random` is a SystemRandom (os.urandom-backed CSPRNG).
        self.assertIsInstance(writer.random, random.SystemRandom)

    def test_number_formats_plus_minus_signs_still_works(self):
        # Guards the CWE-331 remediation site at writer.py line 104
        # (test_number_formats_plus_minus_signs), which formats
        # -random.random() * 10 ** 10 values.
        writer = _load_writer_module()
        columns = writer.test_number_formats_plus_minus_signs()
        self.assertEqual(columns[0][0], "positive with various signs")
        self.assertEqual(columns[1][0], "negative with signs")
        # header + 34 + 34 + 32 generated rows per column.
        self.assertEqual(len(columns[0]), 1 + 34 + 34 + 32)


if __name__ == "__main__":
    unittest.main()
