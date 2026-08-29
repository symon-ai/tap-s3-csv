import os
import random
import unittest
import importlib.util


def _load_writer_module():
    # writer.py is a CSV test-fixture data generator living under
    # tests/resources/tap-s3-csv/. Load it by path since it is not a package.
    writer_path = os.path.join(
        os.path.dirname(os.path.dirname(__file__)),
        "resources",
        "tap-s3-csv",
        "writer.py",
    )
    spec = importlib.util.spec_from_file_location("tap_s3_csv_fixture_writer", writer_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class TestWriterSecureRng(unittest.TestCase):
    """WP-33360: writer.py must draw fixture randomness from a CSPRNG (CWE-331)."""

    def test_writer_uses_system_random_csprng(self):
        module = _load_writer_module()
        # The module-level `random` binding must be an os.urandom-backed CSPRNG
        # (random.SystemRandom), not the insufficient-entropy default PRNG.
        self.assertIsInstance(module.random, random.SystemRandom)

    def test_writer_generator_still_produces_expected_shape(self):
        # The secure-RNG swap must not change generator output shape/behavior.
        module = _load_writer_module()
        columns = module.test_date_time_iso_format_to_seconds()
        self.assertEqual(len(columns), 3)
        self.assertEqual(columns[0][0], "time to seconds")
        self.assertEqual(len(columns[0]), 101)  # header + 100 rows


if __name__ == "__main__":
    unittest.main()
