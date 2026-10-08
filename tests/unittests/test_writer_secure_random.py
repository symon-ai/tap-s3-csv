import importlib.util
import os
import random
import unittest

# The test-data generator lives under tests/resources and is not an importable
# package module, so load it directly by path.
WRITER_PATH = os.path.join(
    os.path.dirname(__file__), os.pardir, "resources", "tap-s3-csv", "writer.py"
)


def _load_writer():
    spec = importlib.util.spec_from_file_location("tap_s3_csv_test_writer", WRITER_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class TestWriterSecureRandom(unittest.TestCase):
    '''
        CWE-331: the test-data generator must synthesize rows with a
        cryptographically-appropriate RNG (random.SystemRandom), not the
        module-level pseudo-random generator.
    '''

    def setUp(self):
        self.writer = _load_writer()

    def test_uses_system_random(self):
        self.assertTrue(hasattr(self.writer, "secure_random"))
        self.assertIsInstance(self.writer.secure_random, random.SystemRandom)

    def test_generators_still_produce_valid_ranges(self):
        # randint-based generator preserves inclusive bounds
        rows = self.writer.test_multiple_file_with_pk_part_a()
        values = list(rows[1])[1:]  # drop the "value" header cell
        self.assertEqual(len(values), 19)
        for value in values:
            self.assertGreaterEqual(value, 1)
            self.assertLessEqual(value, 1000)

        # uniform/random-based datetime generator still emits 100 rows
        date_time = self.writer.test_date_time_iso_format_to_seconds()
        self.assertEqual(len(date_time[0]) - 1, 100)


if __name__ == "__main__":
    unittest.main()
