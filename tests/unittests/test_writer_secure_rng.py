import os
import random
import importlib.util
import unittest

# The fixture CSV data generator lives in a dash-named resources directory, so
# it is not importable as a normal package. Load it directly by file path.
WRITER_PATH = os.path.join(
    os.path.dirname(os.path.realpath(__file__)),
    "..", "resources", "tap-s3-csv", "writer.py")


def _load_writer_module():
    spec = importlib.util.spec_from_file_location("tap_s3_csv_fixture_writer", WRITER_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class TestWriterSecureRng(unittest.TestCase):
    '''
        WP-33400: the fixture CSV data generator must draw all random values
        from a cryptographically secure RNG (CWE-331). It rebinds the module
        `random` reference to a random.SystemRandom instance so every random.*
        call site uses os.urandom-backed entropy instead of the Mersenne
        Twister. This test fails on the pre-fix code (module `random` is the
        stdlib module, not a SystemRandom instance) and passes post-fix.
    '''

    def test_writer_uses_system_random_csprng(self):
        module = _load_writer_module()
        self.assertIsInstance(
            module.random, random.SystemRandom,
            "writer.py must rebind `random` to a random.SystemRandom CSPRNG (CWE-331)")

    def test_writer_still_generates_valid_fixture_output(self):
        module = _load_writer_module()
        # A representative generator that uses random.randint at the flagged
        # line's call pattern must still produce well-formed output.
        columns = module.test_header_order_multiple_file_with_pk_part_a()
        rows = list(zip(*columns))
        self.assertEqual(rows[0], ("key", "value"))
        for key, value in rows[1:]:
            self.assertIsInstance(value, int)
            self.assertTrue(1 <= value <= 1000)


if __name__ == "__main__":
    unittest.main()
