import unittest
import os
import random
import importlib.util


def load_writer_module():
    """Load the test-fixture writer.py from resources/tap-s3-csv/.

    It lives in a hyphenated directory so it is not importable as a package;
    load it directly by path.
    """
    writer_path = os.path.join(
        os.path.dirname(os.path.dirname(__file__)),
        "resources", "tap-s3-csv", "writer.py")
    spec = importlib.util.spec_from_file_location("writer_fixture", writer_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class TestWriterSecureRandom(unittest.TestCase):

    def test_writer_uses_cryptographically_secure_rng(self):
        # WP-33356 (CWE-331): the fixture generator must draw randomness from a
        # cryptographically secure source. Rebinding `random` to a
        # SystemRandom instance (os.urandom-backed) satisfies this. Without the
        # fix, `writer.random` is the insecure stdlib `random` module.
        writer = load_writer_module()
        self.assertIsInstance(writer.random, random.SystemRandom)

    def test_random_call_sites_still_behave(self):
        # SystemRandom inherits randint/uniform/random, so the fixture
        # generators remain fully functional after the fix.
        writer = load_writer_module()
        self.assertEqual(len(writer.test_date_time_iso_format_to_minutes()), 3)
        self.assertEqual(writer.test_bigint_valid_range()[0][0], "bigint_signed")


if __name__ == "__main__":
    unittest.main()
