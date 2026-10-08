import os
import random
import unittest
import importlib.util


def _load_writer_module():
    """Load the hyphenated-path fixture generator tests/resources/tap-s3-csv/writer.py."""
    writer_path = os.path.join(
        os.path.dirname(os.path.dirname(__file__)),
        "resources",
        "tap-s3-csv",
        "writer.py",
    )
    spec = importlib.util.spec_from_file_location("tap_s3_csv_test_writer", writer_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class TestWriterSecureRng(unittest.TestCase):
    """WP-33391 (CWE-331): the CSV test-fixture generator must use a CSPRNG.

    Fails on the pre-fix code (module-level `random` is the stdlib `random`
    module) and passes once `random` is rebound to `random.SystemRandom()`.
    """

    def test_writer_uses_system_random(self):
        writer = _load_writer_module()
        self.assertIsInstance(
            writer.random,
            random.SystemRandom,
            "writer.random must be a cryptographically secure random.SystemRandom "
            "instance (CWE-331 remediation)",
        )

    def test_writer_random_still_produces_valid_output(self):
        writer = _load_writer_module()
        value = writer.random.randint(1, 1000)
        self.assertGreaterEqual(value, 1)
        self.assertLessEqual(value, 1000)


if __name__ == "__main__":
    unittest.main()
