import unittest
import os
import re
import secrets
import importlib.util


def _load_writer_module():
    # writer.py lives under tests/resources/tap-s3-csv/ (a hyphenated dir that
    # is not an importable package), so load it directly by path.
    writer_path = os.path.join(
        os.path.dirname(os.path.dirname(__file__)),
        'resources', 'tap-s3-csv', 'writer.py')
    spec = importlib.util.spec_from_file_location("tap_s3_csv_test_writer", writer_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module, writer_path


class TestWriterSecureRng(unittest.TestCase):
    """WP-33411: the CSV fixture generator must use a cryptographically secure
    RNG (CWE-331) instead of the non-cryptographic `random` module."""

    def test_uses_system_random_not_random_module(self):
        module, writer_path = _load_writer_module()

        # The module must expose a SecureRandom-backed generator.
        self.assertTrue(hasattr(module, '_rng'),
                        "writer.py must define a secrets.SystemRandom() generator")
        self.assertIsInstance(module._rng, secrets.SystemRandom)

        # No residual use of the insecure `random` module (Veracode re-scans the
        # whole file; a single remaining `random.*` call keeps CWE-331 open).
        with open(writer_path, 'r') as f:
            source = f.read()
        self.assertIsNone(re.search(r'^import random$', source, re.MULTILINE),
                          "writer.py must not import the insecure `random` module")
        self.assertIsNone(re.search(r'(?<![_\w])random\.', source),
                          "writer.py must not call the insecure `random` module")

    def test_generator_contract_preserved(self):
        module, _ = _load_writer_module()

        # SystemRandom exposes the same API the generator relies on.
        for method in ('randint', 'uniform', 'random'):
            self.assertTrue(hasattr(module._rng, method))

        # Data shapes/ranges are unchanged by the swap.
        signed = module.test_bigint_valid_range()
        self.assertEqual(signed[0][0], "bigint_signed")
        self.assertEqual(len(signed[0]), 1 + 95 + 5)
        for value in signed[0][1:96]:
            self.assertTrue(-2**63 <= value <= 2**63 - 1)


if __name__ == "__main__":
    unittest.main()
