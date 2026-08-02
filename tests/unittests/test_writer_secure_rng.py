import os
import re
import random
import unittest
import importlib.util

# writer.py is a standalone CSV-fixture generator living under
# tests/resources/tap-s3-csv/. Load it directly by path because the
# containing directory name is not a valid Python package identifier.
WRITER_PATH = os.path.join(
    os.path.dirname(__file__), os.pardir, "resources", "tap-s3-csv", "writer.py"
)


def _load_writer():
    spec = importlib.util.spec_from_file_location("tap_s3_csv_writer", WRITER_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class TestWriterSecureRng(unittest.TestCase):
    """WP-33309: remediate Veracode CWE-331 (insufficient entropy) in writer.py.

    Fixture values must be synthesized with a cryptographically-strong RNG
    (random.SystemRandom / secrets), not the default Mersenne-Twister PRNG.
    """

    def test_module_uses_system_random(self):
        writer = _load_writer()
        self.assertTrue(hasattr(writer, "_rng"), "writer must expose a shared RNG")
        self.assertIsInstance(
            writer._rng,
            random.SystemRandom,
            "writer._rng must be a cryptographically-strong SystemRandom (CWE-331)",
        )

    def test_no_insecure_prng_call_sites(self):
        with open(WRITER_PATH, encoding="utf-8") as source_file:
            source = source_file.read()
        insecure = re.findall(r"\brandom\.(?:randint|uniform|random)\b", source)
        self.assertEqual(
            insecure,
            [],
            "insecure default-PRNG call sites remain in writer.py (CWE-331)",
        )


if __name__ == "__main__":
    unittest.main()
