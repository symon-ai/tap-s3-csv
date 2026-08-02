import importlib.util
import os
import random
import re
import unittest

# Matches a call to the insecure global random module (random.random/randint/
# uniform) while NOT matching the secure `_secure_random.<method>(` instance
# (the char before "random" would be a word char there, so no boundary).
_INSECURE_CALL = re.compile(r"(?<![\w.])random\.(random|randint|uniform)\(")

# WP-33354 / CWE-331: the test-data generator writer.py must draw its random
# values from a cryptographically secure RNG (random.SystemRandom, backed by
# os.urandom) rather than the default seedable Mersenne Twister PRNG.
WRITER_PATH = os.path.join(
    os.path.dirname(os.path.dirname(__file__)),
    "resources", "tap-s3-csv", "writer.py",
)


def _load_writer_module():
    spec = importlib.util.spec_from_file_location("tap_s3_csv_test_writer", WRITER_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class TestWriterSecureRandom(unittest.TestCase):

    def test_writer_uses_system_random(self):
        module = _load_writer_module()
        # Fails without the fix: the module exposed no secure RNG instance.
        self.assertTrue(
            hasattr(module, "_secure_random"),
            "writer.py must expose a cryptographically secure RNG instance",
        )
        self.assertIsInstance(
            module._secure_random,
            random.SystemRandom,
            "writer.py random values must come from random.SystemRandom (CWE-331)",
        )

    def test_writer_source_has_no_insecure_random_calls(self):
        with open(WRITER_PATH, "r", encoding="utf-8") as handle:
            source_lines = handle.readlines()
        offenders = [
            (idx + 1, line.strip())
            for idx, line in enumerate(source_lines)
            if _INSECURE_CALL.search(line)
        ]
        self.assertEqual(
            offenders,
            [],
            f"writer.py still calls the insecure global random module: {offenders}",
        )


if __name__ == "__main__":
    unittest.main()
