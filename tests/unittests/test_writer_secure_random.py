import ast
import os
import random
import unittest
from importlib import util

# The test-resource CSV data generator lives in a hyphenated directory, so it
# cannot be imported by package name; load it directly by file path.
WRITER_PATH = os.path.join(
    os.path.dirname(__file__), os.pardir, "resources", "tap-s3-csv", "writer.py"
)


def _load_writer_module():
    spec = util.spec_from_file_location("tap_s3_csv_test_writer", WRITER_PATH)
    module = util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class TestWriterSecureRandom(unittest.TestCase):
    """CWE-331: writer.py must draw randomness from a cryptographically secure
    generator (random.SystemRandom / os.urandom), not the module-global
    Mersenne-Twister PRNG."""

    def test_uses_system_random_instance(self):
        module = _load_writer_module()
        self.assertTrue(hasattr(module, "_rng"), "expected a module-level RNG")
        self.assertIsInstance(module._rng, random.SystemRandom)

    def test_no_insecure_module_global_random_draws(self):
        with open(WRITER_PATH, encoding="utf-8") as file:
            tree = ast.parse(file.read())

        insecure = []
        for node in ast.walk(tree):
            if (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Attribute)
                and isinstance(node.func.value, ast.Name)
                and node.func.value.id == "random"
                and node.func.attr in {"random", "randint", "uniform"}
            ):
                insecure.append((node.func.attr, node.lineno))

        self.assertEqual(
            insecure, [], f"insecure module-global random.* draws remain: {insecure}"
        )


if __name__ == "__main__":
    unittest.main()
