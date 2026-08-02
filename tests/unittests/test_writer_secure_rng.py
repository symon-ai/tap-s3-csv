import ast
import os
import random
import unittest
from importlib import util

# WP-33311 / CWE-331: the CSV test-fixture generator must not use the
# insecure module-level `random` PRNG. It must use a cryptographically
# secure RNG (secrets.SystemRandom, a drop-in for the random.* API).
WRITER_PATH = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__), os.pardir, "resources", "tap-s3-csv", "writer.py"
    )
)


def _load_writer_module():
    spec = util.spec_from_file_location("tap_s3_csv_test_writer", WRITER_PATH)
    module = util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class TestWriterSecureRng(unittest.TestCase):

    def test_source_has_no_insecure_random_usage(self):
        with open(WRITER_PATH) as file:
            source = file.read()

        tree = ast.parse(source)

        # No `import random` (bare or aliased) should remain.
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                names = [alias.name for alias in node.names]
                self.assertNotIn("random", names)

        # No `random.<func>()` call sites should remain.
        for node in ast.walk(tree):
            if isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name):
                self.assertNotEqual(
                    node.value.id,
                    "random",
                    "writer.py still calls the insecure module-level random API",
                )

    def test_rng_is_secure_system_random(self):
        module = _load_writer_module()
        # SystemRandom draws from os.urandom (a CSPRNG) and exposes the same
        # uniform / randint / random API the fixtures rely on.
        self.assertIsInstance(module._RNG, random.SystemRandom)

    def test_fixture_generation_still_works(self):
        module = _load_writer_module()
        # Behavior/output contract preserved: still returns column-shaped data.
        result = module.test_float_double_representable_range()
        self.assertEqual(len(result), 2)
        self.assertTrue(all(isinstance(column, tuple) for column in result))


if __name__ == "__main__":
    unittest.main()
