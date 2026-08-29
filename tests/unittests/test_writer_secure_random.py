import os
import re
import random
import unittest
import importlib.util

# The CSV fixture generator lives in a hyphenated (non-package) directory,
# so load it directly by path.
WRITER_PATH = os.path.join(
    os.path.dirname(os.path.dirname(__file__)),
    "resources", "tap-s3-csv", "writer.py",
)


def _load_writer():
    spec = importlib.util.spec_from_file_location("tap_s3_csv_fixture_writer", WRITER_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _writer_source():
    with open(WRITER_PATH, "r", encoding="utf-8") as handle:
        return handle.read()


def _strip_comments(source):
    # Drop line comments so token assertions inspect executable code only,
    # not the explanatory CWE-331 remediation comment.
    return "\n".join(line.split("#", 1)[0] for line in source.splitlines())


class TestWriterSecureRandom(unittest.TestCase):
    """WP-33340: CWE-331 remediation for the tap-s3-csv fixture generator."""

    def test_secure_random_is_system_random(self):
        # Regression for CWE-331: the fixture generator must draw from a
        # cryptographically secure (os.urandom-backed) source, not the
        # default insufficient-entropy Mersenne-Twister global.
        writer = _load_writer()
        self.assertIsInstance(writer._secure_random, random.SystemRandom)

    def test_no_residual_insufficient_entropy_call_signature(self):
        # R5 scanner-recognition gate: Veracode CWE-331 keys on the
        # `random.random()/randint()/uniform()` stdlib-global CALL signature
        # at the flagged sink. After remediation NO such bare call may remain
        # anywhere in the fixture generator; every draw must go through the
        # secure SystemRandom instance instead.
        code = _strip_comments(_writer_source())
        residual = re.findall(r"(?<![\w.])random\.(?:random|randint|uniform)\s*\(", code)
        self.assertEqual(
            residual, [],
            "Found residual insufficient-entropy stdlib random.* call(s): %r" % residual,
        )
        # And the secure instance IS the source of the draws.
        self.assertTrue(
            re.search(r"_secure_random\.(?:random|randint|uniform)\s*\(", code),
            "Fixture generation must draw from the secure SystemRandom instance.",
        )

    def test_percentage_generator_still_produces_valid_output(self):
        # Behaviour must be preserved: the flagged generator still returns
        # the same shape (2 columns: positive/negative percentages) of data.
        writer = _load_writer()
        result = writer.test_number_formats_percentages()
        self.assertEqual(len(result), 2)
        # header row preserved on each column
        self.assertEqual(result[0][0], "positive percentages")
        self.assertEqual(result[1][0], "negative percentages")
        # 1 header + 100 generated rows
        self.assertEqual(len(result[0]), 101)
        self.assertTrue(all(str(v).endswith("%") for v in result[0][1:]))


if __name__ == "__main__":
    unittest.main()
