import unittest
from unittest import mock

from tap_s3_csv import dialect


class FakeFileHandle:
    def __init__(self, lines):
        self._lines = lines

    def iter_lines(self):
        for line in self._lines:
            yield line


def build_lines(num_lines):
    # first line is a header, remaining lines carry a high byte so chardet has
    # something to detect; enough lines to force sampling/padding to run.
    lines = [b'id,name,value']
    for i in range(1, num_lines):
        lines.append('row{},na\xefme,{}'.format(i, i).encode('latin-1'))
    return lines


class TestDialectSampling(unittest.TestCase):

    def test_no_standard_prng_used(self):
        '''
            WP-33420 / CWE-331: dialect.py must not rely on the standard `random`
            module for sampling chardet detection lines.
        '''
        self.assertFalse(hasattr(dialect, 'random'),
                         "dialect module should not import the standard random PRNG (CWE-331)")

    @mock.patch('tap_s3_csv.dialect.s3.get_file_handle')
    def test_encoding_detection_is_deterministic(self, mock_get_file_handle):
        '''
            WP-33420: sampling padding must be deterministic/reproducible so the
            detected encoding is stable across runs on the same input.
        '''
        lines = build_lines(1000)

        config = {}
        results = []
        for _ in range(3):
            mock_get_file_handle.return_value = FakeFileHandle(list(lines))
            table = {'delimiter': ',', 'quotechar': '"', 'encoding': ''}
            s3_file = {'key': 'sample.csv'}
            dialect.detect_dialect(config, s3_file, table)
            results.append(table['encoding'])

        self.assertEqual(results[0], results[1])
        self.assertEqual(results[1], results[2])
        self.assertTrue(results[0])


if __name__ == '__main__':
    unittest.main()
