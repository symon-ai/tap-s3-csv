import unittest
from unittest import mock

from tap_s3_csv import dialect

HEADER = b'DealerCode;AttributeCode;AttributeValue;AttributeValueName;StartDate;EndDate;ID\r\n'
ROW = 'GÖNCZCS2;DOMINO_USR;NYILASNEG;NYILASNEG;10/22/2026;12/31/3000;1756080221\r\n'


def detect_encoding(lines):
    table = {'delimiter': ';', 'quotechar': '"'}
    stream = mock.Mock()
    stream.iter_lines.return_value = iter(lines)
    with mock.patch('tap_s3_csv.dialect.s3.get_file_handle'), \
            mock.patch('tap_s3_csv.dialect.preprocess.PreprocessStream', return_value=stream):
        dialect.detect_dialect({}, {'key': 'sample.csv'}, table)
    return table['encoding']


def encoded(text, encoding, count=1):
    return [HEADER] + [text.encode(encoding)] * count


class TestDetectEncoding(unittest.TestCase):

    def assert_decodes(self, lines, expected_text, expected_encoding=None):
        encoding = detect_encoding(lines)
        if expected_encoding:
            self.assertEqual(encoding.lower(), expected_encoding)
        self.assertEqual(lines[-1].decode(encoding), expected_text)

    def test_matrix(self):
        cases = [
            # name, lines, text the last line must decode to, expected encoding (when it matters)
            ('ascii', [HEADER, b'abc;1\r\n'], 'abc;1\r\n', 'utf-8'),
            # chardet 5.2 guesses Windows-1252 (0.73) for this file, which turned Ö into Ã– (WP-36668)
            ('utf-8 single accent', encoded(ROW, 'utf-8'), ROW, 'utf-8'),
            ('utf-8 many accents', encoded('Árvíztűrő tükörfúrógép;Győr\r\n', 'utf-8', 50),
             'Árvíztűrő tükörfúrógép;Győr\r\n', 'utf-8'),
            # BOM must stay UTF-8-SIG so it is stripped from the header
            ('utf-8 bom', [b'\xef\xbb\xbf' + HEADER, ROW.encode('utf-8')], ROW, 'utf-8-sig'),
            ('windows-1252', encoded('GÖNCZCS0;PÉCS;café;x;10/22/2026;12/31/3000;0\r\n', 'cp1252', 3),
             'GÖNCZCS0;PÉCS;café;x;10/22/2026;12/31/3000;0\r\n', None),
            ('windows-1251', encoded('Москва;Привет мир\r\n', 'cp1251', 50), 'Москва;Привет мир\r\n', None),
            # one non-utf-8 line is enough to keep chardet's guess
            ('utf-8 then windows-1252 line', [HEADER, ROW.encode('utf-8'), 'café;x\r\n'.encode('cp1252')],
             'café;x\r\n', 'windows-1252'),
            # already-garbled text is valid utf-8 and is kept as-is
            ('double-encoded utf-8', encoded('GÃ–NCZ;x\r\n', 'utf-8'), 'GÃ–NCZ;x\r\n', 'utf-8'),
        ]
        for name, lines, expected_text, expected_encoding in cases:
            with self.subTest(name):
                self.assert_decodes(lines, expected_text, expected_encoding)

    def test_configured_encoding_skips_detection(self):
        table = {'delimiter': ';', 'quotechar': '"', 'encoding': 'cp1250'}
        stream = mock.Mock()
        stream.iter_lines.return_value = iter(encoded(ROW, 'cp1250'))
        with mock.patch('tap_s3_csv.dialect.s3.get_file_handle'), \
                mock.patch('tap_s3_csv.dialect.preprocess.PreprocessStream', return_value=stream), \
                mock.patch('tap_s3_csv.dialect.chardet.UniversalDetector') as detector_cls:
            dialect.detect_dialect({}, {'key': 'sample.csv'}, table)
            detector_cls.assert_not_called()
        self.assertEqual(table['encoding'], 'cp1250')

    def test_empty_file_is_utf8(self):
        self.assertEqual(detect_encoding([]), 'utf-8')

    def test_utf16_guess_is_kept(self):
        # ascii text in utf-16 is also valid utf-8 byte-wise
        text = '\ufeffDealerCode;ID\r\nGÖNCZCS2;1\r\n'.encode('utf-16-le')
        self.assertTrue(detect_encoding([text]).lower().startswith('utf-16'))

    @unittest.expectedFailure
    def test_hungarian_windows_1250(self):
        # pre-existing: chardet 5.2 guesses ISO-8859-1, so ő and ű become õ and û
        self.assert_decodes(encoded('Árvíztűrő tükörfúrógép;Győr\r\n', 'cp1250', 50), 'Árvíztűrő tükörfúrógép;Győr\r\n')

    @unittest.expectedFailure
    def test_windows_1252_with_sharp_s(self):
        # pre-existing: ß drops chardet 5.2's Latin-1 confidence to 0, so the tap falls back to utf-8
        self.assert_decodes(encoded('Müller;Straße\r\n', 'cp1252', 3), 'Müller;Straße\r\n')