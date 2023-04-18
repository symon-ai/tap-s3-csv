from queue import Queue
import csv
import codecs

# Wrapper class for file streams. Handles preprocessing (skipping header rows, footer rows, detecting headers)
class PreprocessStream():
    def __init__(self, file_handle, table_spec, handle_first_row):
        self.file_iterator = file_handle.iter_lines()
        self.first_row = None
        self.queue = None
        self.header = None

        skip_header_row = table_spec.get('skip_header_row', 0)
        skip_footer_row = table_spec.get('skip_footer_row', 0)

        if skip_header_row > 0:
            self._skip_header_rows(skip_header_row)
        if skip_footer_row > 0:
            self.queue = Queue(maxsize = skip_footer_row)
        if handle_first_row:
            has_header = table_spec.get('has_header', True)
            encoding = table_spec.get('encoding', 'utf-8')
            delimiter = table_spec.get('delimiter', ',')
            quotechar = table_spec.get('quotechar', '"')
            escapechar = table_spec.get('escape_char', '\\')
            self._handle_first_row(has_header, encoding, delimiter, quotechar, escapechar)
    
    def _skip_header_rows(self, skip_header_row):
        try:
            for _ in range(skip_header_row):
                next(self.file_iterator)
        except StopIteration:
            raise Exception(f'preprocess_err: We can’t find any data after the skipped rows in the header.')
    
    # grabs first non empty row and process it as header row or first record row depending on has_header
    def _handle_first_row(self, has_header, encoding, delimiter, quotechar, escapechar):
        # Use csv.DictReader to parse first row and use it as header if has_header == True, else 
        # use it to detect the number of columns and generate headers. We need to use csv.DictReader
        # for parsing first row to handle corner cases such as:
        # - fields in first row contain newline char wrapped with quotechar or escaped with escapechar
        # - fields in first row contain delimiter wrapped with quotechar or escaped with escapechar
        file_stream = codecs.iterdecode(
            self.file_iterator, encoding=encoding, errors='replace')
        reader = csv.DictReader(
            (line.replace('\0', '') for line in file_stream),
            fieldnames=None,
            delimiter=delimiter,
            escapechar=escapechar,
            quotechar=quotechar)
        
        if reader.fieldnames is None:
            raise Exception('File is empty.')

        first_row_parsed = reader.fieldnames

        # first row is header row
        if has_header:
            self.header = first_row_parsed
            return
        
        # first row is a record, generate headers 
        self.header = [f'col_{i}' for i in range(len(first_row_parsed))]
        
        # first row has been parsed into array of string, change it back to byte form to yield in iter_lines.
        first_row_str_form = ''
        for field in first_row_parsed:
            # Each field has been parsed into final value by csv.Dictreader after handling delimiters, quotechars and escapechars.
            # Since csv.DictReader uses quotechar to quote fields containing special chars, such as the delimiter/quote char/newline char,
            # we can simply wrap fields with quotechar so that csv.DictReader can later parse them into same value.
            # Make sure to escape escapechars and quotechars that are actually part of fieldname
            field = field.replace(escapechar, escapechar * 2).replace(quotechar, escapechar + quotechar)
            first_row_str_form += quotechar + field + quotechar + delimiter
        encoder = codecs.getincrementalencoder(encoding)()
        first_row_byte_form = encoder.encode(first_row_str_form[:-1])
        self.first_row = first_row_byte_form

    def iter_lines(self):
        if self.first_row is not None:
            if self.queue is None:
                yield self.first_row
                self.first_row = None
            else:
                self.queue.put(self.first_row)
        
        for row in self.file_iterator:
            if self.queue is None:
                yield row
            else:
                if self.queue.full():
                    yield self.queue.get()
                self.queue.put(row)
