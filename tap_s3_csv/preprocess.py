from queue import Queue

# Wrapper class for file streams. Handles preprocessing (skipping header rows, footer rows, detecting headers)
class PreprocessStream():
    def __init__(self, file_handle, table_spec):
        # self.file_iterator = iter(file_handle.iter_lines())
        self.file_iterator = file_handle.iter_lines()
        self.first_row = None
        self.queue = None

        skip_header_row = table_spec.get('skip_header_row', 0)
        skip_footer_row = table_spec.get('skip_footer_row', 0)

        if skip_header_row > 0:
            self._skip_header_rows(skip_header_row)
        if skip_footer_row > 0:
            self.queue = Queue(maxsize = skip_footer_row)
    
    def get_headers(self, table_spec):
        has_header = table_spec.get('has_header', True)
        fieldnames = None

        # if headers exist, let csv.DictReader grab the header. csv.DictReader automatically skips empty rows
        # and uses first valid row as header
        if has_header:
            return fieldnames
        
        first_row = self._skip_empty_rows()
        delimiter = table_spec.get('delimiter', ',')
        encoding = table_spec.get('encoding', 'utf-8')

        first_row_list = first_row.decode(encoding).split(delimiter)
        fieldnames = [f'col_{i}' for i in range(len(first_row_list))]
        
        # has_header is false, so first row is record, not header. save it to yield later
        self.first_row = first_row

        return fieldnames

    def move_to_first_row(self, has_header):
        first_row = self._skip_empty_rows()
        if not has_header:
            # has_header is false, so first row is record, not header. save it to yield later
            self.first_row = first_row

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

    def _skip_header_rows(self, skip_header_row):
        try:
            for _ in range(skip_header_row):
                line = next(self.file_iterator)
                print(line)
        except StopIteration:
            #TODO improve err msg
            raise Exception(f'No more data after skipping rows in header')

    def _skip_empty_rows(self):
        try:
            first_row = next(self.file_iterator)
            while first_row == b'':
                first_row = next(self.file_iterator)
        except StopIteration:
            raise Exception(f'No more data other than empty rows')
        return first_row
