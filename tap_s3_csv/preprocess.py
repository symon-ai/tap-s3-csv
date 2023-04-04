import codecs
from queue import Queue

def get_file_iter(iterable, encoding):
        return codecs.iterdecode(
            iterable.iter_lines(), encoding=encoding, errors='replace')

class Preprocessor():
    def __init__(self, table_spec):
        self.skip_header_rows = table_spec.get('skip_header', 0)
        self.skip_footer_rows = table_spec.get('skip_footer', 0)
        self.has_header = table_spec.get('has_header', True)
        print(self.skip_header_rows)
        print(self.skip_footer_rows)
        self.first_row = None
        if self.skip_footer_rows > 0:
            self.queue = Queue(maxsize = self.skip_footer_rows)

    def preprocess_header(self, file_stream, delimiter):
        self.skip_header(file_stream)
        field_names = None
        try:
            first_row = next(file_stream).split(delimiter)
        except Exception as e:
            print(e)
            raise e

        if self.has_header:
            field_names = first_row
        else:
            field_names = [f'col_{i}' for i in range(len(first_row))]
            # store first row record
            self.first_row = dict(zip(field_names, first_row))
        print(f'field_names: {field_names}')
        print(f'first_row: {self.first_row}')

        return field_names
    
    def skip_header(self, file_stream):
        if self.skip_header_rows > 0:
            try:
                for i in range(self.skip_header_rows):
                    next(file_stream)
            except StopIteration:
                #TODO improve err msg
                raise Exception('Skipped all rows in the data')

    def preprocess_row(self, row):
        # no footer rows to skip
        if self.skip_footer_rows <= 0:
            return row
        
        # need to skip rows in footer. put the row into the queue and remove when queue is full
        cur_row = None
        if self.queue.full():
            cur_row = self.queue.get()
        
        self.queue.put(row)
        print('---queue---')
        print(self.queue.qsize())
        print(list(self.queue.queue))
        return cur_row
    
    # def handle_first_row(self):
    #     # first row already parsed as header, don't need to handle first row
    #     if self.has_header:
    #         return
        
    #     # first row needs to be processed
    #     if self.first_row is not None:
    #         # no footer rows to skip, no queue needed
    #         if self.skip_footer_rows <= 0:
    #             return self.first_row
    #         # need to skip rows in footer, put first row into the queue
    #         self.queue.put(self.first_row)
            
            

        
    



                


        
