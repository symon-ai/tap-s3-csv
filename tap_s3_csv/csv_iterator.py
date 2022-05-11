import codecs
import csv

def get_row_iterator(iterable, options=None):
    options = options or {}
    file_stream = codecs.iterdecode(iterable, encoding=options.get('encoding', 'utf-8'))

    field_names = None

    # Replace any NULL bytes in the line given to the DictReader
    reader = csv.DictReader(
        (line.replace('\0', '') for line in file_stream), 
        fieldnames=field_names, 
        delimiter=options.get('delimiter', ','),
        escapechar=options.get('escape_char', '\\'),
        quotechar=options.get('quotechar', '"'))

    headers = set(reader.fieldnames)

    # Check for duplicate columns
    if len(reader.fieldnames) != len(headers):
        fieldname_count = {}
        duplicate_cols = set()
        for fieldname in reader.fieldnames:
            fieldname_count[fieldname] = fieldname_count.get(fieldname, 0) + 1
            if fieldname_count[fieldname] > 1:
                duplicate_cols.add(fieldname)
        ', '.join(duplicate_cols)

        raise Exception('CSV file contains duplicate columns: {}'.format(duplicate_cols))

    if options.get('key_properties'):
        key_properties = set(options['key_properties'])
        if not key_properties.issubset(headers):
            raise Exception('CSV file missing required headers: {}, file only contains headers for fields: {}'
                            .format(key_properties - headers, headers))

    if options.get('date_overrides'):
        date_overrides = set(options['date_overrides'])
        if not date_overrides.issubset(headers):
            raise Exception('CSV file missing date_overrides headers: {}, file only contains headers for fields: {}'
                            .format(date_overrides - headers, headers))

    return reader
