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
    fieldname_pool = set()
    duplicate_cols = set()
    if len(reader.fieldnames) != len(headers):
        for fieldname in reader.fieldnames:
            if fieldname == '':
                continue
            fieldname = fieldname.casefold()
            if fieldname in fieldname_pool:
                duplicate_cols.add(fieldname)
            else:
                fieldname_pool.add(fieldname)

        if len(duplicate_cols) > 0:
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

    final_fieldnames = handle_empty_fieldnames(reader.fieldnames, fieldname_pool, options)

    setattr(reader, '_fieldnames', final_fieldnames)

    return reader

def handle_empty_fieldnames(fieldnames, fieldname_pool, options):
    quotechar = options.get('quotechar', '"')
    delimiter = options.get('delimiter', ',')
    is_external = options.get('is_external',  True)

    auto_generate_header_num = 0
    final_fieldnames = []
    for fieldname in fieldnames:
        # handle edge case uncovered in WP-9886 for csv import
        # import is from csv connector (not s3 connector) if it is_external is set to False
        if not is_external and fieldname and delimiter in fieldname:
            fieldname = quotechar + fieldname + quotechar
                
        if fieldname == '' or fieldname is None:
            fieldname = f'col_{auto_generate_header_num}'
            while fieldname in fieldname_pool:
                auto_generate_header_num += 1
                fieldname = f'col_{auto_generate_header_num}'
            auto_generate_header_num += 1
        
        final_fieldnames.append(fieldname)
    
    return final_fieldnames
