import codecs
import csv
from tap_s3_csv.symon_exception import SymonException
import itertools

MAX_COL_LENGTH = 150


def get_row_iterator(iterable, options=None, fieldnames=None, row_limit=None):
    options = options or {}
    iterable_lines = itertools.islice(iterable.iter_lines(), row_limit) if row_limit is not None else iterable.iter_lines()

    file_stream = codecs.iterdecode(
        iterable_lines, encoding=options.get('encoding', 'utf-8'), errors='replace')

    # Replace any NULL bytes in the line given to the DictReader
    reader = csv.DictReader(
        (line.replace('\0', '') for line in file_stream),
        fieldnames=fieldnames,
        delimiter=options.get('delimiter', ','),
        escapechar=options.get('escape_char', '\\'),
        quotechar=options.get('quotechar', '"'))

    if (reader.fieldnames is None):
        raise SymonException('File is empty.', 'EmptyFile')

    reader.fieldnames = truncate_headers(reader.fieldnames)
    headers = set(reader.fieldnames)

    reader.fieldnames = handle_empty_fieldnames(
        reader.fieldnames, options)

    # We do not use key_properties and date_overrides in our config. If we use these later, will need to add code for checking over
    # whether fieldnames included in key_properties/date_overrides have been modified in handle_empty_fieldnames and handle appropriately.
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


# truncate headers that are longer than MAX_COL_LENGTH, then handle duplicates
def truncate_headers(fieldnames):
    # trim white spaces before checking for duplicates.
    fieldnames = [fieldname.strip() if isinstance(fieldname, str) else fieldname for fieldname in fieldnames]
    # stores final fieldname to use for each index (key: fieldname, value: index)
    fieldname_to_index = {}

    # update fieldname_to_index to include first occurring index of column name for column names that don't need truncation
    for index, fieldname in enumerate(fieldnames):
        if fieldname is None or fieldname == '' or len(fieldname) > MAX_COL_LENGTH:
            continue

        fieldname_lowercase = fieldname.casefold()

        if fieldname_lowercase not in fieldname_to_index:
            fieldname_to_index[fieldname_lowercase] = index

    # update fieldname_to_index map to include first occurring index of column name for column names that need truncation
    for index, fieldname in enumerate(fieldnames):
        if fieldname is None or fieldname == '' or len(fieldname) <= MAX_COL_LENGTH:
            continue

        fieldname = fieldname[:MAX_COL_LENGTH]
        fieldname_lowercase = fieldname.casefold()

        if fieldname_lowercase not in fieldname_to_index:
            fieldname_to_index[fieldname_lowercase] = index

    # 4 chars are reserved for "_xxx" used to resolve duplicate names
    max_col_length_dup = MAX_COL_LENGTH - 4
    duplicates_next_id = {}

    # loops through and updates fieldnames, resolving duplicate column names
    #   if index is the first occurring index for given fieldname, use it
    #   else, resolve duplicate by adding "_{id}"
    for index, fieldname in enumerate(fieldnames):
        if fieldname is None or fieldname == '':
            continue

        if len(fieldname) > MAX_COL_LENGTH:
            fieldname = fieldname[:MAX_COL_LENGTH]

        fieldname_lowercase = fieldname.casefold()

        if fieldname_to_index.get(fieldname_lowercase, -1) == index:
            fieldnames[index] = fieldname
        else:
            fieldname = fieldname[:max_col_length_dup]
            fieldname_lowercase = fieldname.casefold()
            duplicate_id = duplicates_next_id.get(fieldname_lowercase, 0)

            while f'{fieldname_lowercase}_{duplicate_id}' in fieldname_to_index:
                duplicate_id += 1

            fieldname_to_index[f'{fieldname_lowercase}_{duplicate_id}'] = index
            fieldnames[index] = f'{fieldname}_{duplicate_id}'
            duplicates_next_id[fieldname_lowercase] = duplicate_id + 1
    
    return fieldnames


# Generates column name for columns without header
def handle_empty_fieldnames(fieldnames, options):
    quotechar = options.get('quotechar', '"')
    delimiter = options.get('delimiter', ',')
    is_csv_connector_import = options.get('is_csv_connector_import',  False)

    fieldname_pool = set([fieldname.casefold() if isinstance(fieldname, str) else fieldname for fieldname in fieldnames])

    auto_generate_header_num = 0
    final_fieldnames = []
    for fieldname in fieldnames:
        # handle edge case uncovered in WP-9866 for csv import
        if is_csv_connector_import and fieldname and delimiter in fieldname and fieldname[0] != quotechar and fieldname[-1] != quotechar:
            fieldname = quotechar + fieldname + quotechar

        if fieldname == '' or fieldname is None:
            fieldname = f'col_{auto_generate_header_num}'
            while fieldname in fieldname_pool:
                auto_generate_header_num += 1
                fieldname = f'col_{auto_generate_header_num}'
            auto_generate_header_num += 1

        final_fieldnames.append(fieldname)

    return final_fieldnames
