import codecs
import csv

MAX_COL_LENGTH = 150


def get_row_iterator(iterable, options=None):
    options = options or {}
    file_stream = codecs.iterdecode(
        iterable.iter_lines(), encoding=options.get('encoding', 'utf-8'), errors='replace')

    field_names = None

    # Replace any NULL bytes in the line given to the DictReader
    reader = csv.DictReader(
        (line.replace('\0', '') for line in file_stream),
        fieldnames=field_names,
        delimiter=options.get('delimiter', ','),
        escapechar=options.get('escape_char', '\\'),
        quotechar=options.get('quotechar', '"'))

    if (reader.fieldnames is None):
        raise Exception('File is empty.')

    reader.fieldnames, fieldname_pool = truncate_headers(reader.fieldnames)
    headers = set(reader.fieldnames)

    reader.fieldnames = handle_empty_fieldnames(
        reader.fieldnames, fieldname_pool, options)

    # csv.DictReader skips empty rows, but we wish to keep empty rows for csv imports, so override __next__ method.
    # Only modifying for imports from csv connector for now as imports from s3 connector might have reasons for skipping empty rows.
    # Could look into using csv.reader instead for cleaner code if s3 connector could also keep empty rows.
    if options.get('is_csv_connector_import', False):
        csv.DictReader.__next__ = next_without_skip

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
    fieldname_pool = set()
    new_fieldnames = []
    duplicates_exist = False
    
    # map to keep first occurrence index of each column name 
    fieldname_first_occur = {}
    truncated_fieldname_first_occur = {}

    for index, fieldname in enumerate(fieldnames):
        if fieldname is None or fieldname == '':
            new_fieldnames.append(fieldname)
            continue

        truncated = False
        if len(fieldname) > MAX_COL_LENGTH:
            fieldname = fieldname[:MAX_COL_LENGTH]
            truncated = True
        
        fieldname_lowercase = fieldname.casefold()
        if truncated and fieldname_lowercase not in truncated_fieldname_first_occur:
            truncated_fieldname_first_occur[fieldname_lowercase] = index
        elif not truncated and fieldname_lowercase not in fieldname_first_occur:
            fieldname_first_occur[fieldname_lowercase] = index

        if fieldname_lowercase not in fieldname_pool:
            fieldname_pool.add(fieldname_lowercase)
            new_fieldnames.append(fieldname)
        else:
            duplicates_exist = True

    if not duplicates_exist:
        return new_fieldnames, fieldname_pool

    # update fieldname_first_occur map to include first occur index of truncated fieldnames
    # when we resolve duplicates, unmodified fieldnames should take priority over truncated fieldnames
    for truncated_fieldname in truncated_fieldname_first_occur:
        if truncated_fieldname not in fieldname_first_occur:
            fieldname_first_occur[truncated_fieldname] = truncated_fieldname_first_occur[truncated_fieldname]

    # 4 chars are reserved for "_xxx" used to resolve duplicate names
    max_col_length_dup = MAX_COL_LENGTH - 4
    new_fieldnames = []
    duplicates_next_id = {} # keeps next id to use for duplicate column names

    for index, fieldname in enumerate(fieldnames):
        if fieldname is None or fieldname == '':
            new_fieldnames.append(fieldname)
            continue
        
        if len(fieldname) > MAX_COL_LENGTH:
            fieldname = fieldname[:MAX_COL_LENGTH]

        if fieldname_first_occur.get(fieldname.casefold(), -1) == index:
            new_fieldnames.append(fieldname)
        else:
            # need to resolve duplicates, reserve last 4 chars for "_xxx" if truncated
            fieldname = fieldname[: min(len(fieldname), max_col_length_dup)]
            fieldname_lowercase = fieldname.casefold()
            duplicate_id = duplicates_next_id.get(fieldname_lowercase, 0)

            while f'{fieldname_lowercase}_{duplicate_id}' in fieldname_pool:
                duplicate_id += 1
            
            fieldname_pool.add(f'{fieldname_lowercase}_{duplicate_id}')
            new_fieldnames.append(f'{fieldname}_{duplicate_id}')
            duplicates_next_id[fieldname_lowercase] = duplicate_id + 1

    return new_fieldnames, fieldname_pool


# Generates column name for columns without header
def handle_empty_fieldnames(fieldnames, fieldname_pool, options):
    quotechar = options.get('quotechar', '"')
    delimiter = options.get('delimiter', ',')
    is_csv_connector_import = options.get('is_csv_connector_import',  False)

    auto_generate_header_num = 0
    final_fieldnames = []
    for fieldname in fieldnames:
        # handle edge case uncovered in WP-9886 for csv import
        if is_csv_connector_import and fieldname and delimiter in fieldname:
            fieldname = quotechar + fieldname + quotechar

        if fieldname == '' or fieldname is None:
            fieldname = f'col_{auto_generate_header_num}'
            while fieldname in fieldname_pool:
                auto_generate_header_num += 1
                fieldname = f'col_{auto_generate_header_num}'
            auto_generate_header_num += 1

        final_fieldnames.append(fieldname)

    return final_fieldnames

# csv.DictReader class skips empty rows when iterating over file stream.
# method to use for overriding csv.DictReader.__next__ in case we wish to keep empty rows


def next_without_skip(self):
    if self.line_num == 0:
        # Used only for its side effect.
        self.fieldnames

    row = next(self.reader)
    self.line_num = self.reader.line_num

    d = dict(zip(self.fieldnames, row))
    lf = len(self.fieldnames)
    lr = len(row)
    if lf < lr:
        d[self.restkey] = row[lf:]
    elif lf > lr:
        for key in self.fieldnames[lr:]:
            d[key] = self.restval
    return d
