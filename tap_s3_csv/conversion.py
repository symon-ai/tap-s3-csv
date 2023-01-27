import singer
import pandas as pd
import numpy as np

LOGGER = singer.get_logger()

# pylint: disable=too-many-return-statements


def infer(key, datum, date_overrides, check_second_call=False):
    """
    Returns the inferred data type
    """
    if datum is None or datum == '':
        return None

    try:
        if isinstance(datum, list):
            data_type = 'string'
            if check_second_call:
                LOGGER.warning(
                    'Unsupported type for "%s", List inside list is not supported hence will be treated as a string', key)
            elif not datum:
                data_type = 'list'
            else:
                inferred = infer(key, datum[0], date_overrides, True)
                # default it to string if element inside datum list is None
                if inferred is None: 
                    inferred = 'string'
                data_type = 'list.' + inferred
            return data_type

        if key in date_overrides:
            return 'date-time'

        if isinstance(datum, dict):
            return 'dict'

        try:
            int(str(datum))
            return 'integer'
        except (ValueError, TypeError):
            pass
        try:
            float(str(datum))
            return 'number'
        except (ValueError, TypeError):
            pass

    except (ValueError, TypeError):
        pass

    return 'string'

def infer_column(column, dateFormatMap, lengths):
    if column.isnull().all():
        lengths[column.name] = 0
        return 'string'

    lengths[column.name] = column.apply(lambda x: len(str(x))).max()

    if column.dtype.name == 'object':
        # Check for list types (occurs from csv.DictReader if data row has more columns than headers)
        if column.apply(lambda x: isinstance(x, list)).any():
            return 'list'

        if infer_number(column):
            return 'number'
        elif infer_datetime(column, dateFormatMap):
            return 'date-time'
        elif infer_boolean(column):
            return 'boolean'
        else:
            return 'string'

    if column.dtype.name in ['int32', 'int64', 'float32']:
        return 'number'

def infer_number(column):
    tmpCol = column.copy()
    tmpResult = pd.to_numeric(tmpCol, errors='ignore')

    if tmpResult.dtype.name in ['float64', 'int64']:
        return True
    else:
        return False

def infer_datetime(column, dateFormatMap):
    tmpCol = column.copy()
    # SalesForce exports empty dates as <NULL>
    tmpCol.replace('(?i)<null>', '', inplace=True, regex=True)
    # pandas does not check the format properly
    return infer_datetime_and_format(tmpCol, dateFormatMap)


def infer_datetime_and_format(column, dateFormatMap):
    try:
        # Formats '%Y-%m-%d' and '%Y/%m/%d' seem work interchangeably in pd.to_datetime function
        # e.g '2022-01-02' and '2022/01/02' would both pass pd.to_datetime using formats '%Y-%m-%d'
        # and '%Y/%m/%d'
        # Choose one cell to check if '/' is in the value and update dateFormatMap correctly
        cell = column[column.astype(bool)].min() # Only consider non-blank rows
        column = pd.to_datetime(column, format='%Y-%m-%d')
        dateFormatMap[column.name] = 'YYYY/MM/DD' if '/' in cell else 'YYYY-MM-DD'
        return True
    except Exception as e:
        pass

    try:
        column = pd.to_datetime(column, format='%m-%d-%Y')
        dateFormatMap[column.name] = 'MM-DD-YYYY'
        return False
    except Exception as e:
        pass

    try:
        column = pd.to_datetime(column, format='%d-%m-%Y')
        dateFormatMap[column.name] = 'DD-MM-YYYY'
        return False
    except Exception as e:
        pass

    try:
        column = pd.to_datetime(column, format='%m/%d/%Y')
        dateFormatMap[column.name] = 'MM/DD/YYYY'
        return False
    except Exception as e:
        pass

    try:
        column = pd.to_datetime(column, format='%d/%m/%Y')
        dateFormatMap[column.name] = 'DD/MM/YYYY'
        return False
    except Exception as e:
        pass

    return False


def infer_boolean(column):
    unique_values = column.unique()

    if len(unique_values) == 0:
        return False

    # If there's only one value and it's blank/missing, then it's not a boolean column
    if len(unique_values) == 1 and pd.isna(unique_values[0]):
        return False

    # All values must be boolean or blank/missing
    for value in unique_values:
        if not is_boolean_value(value) and not pd.isna(value):
            return False

    return True


def is_boolean_value(value):

    if value == 'True' or value == 'False':
        return True

    if value is True or value is np.True_:
        return True

    if value is False or value is np.False_:
        return True

    return False


def process_sample(sample, counts, lengths, table_spec):
    for key, value in sample.items():
        if key not in counts:
            counts[key] = {}

        length = len(value) if value is not None else 0
        if key not in lengths or length > lengths[key]:
            lengths[key] = length

        date_overrides = table_spec.get('date_overrides', [])
        datatype = infer(key, value, date_overrides)

        if datatype is not None:
            counts[key][datatype] = counts[key].get(datatype, 0) + 1

    return counts, lengths


def pick_datatype(counts):
    """
    If the underlying records are ONLY of type `integer`, `number`,
    or `date-time`, then return that datatype.

    If the underlying records are of type `integer` and `number` only,
    return `number`.

    Otherwise return `string`.
    """
    to_return = 'string'

    list_of_datatypes = ['list.date-time', 'list.dict', 'list.integer',
                         'list.number', 'list.string', 'list', 'date-time', 'dict']

    for data_types in list_of_datatypes:
        if counts.get(data_types, 0) > 0:
            return data_types

    if len(counts) == 1:
        if counts.get('integer', 0) > 0:
            to_return = 'integer'
        elif counts.get('number', 0) > 0:
            to_return = 'number'
    elif(len(counts) == 2 and
         counts.get('integer', 0) > 0 and
         counts.get('number', 0) > 0):
        to_return = 'number'

    return to_return


def generate_schema(samples, table_spec, string_max_length: bool):
    df = pd.DataFrame(samples)
    schema = {}
    date_format_map = {} # Stores date formats for any columns that can be interpretted as dates
    lengths = {} # Stores the maximum length of strings in each column
    for colName in df.columns:
        schema[colName] = infer_column(df[colName], date_format_map, lengths)

    for key, datatype in schema.items():
        # Ignore list datatypes (autogenerated if rows have more columns than header row)
        if datatype != 'list':
            schema[key] = datatype_schema(datatype, lengths[key], string_max_length)

    return schema, date_format_map


def datatype_schema(datatype, length, string_max_length: bool):
    if datatype == 'date-time':
        schema = {
            'anyOf': [
                {'type': ['null', 'string'], 'format': 'date-time'},
                {'type': ['null', 'string']}
            ]
        }
        if string_max_length:
            schema['anyOf'][1]['maxLength'] = length
    elif datatype == 'dict':
        schema = {
            'anyOf': [
                {'type': 'object', 'properties': {}},
                {'type': ['null', 'string']}
            ]
        }
        if string_max_length:
            schema['anyOf'][1]['maxLength'] = length
    else:
        types = ['null', datatype]
        if datatype != 'string':
            types.append('string')
        schema = {
            'type': types,
        }
        if string_max_length:
            schema['maxLength'] = length
    return schema
