import singer

LOGGER = singer.get_logger()


def infer(datum):
    """
    Returns the inferred data type
    """
    if datum is None or datum == '':
        return None

    try:
        int(datum)
        return 'integer'
    except (ValueError, TypeError):
        pass

    try:
        # numbers are NOT floats, they are DECIMALS
        float(datum)
        return 'number'
    except (ValueError, TypeError):
        pass

    return 'string'


def count_sample(sample, counts, table_spec):
    for key, value in sample.items():
        length = len(value)

        if key not in counts:
            counts[key] = ({}, length)
        elif length > counts[key][1]:
            counts[key][1] = length

        date_overrides = table_spec.get('date_overrides', [])
        if key in date_overrides:
            datatype = "date-time"
        else:
            datatype = infer(value)

        if datatype is not None:
            counts[key][0][datatype] = counts[key][0].get(datatype, 0) + 1

    return counts


def pick_datatype(counts):
    """
    If the underlying records are ONLY of type `integer`, `number`,
    or `date-time`, then return that datatype.

    If the underlying records are of type `integer` and `number` only,
    return `number`.

    Otherwise return `string`.
    """
    to_return = 'string'

    if counts.get('date-time', 0) > 0:
        return 'date-time'

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
    counts = {}
    for sample in samples:
        # {'name' : { 'string' : 45}}
        counts = count_sample(sample, counts, table_spec)

    schema = {}
    for key, value in counts.items():
        datatype = pick_datatype(value[0])

        if datatype == 'date-time':
            schema[key] = {
                'anyOf': [
                    {'type': ['null', 'string'], 'format': 'date-time'},
                    {'type': ['null', 'string']}
                ]
            }
            if string_max_length:
                schema[key]['anyOf'][1]['maxLength'] = value[1]
        else:
            types = ['null', datatype]
            if datatype != 'string':
                types.append('string')
            schema[key] = {'type': types}
            if string_max_length:
                schema[key]['maxLength'] = value[1]

    return schema
