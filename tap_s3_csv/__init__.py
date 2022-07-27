from asyncio.log import logger
import json
import sys
import singer
import time

from singer import metadata
from tap_s3_csv.discover import discover_streams
from tap_s3_csv import s3
from tap_s3_csv.sync import sync_stream
from tap_s3_csv.config import CONFIG_CONTRACT
from tap_s3_csv import dialect

LOGGER = singer.get_logger()

REQUIRED_CONFIG_KEYS = ["bucket"]
REQUIRED_CONFIG_KEYS_EXTERNAL_SOURCE = [
    "bucket", "account_id", "external_id", "role_name"]

IMPORT_PERF_METRICS_LOG_PREFIX = "IMPORT_PERF_METRICS:"
DISCOVERY_PERF_METRICS_LOG_PREFIX = "DISCOVERY_PERF_METRICS:"


def do_discover(config):
    LOGGER.info("Starting discover")
    logMsg = f"{DISCOVERY_PERF_METRICS_LOG_PREFIX} perf-metrics-here"
    logMsg = add_metadata_to_log(config, logMsg)
    LOGGER.info(logMsg)

    streams = discover_streams(config)
    if not streams:
        raise Exception("No streams found")
    catalog = {"streams": streams}
    json.dump(catalog, sys.stdout, indent=2)
    LOGGER.info("Finished discover")


def add_metadata_to_log(config, logMsg):
    metadata = config.get('metadata', None)
    if (metadata is not None):
        org_id = metadata.get('org_id', None)
        filename = metadata.get('filename', None)
        exectuion_id = metadata.get('execution_id', None)
        logMsg = f"OrgId: {org_id} Filename: {filename} ExecutionId: {exectuion_id} " + \
            logMsg if (
                org_id is not None and filename is not None and exectuion_id is not None) else logMsg

    return logMsg


def stream_is_selected(mdata):
    return mdata.get((), {}).get('selected', False)


def do_sync(config, catalog, state):
    timers = {'pre': 0, 'bookmark': 0, 'input_files': 0, 'get_iter': 0,
              'resolve_fields': 0, 'tfm': 0, 'write_record': 0, 'write_state': 0}

    LOGGER.info('Starting sync.')

    for stream in catalog['streams']:
        start = time.time()
        stream_name = stream['tap_stream_id']
        mdata = metadata.to_map(stream['metadata'])
        table_spec = next(
            s for s in config['tables'] if s['table_name'] == stream_name)
        if not stream_is_selected(mdata):
            LOGGER.info("%s: Skipping - not selected", stream_name)
            continue

        singer.write_state(state)

        key_properties = mdata.get((), {}).get('table-key-properties', [])
        singer.write_schema(stream_name, stream['schema'], key_properties)

        timers['pre'] += time.time() - start
        LOGGER.info("%s: Starting sync", stream_name)
        counter_value = sync_stream(config, state, table_spec, stream, timers)
        LOGGER.info("%s: Completed sync (%s rows)", stream_name, counter_value)

    timers_str = ', '.join(f'"{k}": {v:.0f}' for k, v in timers.items())

    logMsg = f"{IMPORT_PERF_METRICS_LOG_PREFIX} {{{timers_str}}}"
    #logMsg = add_metadata_to_log(config, logMsg)
    LOGGER.info(logMsg)

    LOGGER.info('Done syncing.')


def validate_table_config(config):
    # Parse the incoming tables config as JSON
    tables_config = config['tables']

    for table_config in tables_config:
        if ('search_prefix' in table_config) and (table_config.get('search_prefix') is None):
            table_config.pop('search_prefix')
        if table_config.get('key_properties') == "" or table_config.get('key_properties') is None:
            table_config['key_properties'] = []
        elif table_config.get('key_properties') and isinstance(table_config['key_properties'], str):
            table_config['key_properties'] = [s.strip()
                                              for s in table_config['key_properties'].split(',')]
        if table_config.get('date_overrides') == "" or table_config.get('date_overrides') is None:
            table_config['date_overrides'] = []
        elif table_config.get('date_overrides') and isinstance(table_config['date_overrides'], str):
            table_config['date_overrides'] = [s.strip()
                                              for s in table_config['date_overrides'].split(',')]

    # Reassign the config tables to the validated object
    return CONFIG_CONTRACT(tables_config)


@singer.utils.handle_top_exception(LOGGER)
def main():
    args = singer.utils.parse_args(REQUIRED_CONFIG_KEYS)
    config = args.config

    external_source = False

    if 'external_id' in config:
        args = singer.utils.parse_args(REQUIRED_CONFIG_KEYS_EXTERNAL_SOURCE)
        config = args.config
        external_source = True

    config['tables'] = validate_table_config(config)

    # If external_id is provided, we are trying to access files in another AWS account, and need to assume the role
    if external_source:
        s3.setup_aws_client(config)
    # Otherwise, confirm that we can access the bucket in our own AWS account
    else:
        try:
            for page in s3.list_files_in_bucket(config['bucket']):
                break
        except BaseException as err:
            LOGGER.error(err)

        # If not external source, it is from importing csv (replacement for tap-csv)
        dialect.detect_tables_dialect(config)
    if args.discover:
        do_discover(args.config)
    elif args.properties:
        do_sync(config, args.properties, args.state)


if __name__ == '__main__':
    main()
