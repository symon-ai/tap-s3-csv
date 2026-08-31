import json
import sys
import singer
import time
import traceback
import boto3

from botocore.exceptions import ClientError
from singer import metadata
from tap_s3_csv.discover import discover_streams
from tap_s3_csv import s3
from tap_s3_csv.sync import sync_stream
from tap_s3_csv.config import CONFIG_CONTRACT
from tap_s3_csv import dialect
from tap_s3_csv import aws_auth
from tap_s3_csv.symon_exception import SymonException

LOGGER = singer.get_logger()

BASE_REQUIRED_CONFIG_KEYS = aws_auth.get_required_config_keys(None)

IMPORT_PERF_METRICS_LOG_PREFIX = "IMPORT_PERF_METRICS:"

# for symon error logging
ERROR_START_MARKER = '[tap_error_start]'
ERROR_END_MARKER = '[tap_error_end]'


def _count_singer_col_types(schema: dict) -> tuple:
    """Count column types from Singer JSON Schema properties."""
    cols_string_count = cols_numeric_count = cols_datetime_count = cols_bool_count = 0
    for _, prop_def in schema.get('properties', {}).items():
        prop_type = prop_def.get('type', [])
        prop_format = prop_def.get('format', '')
        if isinstance(prop_type, str):
            prop_type = [prop_type]
        types = [t for t in prop_type if t != 'null']
        primary_type = types[0] if types else 'string'
        if prop_format in ('date-time', 'date'):
            cols_datetime_count += 1
        elif primary_type == 'boolean':
            cols_bool_count += 1
        elif primary_type in ('integer', 'number'):
            cols_numeric_count += 1
        elif prop_format == 'singer.decimal':
            cols_numeric_count += 1
        else:
            cols_string_count += 1
    return cols_string_count, cols_numeric_count, cols_datetime_count, cols_bool_count


def write_export_metrics(
    bucket,
    key,
    row_count,
    col_count,
    cols_string_count=0,
    cols_numeric_count=0,
    cols_datetime_count=0,
    cols_bool_count=0,
):
    """
    Write export metrics JSON to S3 for later aggregation.
    Non-breaking: wrapped in try/catch, logs errors but doesn't raise.
    """
    try:
        s3_client = boto3.client('s3')
        metrics = {
            "metricType": "EXPORT",
            "rowCount": row_count,
            "colCount": col_count,
            "colsStringCount": cols_string_count,
            "colsNumericCount": cols_numeric_count,
            "colsDatetimeCount": cols_datetime_count,
            "colsBoolCount": cols_bool_count,
        }
        s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=json.dumps(metrics),
            ContentType="application/json"
        )
        LOGGER.info(f"Wrote export metrics to s3://{bucket}/{key}")
        return True
    except Exception as e:
        LOGGER.warning(f"Failed to write export metrics (non-breaking): {e}")
        return False


def do_discover(config):
    LOGGER.info("Starting discover")

    streams = discover_streams(config)
    if not streams:
        raise Exception("No streams found")
    catalog = {"streams": streams}
    json.dump(catalog, sys.stdout, indent=2)
    LOGGER.info("Finished discover")


def stream_is_selected(mdata):
    return mdata.get((), {}).get('selected', False)


def do_sync(config, catalog, state):
    start_byte = config.get('start_byte')
    end_byte = config.get('end_byte')
    range_size = config.get('range_size', 1024*1024*5)
    json_lib = config.get('json_lib', 'orjson')
    row_limit = config.get('row_limit', None)

    # Export logs for row and col count
    total_col_count = 0
    total_cols_string_count = 0
    total_cols_numeric_count = 0
    total_cols_datetime_count = 0
    total_cols_bool_count = 0
    current_col_count = 0
    total_row_count = 0
    grouped_logs=[]
    name=""
    tables_config = config['tables']
    # Export logs for row and col count (multisheet files)
    for table_config in tables_config:
        if 'search_prefix' in table_config:
            name=name+table_config["search_prefix"]

    LOGGER.info(f'Starting sync ({start_byte}-{end_byte}).')

    for stream in catalog['streams']:
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

        LOGGER.info("%s: Starting sync", stream_name)
        counter_value = sync_stream(
            config, state, table_spec, stream, start_byte, end_byte, range_size, json_lib)
        # Exports logs for row and col count
        if "properties" in stream['schema']:
            current_col_count = len(stream['schema']["properties"].items())
            total_col_count += current_col_count
            _cs, _cn, _cd, _cb = _count_singer_col_types(stream['schema'])
            total_cols_string_count += _cs
            total_cols_numeric_count += _cn
            total_cols_datetime_count += _cd
            total_cols_bool_count += _cb
            json_row_col = {"name": name, "stream_id":stream_name, "row": counter_value, "col": current_col_count}
            grouped_logs.append("individual_file_data_props: " + str(json_row_col))
        total_row_count += counter_value
        LOGGER.info("%s: Completed sync (%s rows)", stream_name, counter_value)
        

    # import performance logging - left here for convenience
    # timers_str = ', '.join(f'"{k}": {v:.0f}' for k, v in timers.items())
    # logMsg = f"{IMPORT_PERF_METRICS_LOG_PREFIX} {{{timers_str}}}"
    # LOGGER.info(logMsg)
    
    # Exports logs for row and col count
    json_row_col = {"name": name, "row": total_row_count, "col": total_col_count}
    grouped_logs.insert(0,"EXPORTS tap-s3-csv data_props: " + str(json_row_col))
    LOGGER.info("| ".join(grouped_logs))

    # Write export metrics if S3 path is provided (either dict with bucket/key or s3:// URL string)
    export_metrics_s3_path = config.get('export_metrics_s3_path', None)
    if export_metrics_s3_path:
        metrics_bucket = None
        metrics_key = None

        if isinstance(export_metrics_s3_path, dict):
            metrics_bucket = export_metrics_s3_path.get('bucket')
            metrics_key = export_metrics_s3_path.get('key')
        elif isinstance(export_metrics_s3_path, str):
            # Allow formats like "s3://bucket/key" or "bucket/key"
            path = export_metrics_s3_path
            if path.startswith("s3://"):
                path = path[5:]
            if "/" in path:
                metrics_bucket, metrics_key = path.split("/", 1)

        if metrics_bucket and metrics_key:
            write_export_metrics(
                metrics_bucket,
                metrics_key,
                total_row_count,
                total_col_count,
                cols_string_count=total_cols_string_count,
                cols_numeric_count=total_cols_numeric_count,
                cols_datetime_count=total_cols_datetime_count,
                cols_bool_count=total_cols_bool_count,
            )

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
        # if row_limit is provided, validate that it is a non negative integer
        if table_config.get('row_limit') is not None:
            if not isinstance(table_config['row_limit'], int) or table_config['row_limit'] < 0:
                raise Exception(
                    'row_limit must be a non-negative integer')

    # Reassign the config tables to the validated object
    return CONFIG_CONTRACT(tables_config)


@singer.utils.handle_top_exception(LOGGER)
def main():
    try:
        # used for storing error info to write if error occurs
        error_info = None
        args = singer.utils.parse_args(BASE_REQUIRED_CONFIG_KEYS)
        config = args.config

        auth_method = aws_auth.resolve_auth_method(config)
        aws_auth.validate_auth_config(config, auth_method)
        required_config_keys = aws_auth.get_required_config_keys(auth_method)
        if required_config_keys != BASE_REQUIRED_CONFIG_KEYS:
            args = singer.utils.parse_args(required_config_keys)
            config = args.config

        uses_external_source = auth_method is not None

        config['tables'] = validate_table_config(config)

        try:
            if uses_external_source:
                # Customer-owned external S3 requires the configured customer credentials.
                if auth_method == aws_auth.AUTH_METHOD_ACCESS_KEY:
                    s3.setup_external_source_with_aws_access_key(config)
                else:
                    s3.setup_external_source_with_aws_role_assumption(config)
            else:
                # Internal platform S3 uses the ambient worker credentials.
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
        except ClientError as e:
            if not uses_external_source:
                raise
            raise s3.build_symon_exception_from_client_error(e, config.get('bucket')) from e
    except SymonException as e:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        error_info = {
            'message': traceback.format_exception_only(exc_type, exc_value)[-1],
            'code': e.code,
            'traceback': "".join(traceback.format_tb(exc_traceback))
        }

        if e.details is not None:
            error_info['details'] = e.details
        raise
    except BaseException as e:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        error_info = {
            'message': traceback.format_exception_only(exc_type, exc_value)[-1],
            'traceback': "".join(traceback.format_tb(exc_traceback))
        }
        raise
    finally:
        if error_info is not None:
            try:
                error_file_path = args.config.get('error_file_path', None)
                if error_file_path is not None:
                    try:
                        with open(error_file_path, 'w', encoding='utf-8') as fp:
                            json.dump(error_info, fp)
                    except:
                        pass
                # log error info as well in case file is corrupted
                error_info_json = json.dumps(error_info)
                error_start_marker = args.config.get('error_start_marker', ERROR_START_MARKER)
                error_end_marker = args.config.get('error_end_marker', ERROR_END_MARKER)
                LOGGER.info(f'{error_start_marker}{error_info_json}{error_end_marker}')
            except:
                # error occurred before args was parsed correctly, log the error
                error_info_json = json.dumps(error_info)
                LOGGER.info(f'{ERROR_START_MARKER}{error_info_json}{ERROR_END_MARKER}')


if __name__ == '__main__':
    main()
