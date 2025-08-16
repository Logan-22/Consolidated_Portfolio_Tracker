from flask import current_app
from datetime import date
from utils.sql_utils.process.fetch_queries import fetch_queries_as_dictionaries
from utils.sql_utils.query_db.get_or_process_in_db import get_component_info_from_db
from utils.connection_utils.connection_pool_config import connection_pool

def delsert_scd1(process_name, schema_name, table_name, payloads, process_id):
    env = current_app.config['ENVIRONMENT']
    high_end_date = '9998-12-31'
    logs = {
        'payload_count': 0
        ,'inserted_count': 0
        ,'updated_count': 0
        ,'deleted_count': 0
        ,'no_change_count': 0
        ,'skipped_count': 0
        ,'null_count': 0
        ,'skipped_due_to_schema_mismatch': []
        ,'status': ''
        ,'message': ''
    }

    keycolumns_data = fetch_queries_as_dictionaries(f"""
SELECT
    OUT_PROCESS_NAME
    ,KEYCOLUMN_NAME
FROM
    {env}T_META.METADATA_KEY_COLUMNS
WHERE
    OUT_PROCESS_NAME = '{process_name}'
    AND CONSIDER_FOR_PROCESSING = 1;
    """, 'return_none')
    key_columns_list = [row['KEYCOLUMN_NAME'] for row in keycolumns_data]
    print(key_columns_list)
    if len(key_columns_list) == 0:
        logs['status'] = 'Failed'
        logs['message'] = f'No Key Columns Present in Metadata Key Column Table for SCD1 Process {process_name}'
        logs['inserted_count']  = -1
        logs['updated_count']   = -1
        logs['deleted_count']   = -1
        logs['no_change_count'] = -1
        logs['skipped_count']   = -1
        logs['null_count']      = -1
        return logs

    # Fetch Table Schema
    target_component_info = get_component_info_from_db('BASE TABLE', schema_name, table_name)
    column_names_list = target_component_info[schema_name][table_name]
    column_names_list = [f'`{column}`' for column in column_names_list]
    table_column_set = set(column_names_list)
    scd1_columns_set = {'`UPDATE_PROCESS_NAME`', '`UPDATE_PROCESS_ID`', '`PROCESS_NAME`',\
                        '`PROCESS_ID`', '`START_DATE`', '`END_DATE`', '`RECORD_DELETED_FLAG`'}
    columns_in_table_to_be_ignored_set = {'`ID`'}
    #columns_in_payload_to_be_ignored_set = {} # Add If Any

    value_columns_to_be_compared = [column for column in column_names_list if column not in scd1_columns_set and column not in columns_in_table_to_be_ignored_set] # column_names_list is already with ""
    column_index_map = {column: index for index, column in enumerate(value_columns_to_be_compared)} # value_columns_to_be_compared already has ""
    select_clause = ", ".join(value_columns_to_be_compared)
    print(value_columns_to_be_compared, column_index_map, select_clause)

    conn = connection_pool.get_connection()
    cursor = conn.cursor()
    for payload in payloads:
        logs['payload_count'] += 1

        # Checked for NULLs in all Fields
        all_fields_none = all(payload[value_column.replace('`','')] is None for value_column in value_columns_to_be_compared)
        if all_fields_none:
            logs['null_count'] += 1
            continue # If All fields are null go to the next record

        payload_keys_set = set(f'`{key}`' for key in payload.keys())
        missing_in_table = payload_keys_set - table_column_set # columns_in_payload_to_be_ignored_set
        missing_in_payload = table_column_set - payload_keys_set - scd1_columns_set - columns_in_table_to_be_ignored_set
        print(missing_in_table, missing_in_payload)
        if missing_in_table or missing_in_payload:
            logs['skipped_count'] += 1
            logs['skipped_due_to_schema_mismatch'].append({
                'payload': payload
                ,'missing_in_table' : list(missing_in_table)
                ,'missing_in_payload': list(missing_in_payload)
            })
            continue # proceed with the next record/payload after logging and skipping bad record
        print(key_columns_list)
        # Where Clause to find the latest active record
        where_clause = ' AND '.join(f'`{key_column}` = %s' for key_column in key_columns_list) + \
                       ' AND `END_DATE` = %s AND `RECORD_DELETED_FLAG` = 0'
        where_values = [payload[key_column.replace('`','')] for key_column in key_columns_list] + [high_end_date]
        print(where_clause, where_values, sep = "\n")
        existing_rows = fetch_queries_as_dictionaries(f"""
SELECT
    {select_clause}
FROM
    {schema_name}.{table_name}
WHERE
    {where_clause}
    """, 'return_none', where_values)
        print(existing_rows)
        if existing_rows:
            has_changed = any(payload[value_column.replace('`','')] != existing_rows[column_index_map[value_column]] for value_column in value_columns_to_be_compared)
            if has_changed:
                # Hard Delete Existing Record
                cursor.execute(f"DELETE FROM {schema_name}.{table_name} WHERE {where_clause}", where_values)
                logs['deleted_count'] += 1
            else:
                logs['no_change_count'] += 1
                continue # if Payload and existing record matches move on

        # Insert Latest Record
        insert_columns = [f'`{key}`' for key in payload.keys()] + ['`PROCESS_NAME`', '`PROCESS_ID`', '`START_DATE`', '`END_DATE`', '`RECORD_DELETED_FLAG`']
        insert_values = list(payload.values()) + [process_name, process_id, date.today().strftime('%Y-%m-%d'), high_end_date, 0]
        insert_statement_placeholders = ", ".join('%s' for _ in insert_values) # eg., (%s, %s, %s)
        insert_clause = ", ".join(insert_columns)

        insert_sql = f"INSERT INTO {schema_name}.{table_name} ({insert_clause}) VALUES ({insert_statement_placeholders})"
        print(insert_sql, insert_values)
        cursor.execute(insert_sql, insert_values)
        logs['inserted_count'] += 1

    logs['status'] = 'Success'
    try:
        if payloads[0]['ALT_SYMBOL']:
            logs['message'] = f'SCD1 Completed for Process {process_name} for {payloads[0]["ALT_SYMBOL"]}'
        else:
            logs['message'] = f'SCD1 Completed for Process {process_name}'
    except Exception as e:
        logs['message'] = f'SCD1 Completed for Process {process_name}'

    conn.commit()
    cursor.close()
    conn.close()
    return logs
