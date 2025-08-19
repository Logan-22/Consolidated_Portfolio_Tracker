from datetime import date
from utils.sql_utils.query_db.get_or_process_in_db import get_component_info_from_db
from utils.connection_utils.connection_pool_config import connection_pool

def insert_append(process_name, schema_name, table_name, payloads, process_id):
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

    # Fetch Table Schema
    target_component_info = get_component_info_from_db('BASE TABLE', schema_name, table_name)
    column_names_list = target_component_info[schema_name][table_name]
    column_names_list = [f'`{column}`' for column in column_names_list]
    table_column_set = set(column_names_list)
    meta_columns_set = {'`UPDATE_PROCESS_NAME`', '`UPDATE_PROCESS_ID`', '`PROCESS_NAME`',\
                        '`PROCESS_ID`', '`START_DATE`', '`END_DATE`', '`RECORD_DELETED_FLAG`'}
    columns_in_table_to_be_ignored_set = {'`ID`'} # Generic Column to be ignored
    value_columns_to_be_compared = [column for column in column_names_list if column not in meta_columns_set and column not in columns_in_table_to_be_ignored_set] # column_names_list is already with ""

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
        missing_in_table = payload_keys_set - table_column_set
        missing_in_payload = table_column_set - payload_keys_set - meta_columns_set - columns_in_table_to_be_ignored_set
        if missing_in_table or missing_in_payload:
            logs['skipped_count'] += 1
            logs['skipped_due_to_schema_mismatch'].append({
                'payload': payload
                ,'missing_in_table' : list(missing_in_table)
                ,'missing_in_payload': list(missing_in_payload)
            })
            continue # proceed with the next record/payload after logging and skipping bad record

        # Insert Latest Record
        insert_columns = [f'`{key}`' for key in payload.keys()] + ['`PROCESS_NAME`', '`PROCESS_ID`', '`START_DATE`', '`END_DATE`', '`RECORD_DELETED_FLAG`']
        insert_values = list(payload.values()) + [process_name, process_id, date.today().strftime('%Y-%m-%d'), high_end_date, 0]
        insert_statement_placeholders = ", ".join('%s' for _ in insert_values) # eg., (%s, %s, %s)
        insert_clause = ", ".join(insert_columns)

        insert_sql = f"INSERT INTO {schema_name}.{table_name} ({insert_clause}) VALUES ({insert_statement_placeholders})"
        cursor.execute(insert_sql, insert_values)
        logs['inserted_count'] += 1

    logs['status'] = 'Success'
    try:
        if payloads[0].get('ALT_SYMBOL'):
            logs['message'] = f'Insert/Append Completed for Process {process_name} for {payloads[0]["ALT_SYMBOL"]}'
        else:
            logs['message'] = f'Insert/Append Completed for Process {process_name}'
    except Exception as e:
        logs['message'] = f'Insert/Append Completed for Process {process_name}'

    conn.commit()
    cursor.close()
    conn.close()
    return logs
