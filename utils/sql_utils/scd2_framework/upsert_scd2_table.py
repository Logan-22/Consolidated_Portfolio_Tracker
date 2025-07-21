import sqlite3, os
db_path = os.path.join(os.path.dirname(__file__), '..', '..', '..', 'databases', 'consolidated_portfolio.db')

def upsert_scd2(process_name, table_name, payloads, process_id, processing_date, prev_processing_date, next_processing_date):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    high_end_date = '9998-12-31'
    logs = {
        'payload_count': 0
        ,'inserted_count': 0
        ,'updated_count': 0
        ,'no_change_count': 0
        ,'skipped_count': 0
        ,'skipped_due_to_schema_mismatch': []
        ,'status': ''
        ,'message': ''
    }

    # Fetch SCD2 Key columns for the table

    cursor.execute(f"SELECT PROCESS_NAME, KEYCOLUMN_NAME FROM METADATA_KEY_COLUMNS WHERE PROCESS_NAME = '{process_name}' AND CONSIDER_FOR_PROCESSING = 1")
    key_column_rows = cursor.fetchall()
    key_columns_list = [f'"{row[1]}"' for row in key_column_rows]
    if len(key_columns_list) == 0:
        logs['status'] = 'Failed'
        logs['message'] = f'No Key Columns Present in Metadata Key Column Table for SCD2 Process {process_name}'
        logs['inserted_count'] = -1
        logs['updated_count'] = -1
        return logs

    # Fetch Table Schema
    cursor.execute(f'PRAGMA TABLE_INFO ("{table_name}");')
    column_info_rows = cursor.fetchall()

    column_names_list = [f'"{column[1]}"' for column in column_info_rows] # Second Column in the pragma is column name
    table_column_set = set(column_names_list)
    scd2_columns_set = {'"UPDATE_PROCESS_NAME"', '"UPDATE_PROCESS_ID"', '"PROCESS_NAME"',\
                        '"PROCESS_ID"', '"START_DATE"', '"END_DATE"', '"RECORD_DELETED_FLAG"'}
    columns_in_table_to_be_ignored_set = {'"ID"'}
    columns_in_payload_to_be_ignored_set = {'"PROC_DATE"'}

    value_columns_to_be_compared = [column for column in column_names_list if column not in scd2_columns_set and column not in columns_in_table_to_be_ignored_set] # column_names_list is already with ""
    column_index_map = {column: index for index, column in enumerate(value_columns_to_be_compared)} # value_columns_to_be_compared already has ""
    select_clause = ", ".join(value_columns_to_be_compared)

    for payload in payloads:
        updated = False
        logs['payload_count'] += 1
        payload_keys_set = set(f'"{key}"' for key in payload.keys())
        missing_in_table = payload_keys_set - table_column_set - columns_in_payload_to_be_ignored_set
        missing_in_payload = table_column_set - payload_keys_set - scd2_columns_set - columns_in_table_to_be_ignored_set

        if missing_in_table or missing_in_payload:
            logs['skipped_count'] += 1
            logs['skipped_due_to_schema_mismatch'].append({
                'payload': payload
                ,'missing_in_table' : list(missing_in_table)
                ,'missing_in_payload': list(missing_in_payload)
            })
            continue # proceed with the next record/payload after logging and skipping bad record
        
        # Where Clause to find the latest active record
        where_clause = ' AND '.join(f'{key_column} = ?' for key_column in key_columns_list) + \
                       ' AND "END_DATE" = ? AND "RECORD_DELETED_FLAG" = 0'
        where_values = [payload[key_column.replace('"','')] for key_column in key_columns_list] + [high_end_date]
        cursor.execute(f"SELECT {select_clause} FROM {table_name} WHERE {where_clause}", where_values)
        existing_rows = cursor.fetchone()

        if existing_rows:
            has_changed = any(payload[value_column.replace('"','')] != existing_rows[column_index_map[value_column]] for value_column in value_columns_to_be_compared)
            if has_changed:
                # Soft Delete Existing Record
                update_sql = f"UPDATE {table_name} SET UPDATE_PROCESS_NAME = ?, UPDATE_PROCESS_ID = ?, END_DATE = ?, RECORD_DELETED_FLAG = 1 WHERE {where_clause}"
                cursor.execute(update_sql, [process_name, process_id, prev_processing_date] + where_values) # first ? is for end_date update
                updated = True
                logs['updated_count'] += 1
            else:
                logs['no_change_count'] += 1
                continue # if Payload and existing record matches move on

        # Insert Latest Record
        insert_columns = [f'"{key}"' for key in payload.keys()] + ['"PROCESS_NAME"', '"PROCESS_ID"', '"START_DATE"', '"END_DATE"', '"RECORD_DELETED_FLAG"']
        insert_values = list(payload.values()) + [process_name, process_id, processing_date, high_end_date, 0]
        insert_statement_placeholders = ", ".join('?' for _ in insert_values) # eg., (?, ?, ?)
        insert_clause = ", ".join(insert_columns)

        insert_sql = f"INSERT INTO {table_name} ({insert_clause}) VALUES ({insert_statement_placeholders})"
        cursor.execute(insert_sql, insert_values)
        if not updated:
            logs['inserted_count'] += 1
    conn.commit()

    logs['status'] = 'Success'
    logs['message'] = f'SCD2 Completed for Process {process_name}'

    return logs
