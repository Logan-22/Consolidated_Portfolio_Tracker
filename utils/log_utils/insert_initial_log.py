import sqlite3, os
db_path = os.path.join(os.path.dirname(__file__), '..', '..', 'databases', 'consolidated_portfolio.db')

from utils.date_utils.get_current_time import get_current_timestamp

def insert_intitial_log_record(process_name):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Setup Log Parameter
    cursor.execute("SELECT MAX(PROCESS_ID) FROM EXECUTION_LOGS;")
    max_process_id_row = cursor.fetchone()
    if not max_process_id_row[0]:
        max_process_id = 0
    else:
        max_process_id = max_process_id_row[0]

    process_id = max_process_id + 1

    logs_insert_clause = "INSERT INTO EXECUTION_LOGS (PROCESS_NAME, PROCESS_ID, STATUS, LOG, PROCESSING_START_DATE, PROCESSING_END_DATE, PAYLOAD_COUNT, INSERTED_COUNT, UPDATED_COUNT, NO_CHANGE_COUNT, SKIPPED_DUE_TO_SCHEMA, START_TS, END_TS) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    logs_insert_values = [process_name, process_id, 'Started', 'In Progress', None, None, None, None, None, None, None, get_current_timestamp(), None ]
    cursor.execute(logs_insert_clause, logs_insert_values)
    conn.commit()

    return process_id