import sqlite3, os
db_path = os.path.join(os.path.dirname(__file__), '..', '..', 'databases', 'consolidated_portfolio.db')

from utils.date_utils.get_current_time import get_current_timestamp

def update_log_record(process_name, process_id, status, message = None, payload_count = None, inserted_count = None, updated_count = None, no_change_count = None, skipped_count = None, skipped_due_to_schema_mismatch = None):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    logs_update_clause = "UPDATE EXECUTION_LOGS SET STATUS = ?, LOG = ?, PAYLOAD_COUNT = ?, INSERTED_COUNT =?, UPDATED_COUNT = ?, NO_CHANGE_COUNT = ?, SKIPPED_COUNT = ?, SKIPPED_DUE_TO_SCHEMA = ?, END_TS = ?  WHERE PROCESS_NAME = ? AND PROCESS_ID = ?"
    logs_update_values = [status, message, payload_count, inserted_count, updated_count, no_change_count, skipped_count, skipped_due_to_schema_mismatch, get_current_timestamp(), process_name, process_id]
    cursor.execute(logs_update_clause, logs_update_values)
    conn.commit()

    return process_id