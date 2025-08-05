import sqlite3, os
db_path = os.path.join(os.path.dirname(__file__), '..', '..', 'databases', 'consolidated_portfolio.db')

from utils.date_utils.get_current_time import get_current_timestamp

def insert_into_duplicate_logs(target_table, duplicate_data, cnt, query_executed):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("INSERT INTO DUPLICATE_LOGS (TABLE_NAME, DUPLICATE_DATA, CNT, QUERY_EXECUTED, ENTRY_TIMESTAMP) VALUES (?, ?, ?, ?, ?)",
                  (target_table,duplicate_data, cnt, query_executed, get_current_timestamp()))
    conn.commit()
    conn.close()