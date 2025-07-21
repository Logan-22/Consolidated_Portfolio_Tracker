import sqlite3, os
db_path = os.path.join(os.path.dirname(__file__), '..', '..', '..', 'databases', 'consolidated_portfolio.db')

def fetch_queries_as_dictionaries(query):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute(query)
    rows = cursor.fetchall()
    result = [dict(row) for row in rows]
    return result