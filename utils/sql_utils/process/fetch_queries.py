import sqlite3, os
db_path = os.path.join(os.path.dirname(__file__), '..', '..', '..', 'databases', 'consolidated_portfolio.db')

def fetch_queries_as_dictionaries(query):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute(query)
    rows = cursor.fetchall()
    if rows:
        result = [dict(row) for row in rows]
    else:
        # Extract column names from the cursor description
        column_names = [desc[0] for desc in cursor.description]
        # Create a single dict with None values
        result = [{col: None for col in column_names}]
    return result