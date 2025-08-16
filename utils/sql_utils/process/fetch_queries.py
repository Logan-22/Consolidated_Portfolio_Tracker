from mysql.connector import Error
from utils.connection_utils.connection_pool_config import connection_pool

def fetch_queries_as_dictionaries(query, on_empty_result = "return_none_list", params = None):
    """
    Execute a MySQL query and return the results as a list of dictionaries.

    :param query: MySQL query string
    :param on_empty_result: If the MySQL Result is empty, returns List of Nones if "Return None List" else Returns None
    :return: List of dictionaries
    """
    try:
        conn = connection_pool.get_connection()
        cursor = conn.cursor(dictionary = True)
        cursor.execute(query, params)
        rows = cursor.fetchall()
        if rows:
            result = rows
        elif on_empty_result == "return_none_list":
            # Extract column names from the cursor description
            column_names = [desc[0] for desc in cursor.description]
            # Create a single dict with None values
            result = [{col: None for col in column_names}]
        else:
            result = []
        return result
    except Error as e:
        print(f"MySQL error: {e}")
        return []

    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()