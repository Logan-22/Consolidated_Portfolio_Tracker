import sqlite3
from utils.folder_utils.paths import db_path
from utils.connection_utils.connection_pool_config import connection_pool

def update_table_with_payload(schema_name, table_name, payload):
    """
    Update the table using the provided payload.

    :param schema_name: str -> schema_name of the table to be updated
    :param table_name: str -> table_name to be updated
    :param payload: dict -> {data: object, conditions: object}
    :return: int -> Number of rows updated
    """
    if not schema_name:
        raise ValueError("Schema Name is mandatory to update the table")
    if not table_name:
        raise ValueError("Table Name is mandatory to update the table")
    data       = payload['data']
    conditions = payload['conditions']
    if not data:
        raise ValueError("No Columns provided for update")
    if not conditions:
        raise ValueError("Where clause is mandatory to prevent full table update")
    
    set_parts = []
    update_values = []
    for column, value in data.items():
        if isinstance(value, tuple) and value[0] == 'increment':
            set_parts.append(f"`{column}` = `{column}` + %s")
            update_values.append(value[1])
        elif isinstance(value, tuple) and value[0] == 'decrement':
            set_parts.append(f"`{column}` = `{column}` - %s")
            update_values.append(value[1])
        elif isinstance(value, dict) and "raw" in value:
            set_parts.append(f"`{column}` = {value['raw']}")
        else:
            set_parts.append(f"`{column}` = %s")
            update_values.append(value)

    set_clause   = ", ".join(set_parts)
    where_clause = "AND ".join([f"`{column}` = %s" for column in conditions.keys()])
    update_values.extend(conditions.values())
    update_query = f"UPDATE {schema_name}.{table_name} SET {set_clause} WHERE {where_clause};"

    conn = connection_pool.get_connection()
    cursor = conn.cursor()
    cursor.execute(update_query, update_values)
    conn.commit()
    updated_row_count = cursor.rowcount
    cursor.close()
    conn.close()
    return updated_row_count
