from utils.connection_utils.connection_pool_config import connection_pool

def insert_into_table(schema, table_name, table_payload):
    conn = connection_pool.get_connection()
    cursor = conn.cursor()
    insert_values = []
    insert_statement = f"""INSERT INTO {schema}.{table_name} ({" ,".join(list(table_payload[0].keys()))})
VALUES ({" ,".join(['%s'] * len(table_payload[0]))})"""
    for payload in table_payload:
        insert_values.append(tuple(payload.values()))
    cursor.executemany(insert_statement, insert_values)
    conn.commit()
    cursor.close()
    conn.close()
    return cursor.rowcount
