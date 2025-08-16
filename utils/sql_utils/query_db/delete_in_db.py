from utils.connection_utils.connection_pool_config import connection_pool

def truncate_table(schema, table_name):
    conn = connection_pool.get_connection()
    cursor = conn.cursor()
    cursor.execute(f"""
DELETE FROM {schema}.{table_name};
    """)
    conn.commit()
    cursor.close()
    conn.close()
