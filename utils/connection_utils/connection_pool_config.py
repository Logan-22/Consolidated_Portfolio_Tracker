from mysql.connector import pooling
from os import getenv

connection_pool = pooling.MySQLConnectionPool(
    pool_name = "consolidated_tracker_connection_pool"
    ,pool_size = 5
    ,pool_reset_session = True
    ,host = getenv("DB_HOST")
    ,user = getenv("DB_USER")
    ,password = getenv("DB_PASSWORD")
    ,port = int(getenv("DB_PORT"))
)
