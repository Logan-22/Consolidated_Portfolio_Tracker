from os import getenv
from utils.connection_utils.connection_pool_config import connection_pool

env = getenv('ENVIRONMENT')

def create_log_schema(log_schema = f"{env}T_LOG"):
    conn = connection_pool.get_connection()
    cursor = conn.cursor()
    cursor.execute(f"""
CREATE DATABASE IF NOT EXISTS {log_schema}
CHARACTER SET utf8mb4
COLLATE utf8mb4_unicode_ci;
    """)
    cursor.close()
    conn.close()

def create_execution_logs_table(log_schema = f"{env}T_LOG"):
    conn = connection_pool.get_connection()
    cursor = conn.cursor()
    cursor.execute(f"""
CREATE TABLE IF NOT EXISTS {log_schema}.EXECUTION_LOGS (
    ID                             INT                AUTO_INCREMENT PRIMARY KEY,
    PROCESS_NAME                   VARCHAR (100),
    PROCESS_ID                     INT,
    STATUS                         VARCHAR (100),
    LOG                            VARCHAR (100),
    PROCESSING_START_DATE          DATE,
    PROCESSING_END_DATE            DATE,
    PAYLOAD_COUNT                  TINYINT,
    INSERTED_COUNT                 TINYINT,
    UPDATED_COUNT                  TINYINT,
    DELETED_COUNT                  TINYINT,
    NO_CHANGE_COUNT                TINYINT,
    SKIPPED_COUNT                  TINYINT,
    NULL_COUNT                     TINYINT,
    SKIPPED_DUE_TO_METADATA_SCHEMA JSON,
    START_TS                       TIMESTAMP          DEFAULT CURRENT_TIMESTAMP,
    END_TS                         TIMESTAMP
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_unicode_ci;
    """)
    cursor.close()
    conn.close()

def create_duplicate_logs_table(log_schema = f"{env}T_LOG"):
    conn = connection_pool.get_connection()
    cursor = conn.cursor()
    cursor.execute(f"""
CREATE TABLE IF NOT EXISTS {log_schema}.DUPLICATE_LOGS
(
    ID              INT                     AUTO_INCREMENT PRIMARY KEY,
    TABLE_NAME      VARCHAR (100),
    DUPLICATE_DATA  VARCHAR (1000),
    CNT             INT,
    QUERY_EXECUTED  VARCHAR (1000),
    ENTRY_TIMESTAMP TIMESTAMP               DEFAULT CURRENT_TIMESTAMP
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_unicode_ci;
    """)
    cursor.close()
    conn.close()