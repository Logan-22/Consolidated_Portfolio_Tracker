from os import getenv
from utils.connection_utils.connection_pool_config import connection_pool

env = getenv('ENVIRONMENT')

def create_user_investment_schema(invs_schema = f"{env}T_USER_INVS"):
    conn = connection_pool.get_connection()
    cursor = conn.cursor()
    cursor.execute(f"""
CREATE DATABASE IF NOT EXISTS {invs_schema}
CHARACTER SET utf8mb4
COLLATE utf8mb4_unicode_ci;
    """)
    cursor.close()
    conn.close()

def create_mf_transaction_table(invs_schema = f"{env}T_USER_INVS"):
    conn = connection_pool.get_connection()
    cursor = conn.cursor()
    cursor.execute(f"""
CREATE TABLE IF NOT EXISTS {invs_schema}.MF_TRANSACTIONS
(
    TXN_ID                   INT            AUTO_INCREMENT PRIMARY KEY,
    USER_ID                  BIGINT,
    FUND_NAME                VARCHAR (255),
    TXN_DATE                 DATE,
    TXN_TYPE                 VARCHAR (10),  
    INVESTED_AMOUNT          DECIMAL (10, 4),
    STAMP_FEES_AMOUNT        DECIMAL (10, 4),
    AMC_AMOUNT               DECIMAL (10, 4),
    NAV_DURING_PURCHASE      DECIMAL (10, 4),
    UNITS                    DECIMAL (10, 4),
    UPDATE_PROCESS_NAME      VARCHAR(100),
    UPDATE_PROCESS_ID        INT,
    PROCESS_NAME             VARCHAR(100),
    PROCESS_ID               INT,
    START_DATE               DATE,
    END_DATE                 DATE,
    RECORD_DELETED_FLAG      INT            DEFAULT 0,
    INDEX idx_mf_txn (TXN_ID)
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_unicode_ci;
    """)
    cursor.close()
    conn.close()