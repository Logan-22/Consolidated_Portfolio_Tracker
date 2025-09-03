from os import getenv
from utils.connection_utils.connection_pool_config import connection_pool

env = getenv('ENVIRONMENT')

def create_tier0_metrics_schema(tier0_metrics_schema = f"{env}T_TIER0_METRICS"):
    conn = connection_pool.get_connection()
    cursor = conn.cursor()
    cursor.execute(f"""
CREATE DATABASE IF NOT EXISTS {tier0_metrics_schema}
CHARACTER SET utf8mb4
COLLATE utf8mb4_unicode_ci;
    """)
    cursor.close()
    conn.close()

def create_daily_instrument_prices_table(tier0_metrics_schema = f"{env}T_TIER0_METRICS"):
    conn = connection_pool.get_connection()
    cursor = conn.cursor()
    cursor.execute(f"""
CREATE TABLE IF NOT EXISTS {tier0_metrics_schema}.DAILY_INSTRUMENT_PRICES
(
    ID                       INT            AUTO_INCREMENT PRIMARY KEY,
    INSTRUMENT_ID            INT            NOT NULL,
    VALUE_DATE               DATE           NOT NULL,
    PRICE                    DECIMAL(10,4)  NOT NULL,
    PROCESSING_DATE          DATE,
    PREVIOUS_PROCESSING_DATE DATE,
    NEXT_PROCESSING_DATE     DATE,
    UPDATE_PROCESS_NAME      VARCHAR(100),
    UPDATE_PROCESS_ID        INT,
    PROCESS_NAME             VARCHAR(100),
    PROCESS_ID               INT,
    START_DATE               DATE,
    END_DATE                 DATE,
    RECORD_DELETED_FLAG      INT            DEFAULT 0,
    INDEX idx_instr (INSTRUMENT_ID),
    INDEX idx_instr_date (INSTRUMENT_ID, VALUE_DATE),
    INDEX idx_processing_date (PROCESSING_DATE)
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_unicode_ci;
    """)
    cursor.close()
    conn.close()

def create_mf_depository_holding_table(tier0_metrics_schema = f"{env}T_TIER0_METRICS"):
    conn = connection_pool.get_connection()
    cursor = conn.cursor()
    cursor.execute(f"""
CREATE TABLE IF NOT EXISTS {tier0_metrics_schema}.MF_DEPOSITORY_HOLDINGS
(
    ID                       INT            AUTO_INCREMENT PRIMARY KEY,
    INSTRUMENT_ID            INT,
    USER_ID                  BIGINT,
    EXCHANGE_SYMBOL          VARCHAR(255),
    TOTAL_QUANTITY           INT,
    TOTAL_INVESTED_AMOUNT    DECIMAL(10, 4),
    AVERAGE_PRICE            DECIMAL(10, 4),
    LAST_TXN_ID              INT,
    DEP_STATUS               VARCHAR(20),
    CREATED_AT               TIMESTAMP      DEFAULT CURRENT_TIMESTAMP,
    LAST_UPDATED_AT          TIMESTAMP      DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PROCESSING_DATE          DATE,
    PREVIOUS_PROCESSING_DATE DATE,
    NEXT_PROCESSING_DATE     DATE,
    UPDATE_PROCESS_NAME      VARCHAR(100),
    UPDATE_PROCESS_ID        INT,
    PROCESS_NAME             VARCHAR(100),
    PROCESS_ID               INT,
    START_DATE               DATE,
    END_DATE                 DATE,
    RECORD_DELETED_FLAG      INT            DEFAULT 0,
    INDEX idx_mf_dep_hold(INSTRUMENT_ID, USER_ID)
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_unicode_ci;
    """)
    cursor.close()
    conn.close()