from os import getenv
from utils.connection_utils.connection_pool_config import connection_pool

env = getenv('ENVIRONMENT')

def create_tier0_metrics_schema(tier1_metrics_schema = f"{env}T_TIER1_METRICS"):
    conn = connection_pool.get_connection()
    cursor = conn.cursor()
    cursor.execute(f"""
CREATE DATABASE IF NOT EXISTS {tier1_metrics_schema}
CHARACTER SET utf8mb4
COLLATE utf8mb4_unicode_ci;
    """)
    cursor.close()
    conn.close()

def create_daily_instrument_prices_table(tier1_metrics_schema = f"{env}T_TIER1_METRICS"):
    conn = connection_pool.get_connection()
    cursor = conn.cursor()
    cursor.execute(f"""
CREATE TABLE IF NOT EXISTS {tier1_metrics_schema}.MF_PORTFOLIO_VIEW
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