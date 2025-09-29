from os import getenv
from utils.connection_utils.connection_pool_config import connection_pool

env = getenv('ENVIRONMENT')

def create_tier1_metrics_schema(tier1_metrics_schema = f"{env}T_TIER1_METRICS"):
    conn = connection_pool.get_connection()
    cursor = conn.cursor()
    cursor.execute(f"""
CREATE DATABASE IF NOT EXISTS {tier1_metrics_schema}
CHARACTER SET utf8mb4
COLLATE utf8mb4_unicode_ci;
    """)
    cursor.close()
    conn.close()

def create_tier1_mf_pf_table(tier1_metrics_schema = f"{env}T_TIER1_METRICS"):
    conn = connection_pool.get_connection()
    cursor = conn.cursor()
    cursor.execute(f"""
CREATE TABLE IF NOT EXISTS {tier1_metrics_schema}.MF_PORTFOLIO
(
    ID                       INT            AUTO_INCREMENT PRIMARY KEY,
    INSTRUMENT_ID            INT            NOT NULL,
    USER_ID                  BIGINT         NOT NULL,
    FUND_AMC                 VARCHAR (50),
    FUND_TYPE                VARCHAR (100),
    FUND_CATEGORY            VARCHAR (100),
    FUND_ALLOCATION_CATEGORY VARCHAR (200),
    FUND_UNITS               DECIMAL (20, 4),
    AVERAGE_PRICE            DECIMAL (20, 4),
    TOTAL_INVESTED_AMOUNT    DECIMAL (20, 4),
    TOTAL_AMC_AMOUNT         DECIMAL (20, 4),
    TOTAL_STAMP_FEES_AMOUNT  DECIMAL (20, 4),
    CURRENT_NAV              DECIMAL (20, 4),
    CURRENT_VALUE            DECIMAL (20, 4),
    P_L                      DECIMAL (20, 4),
    PERC_P_L                 DECIMAL (20, 4),
    PREVIOUS_NAV             DECIMAL (20, 4),
    PREVIOUS_VALUE           DECIMAL (20, 4),
    DAY_P_L                  DECIMAL (20, 4),
    PERC_DAY_P_L             DECIMAL (20, 4),
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
    INDEX idx_t1mf_instr (INSTRUMENT_ID),
    INDEX idx_t1mf_instr_usr (INSTRUMENT_ID, USER_ID),
    INDEX idx_t1mf_processing_date (PROCESSING_DATE)
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_unicode_ci;
    """)
    cursor.close()
    conn.close()