from os import getenv
from utils.connection_utils.connection_pool_config import connection_pool

env = getenv('ENVIRONMENT')

def create_tier2_metrics_schema(tier2_metrics_schema = f"{env}T_TIER2_METRICS"):
    conn = connection_pool.get_connection()
    cursor = conn.cursor()
    cursor.execute(f"""
CREATE DATABASE IF NOT EXISTS {tier2_metrics_schema}
CHARACTER SET utf8mb4
COLLATE utf8mb4_unicode_ci;
    """)
    cursor.close()
    conn.close()

def create_tier2_agg_mf_pf_table(tier2_metrics_schema = f"{env}T_TIER2_METRICS"):
    conn = connection_pool.get_connection()
    cursor = conn.cursor()
    cursor.execute(f"""
CREATE TABLE IF NOT EXISTS {tier2_metrics_schema}.AGG_MF_PORTFOLIO
(
    ID                           INT            AUTO_INCREMENT PRIMARY KEY,
    INSTRUMENT_ID                INT            NOT NULL,
    USER_ID                      BIGINT         NOT NULL,
    FUND_AMC                     VARCHAR (50),
    FUND_TYPE                    VARCHAR (100),
    FUND_CATEGORY                VARCHAR (100),
    FUND_ALLOCATION_CATEGORY     VARCHAR (200),
    AGG_FUND_UNITS               DECIMAL (20, 4),
    AGG_AVERAGE_PRICE            DECIMAL (20, 4),
    AGG_TOTAL_INVESTED_AMOUNT    DECIMAL (20, 4),
    AGG_TOTAL_AMC_AMOUNT         DECIMAL (20, 4),
    AGG_TOTAL_STAMP_FEES_AMOUNT  DECIMAL (20, 4),
    CURRENT_NAV                  DECIMAL (20, 4),
    AGG_CURRENT_VALUE            DECIMAL (20, 4),
    AGG_P_L                      DECIMAL (20, 4),
    AGG_PERC_P_L                 DECIMAL (20, 4),
    PREVIOUS_NAV                 DECIMAL (20, 4),
    AGG_PREVIOUS_VALUE           DECIMAL (20, 4),
    AGG_DAY_P_L                  DECIMAL (20, 4),
    AGG_PERC_DAY_P_L             DECIMAL (20, 4),
    PROCESSING_DATE              DATE,
    PREVIOUS_PROCESSING_DATE     DATE,
    NEXT_PROCESSING_DATE         DATE,
    UPDATE_PROCESS_NAME          VARCHAR(100),
    UPDATE_PROCESS_ID            INT,
    PROCESS_NAME                 VARCHAR(100),
    PROCESS_ID                   INT,
    START_DATE                   DATE,
    END_DATE                     DATE,
    RECORD_DELETED_FLAG          INT            DEFAULT 0,
    INDEX idx_t2mf_instr (INSTRUMENT_ID),
    INDEX idx_t2mf_instr_usr (INSTRUMENT_ID, USER_ID),
    INDEX idx_t2mf_processing_date (PROCESSING_DATE)
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_unicode_ci;
    """)
    cursor.close()
    conn.close()