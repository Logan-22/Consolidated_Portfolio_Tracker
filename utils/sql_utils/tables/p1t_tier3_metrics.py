from os import getenv
from utils.connection_utils.connection_pool_config import connection_pool

env = getenv('ENVIRONMENT')

def create_tier3_metrics_schema(tier3_metrics_schema = f"{env}T_TIER3_METRICS"):
    conn = connection_pool.get_connection()
    cursor = conn.cursor()
    cursor.execute(f"""
CREATE DATABASE IF NOT EXISTS {tier3_metrics_schema}
CHARACTER SET utf8mb4
COLLATE utf8mb4_unicode_ci;
    """)
    cursor.close()
    conn.close()

def create_tier3_fin_mf_pf_table(tier3_metrics_schema = f"{env}T_TIER3_METRICS"):
    conn = connection_pool.get_connection()
    cursor = conn.cursor()
    cursor.execute(f"""
CREATE TABLE IF NOT EXISTS {tier3_metrics_schema}.FIN_MF_PORTFOLIO
(
    ID                           INT            AUTO_INCREMENT PRIMARY KEY,
    USER_ID                      BIGINT         NOT NULL,
    FIN_TOTAL_INVESTED_AMOUNT    DECIMAL (20, 4),
    FIN_TOTAL_AMC_AMOUNT         DECIMAL (20, 4),
    FIN_TOTAL_STAMP_FEES_AMOUNT  DECIMAL (20, 4),
    FIN_CURRENT_VALUE            DECIMAL (20, 4),
    FIN_P_L                      DECIMAL (20, 4),
    FIN_PERC_P_L                 DECIMAL (20, 4),
    FIN_PREVIOUS_VALUE           DECIMAL (20, 4),
    FIN_DAY_P_L                  DECIMAL (20, 4),
    FIN_PERC_DAY_P_L             DECIMAL (20, 4),
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
    INDEX idx_t3mf_usr (USER_ID),
    INDEX idx_t3mf_processing_date (PROCESSING_DATE)
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_unicode_ci;
    """)
    cursor.close()
    conn.close()