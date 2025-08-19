from os import getenv
from utils.connection_utils.connection_pool_config import connection_pool

env = getenv('ENVIRONMENT')

def create_metadata_schema(metadata_schema = f"{env}T_META"):
    conn = connection_pool.get_connection()
    cursor = conn.cursor()
    cursor.execute(f"""
CREATE DATABASE IF NOT EXISTS {metadata_schema}
CHARACTER SET utf8mb4
COLLATE utf8mb4_unicode_ci;
    """)
    cursor.close()
    conn.close()

def create_metadata_store_table(metadata_schema = f"{env}T_META"):
    conn = connection_pool.get_connection()
    cursor = conn.cursor()
    cursor.execute(f"""
CREATE TABLE IF NOT EXISTS {metadata_schema}.METADATA_STORE
(
    ID                       INT            AUTO_INCREMENT PRIMARY KEY,
    EXCHANGE_SYMBOL          VARCHAR (100),
    YAHOO_SYMBOL             VARCHAR (100),
    ALT_SYMBOL               VARCHAR (100),
    ALLOCATION_CATEGORY      VARCHAR (100),
    PORTFOLIO_TYPE           VARCHAR (50),
    AMC                      VARCHAR (50),
    MF_TYPE                  VARCHAR (100),
    FUND_CATEGORY            VARCHAR (100),
    LAUNCHED_ON              DATE,
    EXIT_LOAD                DECIMAL (2, 2),
    EXPENSE_RATIO            DECIMAL (2, 2),
    FUND_MANAGER             VARCHAR (100),
    FUND_MANAGER_STARTED_ON  DATE,
    ISIN                     VARCHAR (100),
    PROCESS_FLAG             TINYINT,
    CONSIDER_FOR_RETURNS     TINYINT,
    UPDATE_PROCESS_NAME      VARCHAR(100),
    UPDATE_PROCESS_ID        INT,
    PROCESS_NAME             VARCHAR(100),
    PROCESS_ID               INT,
    START_DATE               DATE,
    END_DATE                 DATE,
    RECORD_DELETED_FLAG      TINYINT        DEFAULT 0
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_unicode_ci;
    """)
    cursor.close()
    conn.close()

def create_metadata_process_group_table(metadata_schema = f"{env}T_META"):
    conn = connection_pool.get_connection()
    cursor = conn.cursor()
    cursor.execute(f"""
CREATE TABLE IF NOT EXISTS {metadata_schema}.METADATA_PROCESS_GROUP
(
    ID                       INT            AUTO_INCREMENT PRIMARY KEY,
    PROCESS_GROUP            VARCHAR(100),
    OUT_PROCESS_NAME         VARCHAR(100),
    CONSIDER_FOR_PROCESSING  TINYINT,
    EXECUTION_ORDER          TINYINT,
    UPDATE_PROCESS_NAME      VARCHAR(100),
    UPDATE_PROCESS_ID        INT,
    PROCESS_NAME             VARCHAR(100),
    PROCESS_ID               INT,
    START_DATE               DATE,
    END_DATE                 DATE,
    RECORD_DELETED_FLAG      TINYINT        DEFAULT 0
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_unicode_ci;
    """)
    cursor.close()
    conn.close()

def create_metadata_process_table(metadata_schema = f"{env}T_META"):
    conn = connection_pool.get_connection()
    cursor = conn.cursor()
    cursor.execute(f"""
CREATE TABLE IF NOT EXISTS {metadata_schema}.METADATA_PROCESS
(
    ID                         INT          AUTO_INCREMENT PRIMARY KEY,
    OUT_PROCESS_NAME           VARCHAR(100),
    PROCESS_TYPE               VARCHAR(100),
    PROC_TYP_CD_LIST           VARCHAR(200),
    INPUT_DATABASE             VARCHAR(100),
    INPUT_VIEW                 VARCHAR(100),
    TARGET_DATABASE            VARCHAR(100),
    TARGET_TABLE               VARCHAR(100),
    PROCESS_DESCRIPTION        VARCHAR(300),
    AUTO_TRIGGER_ON_LAUNCH     TINYINT,
    PROCESS_DECOMMISSIONED     TINYINT,
    DEFAULT_START_DATE_TYPE_CD VARCHAR(100),
    UPDATE_PROCESS_NAME        VARCHAR(100),
    UPDATE_PROCESS_ID          INT,
    PROCESS_NAME               VARCHAR(100),
    PROCESS_ID                 INT,
    START_DATE                 DATE,
    END_DATE                   DATE,
    RECORD_DELETED_FLAG        TINYINT      DEFAULT 0
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_unicode_ci;
    """)
    cursor.close()
    conn.close()

def create_metadata_columns_table(metadata_schema = f"{env}T_META"):
    conn = connection_pool.get_connection()
    cursor = conn.cursor()
    cursor.execute(f"""
CREATE TABLE IF NOT EXISTS {metadata_schema}.METADATA_COLUMNS
(
    ID                       INT            AUTO_INCREMENT PRIMARY KEY,
    OUT_PROCESS_NAME         VARCHAR (100),
    COLUMN_NAME              VARCHAR (100),
    COLUMN_TYP_CD            VARCHAR (20),
    CONSIDER_FOR_PROCESSING  TINYINT,
    UPDATE_PROCESS_NAME      VARCHAR (100),
    UPDATE_PROCESS_ID        INT,
    PROCESS_NAME             VARCHAR (100),
    PROCESS_ID               INT,
    START_DATE               DATE,
    END_DATE                 DATE,
    RECORD_DELETED_FLAG      TINYINT        DEFAULT 0
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_unicode_ci;
    """)
    cursor.close()
    conn.close()

def create_metadata_excluded_columns_table(metadata_schema = f"{env}T_META"):
    conn = connection_pool.get_connection()
    cursor = conn.cursor()
    cursor.execute(f"""
CREATE TABLE IF NOT EXISTS {metadata_schema}.METADATA_EXCL_COLUMNS
(
    ID                       INT            AUTO_INCREMENT PRIMARY KEY,
    OUT_PROCESS_NAME         VARCHAR (100),
    EXCL_COLUMN_NAME         VARCHAR (100),
    CONSIDER_FOR_PROCESSING  TINYINT,
    UPDATE_PROCESS_NAME      VARCHAR (100),
    UPDATE_PROCESS_ID        INT,
    PROCESS_NAME             VARCHAR (100),
    PROCESS_ID               INT,
    START_DATE               DATE,
    END_DATE                 DATE,
    RECORD_DELETED_FLAG      TINYINT        DEFAULT 0
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_unicode_ci;
    """)
    cursor.close()
    conn.close()

def create_holiday_date_table(metadata_schema = f"{env}T_META"):
    conn = connection_pool.get_connection()
    cursor = conn.cursor()
    cursor.execute(f"""
CREATE TABLE IF NOT EXISTS {metadata_schema}.HOLIDAY_DATES
(
    ID                       INT            AUTO_INCREMENT PRIMARY KEY,
    HOLIDAY_DATE             DATE,
    HOLIDAY_NAME             VARCHAR (200),
    HOLIDAY_DAY              VARCHAR (20),
    PROCESSING_DATE          DATE,
    PREVIOUS_PROCESSING_DATE DATE,
    NEXT_PROCESSING_DATE     DATE,
    UPDATE_PROCESS_NAME      VARCHAR (100),
    UPDATE_PROCESS_ID        INT,
    PROCESS_NAME             VARCHAR (100),
    PROCESS_ID               INT,
    START_DATE               DATE,
    END_DATE                 DATE,
    RECORD_DELETED_FLAG      TINYINT        DEFAULT 0
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_unicode_ci;
    """)
    cursor.close()
    conn.close()

def create_working_date_table(metadata_schema = f"{env}T_META"):
    conn = connection_pool.get_connection()
    cursor = conn.cursor()
    cursor.execute(f"""
CREATE TABLE IF NOT EXISTS {metadata_schema}.WORKING_DATES
(
    ID                       INT            AUTO_INCREMENT PRIMARY KEY,
    WORKING_DATE             DATE,
    WORKING_DAY_NAME         VARCHAR (200),
    WORKING_DAY              VARCHAR (20),
    PROCESSING_DATE          DATE,
    PREVIOUS_PROCESSING_DATE DATE,
    NEXT_PROCESSING_DATE     DATE,
    UPDATE_PROCESS_NAME      VARCHAR (100),
    UPDATE_PROCESS_ID        INT,
    PROCESS_NAME             VARCHAR (100),
    PROCESS_ID               INT,
    START_DATE               DATE,
    END_DATE                 DATE,
    RECORD_DELETED_FLAG      TINYINT        DEFAULT 0
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_unicode_ci;
    """)
    cursor.close()
    conn.close()

def create_holiday_calendar_table(metadata_schema = f"{env}T_META"):
    conn = connection_pool.get_connection()
    cursor = conn.cursor()
    cursor.execute(f"""
CREATE TABLE IF NOT EXISTS {metadata_schema}.HOLIDAY_CALENDAR (
    ID                       INT            AUTO_INCREMENT PRIMARY KEY,
    PROCESSING_DATE          DATE,
    PROCESSING_DAY           VARCHAR (15),
    NEXT_PROCESSING_DATE     DATE,
    NEXT_PROCESSING_DAY      VARCHAR (15),
    PREVIOUS_PROCESSING_DATE DATE,
    PREVIOUS_PROCESSING_DAY  VARCHAR (15),
    UPDATE_PROCESS_NAME      VARCHAR (100),
    UPDATE_PROCESS_ID        INT,
    PROCESS_NAME             VARCHAR (100),
    PROCESS_ID               INT,
    START_DATE               DATE,
    END_DATE                 DATE,
    RECORD_DELETED_FLAG      TINYINT        DEFAULT 0
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_unicode_ci;
    """)
    cursor.close()
    conn.close()
