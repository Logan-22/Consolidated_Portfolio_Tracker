import datetime
from utils.connection_utils.connection_pool_config import connection_pool
from utils.sql_utils.process.fetch_queries_in_aws import fetch_queries_as_dictionaries_in_aws

def create_metadata_schema_in_aws(schema_id = "P1"):
    conn = connection_pool.get_connection()
    cursor = conn.cursor()
    cursor.execute(f"""
CREATE DATABASE IF NOT EXISTS {schema_id}T_META
CHARACTER SET utf8mb4
COLLATE utf8mb4_unicode_ci;
    """)
    cursor.close()
    conn.close()

def create_metadata_store_table_in_aws(schema_id = "P1"):
    conn = connection_pool.get_connection()
    cursor = conn.cursor()
    cursor.execute(f"""
CREATE TABLE IF NOT EXISTS {schema_id}T_META.METADATA_STORE
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
    PROCESS_FLAG             INT,
    CONSIDER_FOR_RETURNS     INT,
    UPDATE_PROCESS_NAME      VARCHAR(100),
    UPDATE_PROCESS_ID        INT,
    PROCESS_NAME             VARCHAR(100),
    PROCESS_ID               INT,
    START_DATE               DATE,
    END_DATE                 DATE,
    RECORD_DELETED_FLAG      INT            DEFAULT 0
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_unicode_ci;
    """)
    cursor.close()
    conn.close()

def create_processing_date_table_in_aws(schema_id = "P1"):
    conn = connection_pool.get_connection()
    cursor = conn.cursor()
    cursor.execute(f"""
CREATE TABLE IF NOT EXISTS {schema_id}T_META.PROCESSING_DATE
(
    ID             INT                      AUTO_INCREMENT PRIMARY KEY,
    PROC_TYP_CD    VARCHAR (100),
    PROC_DATE      DATE,
    NEXT_PROC_DATE DATE,
    PREV_PROC_DATE DATE
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_unicode_ci;
    """)
    processing_date_count_data = fetch_queries_as_dictionaries_in_aws(f"SELECT COUNT(*) AS RCOUNT FROM {schema_id}T_META.PROCESSING_DATE;", "return_none_list")
    if processing_date_count_data[0]['RCOUNT'] == 0:
        proc_date_typ_cds = [('MF_PROC', datetime.date.today(),datetime.date.today(),datetime.date.today())
                             ,('STOCK_PROC', datetime.date.today(),datetime.date.today(),datetime.date.today())
                             ,('SIM_MF_PROC', datetime.date.today(),datetime.date.today(),datetime.date.today())
                             ,('SIM_STOCK_PROC', datetime.date.today(),datetime.date.today(),datetime.date.today())
                             ]
        insert_query = f"INSERT INTO {schema_id}T_META.PROCESSING_DATE (PROC_TYP_CD, PROC_DATE, NEXT_PROC_DATE, PREV_PROC_DATE) VALUES(%s, %s, %s, %s)"
        for proc_date_typ_cd_values in proc_date_typ_cds:
            cursor.execute(insert_query, proc_date_typ_cd_values)
    conn.commit()
    cursor.close()
    conn.close()

def create_processing_type_table_in_aws(schema_id = "P1"):
    conn = connection_pool.get_connection()
    cursor = conn.cursor()
    cursor.execute(f"""
CREATE TABLE IF NOT EXISTS {schema_id}T_META.PROCESSING_TYPE
(
    ID               INT                      AUTO_INCREMENT PRIMARY KEY,
    PROC_TYP_CD      VARCHAR (100),
    PROC_TYPE        VARCHAR (100),
    PROC_DESCRIPTION VARCHAR (100) 
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_unicode_ci;
    """)
    processing_type_count_data = fetch_queries_as_dictionaries_in_aws(f"SELECT COUNT(*) AS RCOUNT FROM {schema_id}T_META.PROCESSING_TYPE;", "return_none_list")
    if processing_type_count_data[0]['RCOUNT'] == 0:
        cursor.execute(f"INSERT INTO {schema_id}T_META.PROCESSING_TYPE (PROC_TYP_CD, PROC_TYPE, PROC_DESCRIPTION) VALUES (%s, %s, %s)",
                      ('SIMULATED_RETURNS', 'nifty_50', 'NIFTY 50'))
    conn.commit()
    cursor.close()
    conn.close()

def create_metadata_process_group_table_in_aws(schema_id = "P1"):
    conn = connection_pool.get_connection()
    cursor = conn.cursor()
    cursor.execute(f"""
CREATE TABLE IF NOT EXISTS {schema_id}T_META.METADATA_PROCESS_GROUP
(
    ID                       INT                      AUTO_INCREMENT PRIMARY KEY,
    PROCESS_GROUP            VARCHAR(100),
    OUT_PROCESS_NAME         VARCHAR(100),
    CONSIDER_FOR_PROCESSING  INT,
    EXECUTION_ORDER          INT,
    UPDATE_PROCESS_NAME      VARCHAR(100),
    UPDATE_PROCESS_ID        INT,
    PROCESS_NAME             VARCHAR(100),
    PROCESS_ID               INT,
    START_DATE               DATE,
    END_DATE                 DATE,
    RECORD_DELETED_FLAG      INT DEFAULT 0
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_unicode_ci;
    """)
    cursor.close()
    conn.close()

def create_metadata_process_table_in_aws(schema_id = "P1"):
    conn = connection_pool.get_connection()
    cursor = conn.cursor()
    cursor.execute(f"""
CREATE TABLE IF NOT EXISTS {schema_id}T_META.METADATA_PROCESS
(
    ID                         INT                      AUTO_INCREMENT PRIMARY KEY,
    OUT_PROCESS_NAME           VARCHAR(100),
    PROCESS_TYPE               VARCHAR(100),
    PROC_TYP_CD_LIST           VARCHAR(200),
    INPUT_VIEW                 VARCHAR(100),
    TARGET_TABLE               VARCHAR(100),
    PROCESS_DESCRIPTION        VARCHAR(300),
    AUTO_TRIGGER_ON_LAUNCH     INT,
    PROCESS_DECOMMISSIONED     INT,
    FREQUENCY                  VARCHAR(100),
    DEFAULT_START_DATE_TYPE_CD VARCHAR(100),
    UPDATE_PROCESS_NAME        VARCHAR(100),
    UPDATE_PROCESS_ID          INT,
    PROCESS_NAME               VARCHAR(100),
    PROCESS_ID                 INT,
    START_DATE                 DATE,
    END_DATE                   DATE,
    RECORD_DELETED_FLAG        INT DEFAULT 0
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_unicode_ci;
    """)
    cursor.close()
    conn.close()

def create_metadata_key_columns_table_in_aws(schema_id = "P1"):
    conn = connection_pool.get_connection()
    cursor = conn.cursor()
    cursor.execute(f"""
CREATE TABLE IF NOT EXISTS {schema_id}T_META.METADATA_KEY_COLUMNS
(
    ID                       INT                      AUTO_INCREMENT PRIMARY KEY,
    OUT_PROCESS_NAME         VARCHAR (100),
    KEYCOLUMN_NAME           VARCHAR (100),
    CONSIDER_FOR_PROCESSING  INT,
    PROCESSING_DATE          DATE,
    PREVIOUS_PROCESSING_DATE DATE,
    NEXT_PROCESSING_DATE     DATE,
    UPDATE_PROCESS_NAME      VARCHAR (100),
    UPDATE_PROCESS_ID        INT,
    PROCESS_NAME             VARCHAR (100),
    PROCESS_ID               INT,
    START_DATE               DATE,
    END_DATE                 DATE,
    RECORD_DELETED_FLAG      INT    DEFAULT 0
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_unicode_ci;
    """)
    cursor.close()
    conn.close()

def create_execution_logs_table_in_aws(schema_id = "P1"):
    conn = connection_pool.get_connection()
    cursor = conn.cursor()
    cursor.execute(f"""
CREATE TABLE IF NOT EXISTS {schema_id}T_META.EXECUTION_LOGS (
    ID                    INT                      AUTO_INCREMENT PRIMARY KEY,
    PROCESS_NAME          VARCHAR (100),
    PROCESS_ID            INT,
    STATUS                VARCHAR (100),
    LOG                   VARCHAR (100),
    PROCESSING_START_DATE DATE,
    PROCESSING_END_DATE   DATE,
    PAYLOAD_COUNT         INT,
    INSERTED_COUNT        INT,
    UPDATED_COUNT         INT,
    DELETED_COUNT         INT,
    NO_CHANGE_COUNT       INT,
    SKIPPED_COUNT         INT,
    NULL_COUNT            INT,
    SKIPPED_DUE_TO_SCHEMA VARCHAR (100),
    START_TS              TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    END_TS                TIMESTAMP
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_unicode_ci;
    """)
    cursor.close()
    conn.close()

def create_holiday_date_table_in_aws(schema_id = "P1"):
    conn = connection_pool.get_connection()
    cursor = conn.cursor()
    cursor.execute(f"""
CREATE TABLE IF NOT EXISTS {schema_id}T_META.HOLIDAY_DATES
(
    ID                       INT                      AUTO_INCREMENT PRIMARY KEY,
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
    RECORD_DELETED_FLAG      INT       DEFAULT 0
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_unicode_ci;
    """)
    cursor.close()
    conn.close()

def create_working_date_table_in_aws(schema_id = "P1"):
    conn = connection_pool.get_connection()
    cursor = conn.cursor()
    cursor.execute(f"""
CREATE TABLE IF NOT EXISTS {schema_id}T_META.WORKING_DATES
(
    ID                       INT                      AUTO_INCREMENT PRIMARY KEY,
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
    RECORD_DELETED_FLAG      INT       DEFAULT 0
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_unicode_ci;
    """)
    cursor.close()
    conn.close()

def create_holiday_calendar_table_in_aws(schema_id = "P1"):
    conn = connection_pool.get_connection()
    cursor = conn.cursor()
    cursor.execute(f"""
CREATE TABLE IF NOT EXISTS {schema_id}T_META.HOLIDAY_CALENDAR (
    ID                       INT                      AUTO_INCREMENT PRIMARY KEY,
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
    RECORD_DELETED_FLAG      INT DEFAULT 0
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_unicode_ci;
    """)
    cursor.close()
    conn.close()

def create_duplicate_logs_table_in_aws(schema_id = "P1"):
    conn = connection_pool.get_connection()
    cursor = conn.cursor()
    cursor.execute(f"""
CREATE TABLE IF NOT EXISTS {schema_id}T_META.DUPLICATE_LOGS
(
    ID              INT                      AUTO_INCREMENT PRIMARY KEY,
    TABLE_NAME      VARCHAR (100),
    DUPLICATE_DATA  VARCHAR (1000),
    CNT             INT,
    QUERY_EXECUTED  VARCHAR (1000),
    ENTRY_TIMESTAMP TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_unicode_ci;
    """)
    cursor.close()
    conn.close()