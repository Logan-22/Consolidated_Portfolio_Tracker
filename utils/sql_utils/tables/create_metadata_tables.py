import sqlite3
from utils.folder_utils.paths import db_path
from utils.sql_utils.process.fetch_queries import fetch_queries_as_dictionaries
from datetime import datetime

def create_metadata_store_table():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("""
CREATE TABLE IF NOT EXISTS METADATA_STORE
(
    ID                       INTEGER        PRIMARY KEY AUTOINCREMENT,
    EXCHANGE_SYMBOL          TEXT (100),
    YAHOO_SYMBOL             TEXT (100),
    ALT_SYMBOL               TEXT (100),
    ALLOCATION_CATEGORY      TEXT (100),
    PORTFOLIO_TYPE           TEXT (50),
    AMC                      TEXT (50),
    MF_TYPE                  TEXT (100),
    FUND_CATEGORY            TEXT (100),
    LAUNCHED_ON              DATE,
    EXIT_LOAD                NUMERIC (2, 2),
    EXPENSE_RATIO            NUMERIC (2, 2),
    FUND_MANAGER             TEXT (100),
    FUND_MANAGER_STARTED_ON  DATE,
    ISIN                     TEXT (100),
    PROCESS_FLAG             INTEGER,
    CONSIDER_FOR_RETURNS     INTEGER,
    PROCESSING_DATE          DATE,
    PREVIOUS_PROCESSING_DATE DATE,
    NEXT_PROCESSING_DATE     DATE,
    UPDATE_PROCESS_NAME      TEXT,
    UPDATE_PROCESS_ID        INTEGER,
    PROCESS_NAME             TEXT,
    PROCESS_ID               INTEGER,
    START_DATE               DATE,
    END_DATE                 DATE,
    RECORD_DELETED_FLAG      INTEGER        DEFAULT 0
);
    """)
    conn.close()

def create_processing_date_table():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("""
CREATE TABLE IF NOT EXISTS PROCESSING_DATE
(
    ID             INTEGER       PRIMARY KEY AUTOINCREMENT,
    PROC_TYP_CD    VARCHAR (100),
    PROC_DATE      DATE,
    NEXT_PROC_DATE DATE,
    PREV_PROC_DATE DATE
);
    """)
    processing_date_count_data = fetch_queries_as_dictionaries("SELECT COUNT(*) AS RCOUNT FROM PROCESSING_DATE;")
    if processing_date_count_data[0]['RCOUNT'] == 0:
        cursor.execute("INSERT INTO PROCESSING_DATE (PROC_TYP_CD, PROC_DATE, NEXT_PROC_DATE, PREV_PROC_DATE) VALUES (?, ?, ?, ?)",
                   ('MF_PROC', datetime.date.today(), datetime.date.today(), datetime.date.today()))
        cursor.execute("INSERT INTO PROCESSING_DATE (PROC_TYP_CD, PROC_DATE, NEXT_PROC_DATE, PREV_PROC_DATE) VALUES (?, ?, ?, ?)",
                   ('PPF_MF_PROC', datetime.date.today(), datetime.date.today(), datetime.date.today()))
        cursor.execute("INSERT INTO PROCESSING_DATE (PROC_TYP_CD, PROC_DATE, NEXT_PROC_DATE, PREV_PROC_DATE) VALUES (?, ?, ?, ?)",
                   ('STOCK_PROC', datetime.date.today(), datetime.date.today(), datetime.date.today()))
        cursor.execute("INSERT INTO PROCESSING_DATE (PROC_TYP_CD, PROC_DATE, NEXT_PROC_DATE, PREV_PROC_DATE) VALUES (?, ?, ?, ?)",
                   ('SIM_MF_PROC', datetime.date.today(), datetime.date.today(), datetime.date.today()))
        cursor.execute("INSERT INTO PROCESSING_DATE (PROC_TYP_CD, PROC_DATE, NEXT_PROC_DATE, PREV_PROC_DATE) VALUES (?, ?, ?, ?)",
                   ('SIM_STOCK_PROC', datetime.date.today(), datetime.date.today(), datetime.date.today()))
        conn.commit()
    conn.close()

def create_processing_type_table():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("""
CREATE TABLE IF NOT EXISTS PROCESSING_TYPE
(
    ID               INTEGER       PRIMARY KEY AUTOINCREMENT,
    PROC_TYP_CD      VARCHAR (100),
    PROC_TYPE        VARCHAR (100),
    PROC_DESCRIPTION VARCHAR (100) 
);
    """)
    processing_type_count_data = fetch_queries_as_dictionaries(f"SELECT COUNT(*) AS RCOUNT FROM PROCESSING_TYPE;")
    if processing_type_count_data[0]['RCOUNT'] == 0:
        cursor.execute("INSERT INTO PROCESSING_TYPE (PROC_TYP_CD, PROC_TYPE, PROC_DESCRIPTION) VALUES (?, ?, ?)",
                      ('SIMULATED_RETURNS', 'nifty_50', 'NIFTY 50'))
        conn.commit()
    conn.close()

def create_metadata_process_group_table():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("""
CREATE TABLE IF NOT EXISTS METADATA_PROCESS_GROUP
(
    ID                       INTEGER PRIMARY KEY AUTOINCREMENT,
    PROCESS_GROUP            TEXT,
    OUT_PROCESS_NAME         TEXT,
    CONSIDER_FOR_PROCESSING  INTEGER,
    EXECUTION_ORDER          INTEGER,
    PROCESSING_DATE          DATE,
    PREVIOUS_PROCESSING_DATE DATE,
    NEXT_PROCESSING_DATE     DATE,
    UPDATE_PROCESS_NAME      TEXT,
    UPDATE_PROCESS_ID        INTEGER,
    PROCESS_NAME             TEXT,
    PROCESS_ID               INTEGER,
    START_DATE               DATE,
    END_DATE                 DATE,
    RECORD_DELETED_FLAG      INTEGER DEFAULT 0
);
    """)
    conn.close()

def create_metadata_process_table():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("""
CREATE TABLE IF NOT EXISTS METADATA_PROCESS
(
    ID                         INTEGER PRIMARY KEY AUTOINCREMENT,
    OUT_PROCESS_NAME           TEXT,
    PROCESS_TYPE               TEXT,
    PROC_TYP_CD_LIST           TEXT,
    INPUT_VIEW                 TEXT,
    TARGET_TABLE               TEXT,
    PROCESS_DESCRIPTION        TEXT,
    AUTO_TRIGGER_ON_LAUNCH     INTEGER,
    PROCESS_DECOMMISSIONED     INTEGER,
    DEFAULT_START_DATE_TYPE_CD TEXT,
    PROCESSING_DATE            DATE,
    PREVIOUS_PROCESSING_DATE   DATE,
    NEXT_PROCESSING_DATE       DATE,
    UPDATE_PROCESS_NAME        TEXT,
    UPDATE_PROCESS_ID          INTEGER,
    PROCESS_NAME               TEXT,
    PROCESS_ID                 INTEGER,
    START_DATE                 DATE,
    END_DATE                   DATE,
    RECORD_DELETED_FLAG        INTEGER DEFAULT 0
);
    """)
    conn.close()

def create_metadata_key_columns_table():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("""
CREATE TABLE IF NOT EXISTS METADATA_KEY_COLUMNS
(
    ID                       INTEGER    PRIMARY KEY AUTOINCREMENT,
    OUT_PROCESS_NAME         TEXT (100),
    KEYCOLUMN_NAME           TEXT (100),
    CONSIDER_FOR_PROCESSING  INTEGER,
    PROCESSING_DATE          DATE,
    PREVIOUS_PROCESSING_DATE DATE,
    NEXT_PROCESSING_DATE     DATE,
    UPDATE_PROCESS_NAME      TEXT,
    UPDATE_PROCESS_ID        INTEGER,
    PROCESS_NAME             TEXT,
    PROCESS_ID               INTEGER,
    START_DATE               DATE,
    END_DATE                 DATE,
    RECORD_DELETED_FLAG      INTEGER    DEFAULT 0
);
    """)
    conn.close()

def create_execution_logs_table():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("""
CREATE TABLE IF NOT EXISTS EXECUTION_LOGS (
    ID                    INTEGER PRIMARY KEY AUTOINCREMENT,
    PROCESS_NAME          TEXT,
    PROCESS_ID            INTEGER,
    STATUS                TEXT,
    LOG                   TEXT,
    PROCESSING_START_DATE DATE,
    PROCESSING_END_DATE   DATE,
    PAYLOAD_COUNT         INTEGER,
    INSERTED_COUNT        INTEGER,
    UPDATED_COUNT         INTEGER,
    DELETED_COUNT         INTEGER,
    NO_CHANGE_COUNT       INTEGER,
    SKIPPED_COUNT         INTEGER,
    NULL_COUNT            INTEGER,
    SKIPPED_DUE_TO_SCHEMA TEXT,
    START_TS              TEXT,
    END_TS                TEXT
);
    """)
    conn.close()

def create_holiday_date_table():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("""
CREATE TABLE IF NOT EXISTS HOLIDAY_DATES
(
    ID                       INTEGER       PRIMARY KEY AUTOINCREMENT,
    HOLIDAY_DATE             DATE,
    HOLIDAY_NAME             VARCHAR (200),
    HOLIDAY_DAY              VARCHAR (20),
    PROCESSING_DATE          DATE,
    PREVIOUS_PROCESSING_DATE DATE,
    NEXT_PROCESSING_DATE     DATE,
    UPDATE_PROCESS_NAME      TEXT,
    UPDATE_PROCESS_ID        INTEGER,
    PROCESS_NAME             TEXT,
    PROCESS_ID               INTEGER,
    START_DATE               DATE,
    END_DATE                 DATE,
    RECORD_DELETED_FLAG      INTEGER       DEFAULT 0
);
    """)
    conn.close()

def create_working_date_table():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("""
CREATE TABLE IF NOT EXISTS WORKING_DATES
(
    ID                       INTEGER       PRIMARY KEY AUTOINCREMENT,
    WORKING_DATE             DATE,
    WORKING_DAY_NAME         VARCHAR (200),
    WORKING_DAY              VARCHAR (20),
    PROCESSING_DATE          DATE,
    PREVIOUS_PROCESSING_DATE DATE,
    NEXT_PROCESSING_DATE     DATE,
    UPDATE_PROCESS_NAME      TEXT,
    UPDATE_PROCESS_ID        INTEGER,
    PROCESS_NAME             TEXT,
    PROCESS_ID               INTEGER,
    START_DATE               DATE,
    END_DATE                 DATE,
    RECORD_DELETED_FLAG      INTEGER       DEFAULT 0
);
    """)
    conn.close()

def create_holiday_calendar_table():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("""
CREATE TABLE IF NOT EXISTS HOLIDAY_CALENDAR (
    ID                       INTEGER      PRIMARY KEY AUTOINCREMENT,
    PROCESSING_DATE          DATE,
    PROCESSING_DAY           VARCHAR (15),
    NEXT_PROCESSING_DATE     DATE,
    NEXT_PROCESSING_DAY      VARCHAR (15),
    PREVIOUS_PROCESSING_DATE DATE,
    PREVIOUS_PROCESSING_DAY  VARCHAR (15),
    UPDATE_PROCESS_NAME      TEXT,
    UPDATE_PROCESS_ID        INTEGER,
    PROCESS_NAME             TEXT,
    PROCESS_ID               INTEGER,
    START_DATE               DATE,
    END_DATE                 DATE,
    RECORD_DELETED_FLAG      INTEGER DEFAULT 0
);
    """)
    conn.close()

def create_duplicate_logs_table():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("""
CREATE TABLE IF NOT EXISTS DUPLICATE_LOGS
(
    ID              INTEGER PRIMARY KEY AUTOINCREMENT,
    TABLE_NAME      TEXT,
    DUPLICATE_DATA  TEXT,
    CNT             TEXT,
    QUERY_EXECUTED  TEXT,
    ENTRY_TIMESTAMP TEXT
);
    """)
    conn.close()