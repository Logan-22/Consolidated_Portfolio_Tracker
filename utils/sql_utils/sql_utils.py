import sqlite3
from flask import  jsonify
import datetime
from utils.folder_utils.paths import db_path
from utils.sql_utils.process.fetch_queries import fetch_queries_as_dictionaries

from utils.sql_utils.views.MUTUAL_FUND_PORTFOLIO_VIEW import MUTUAL_FUND_PORTFOLIO_VIEW
from utils.sql_utils.views.AGG_MUTUAL_FUND_PORTFOLIO_VIEW import AGG_MUTUAL_FUND_PORTFOLIO_VIEW
from utils.sql_utils.views.FIN_MUTUAL_FUND_PORTFOLIO_VIEW import FIN_MUTUAL_FUND_PORTFOLIO_VIEW
from utils.sql_utils.views.STOCK_INTRADAY_REALISED_PORTFOLIO_VIEW import STOCK_INTRADAY_REALISED_PORTFOLIO_VIEW
from utils.sql_utils.views.AGG_STOCK_INTRADAY_REALISED_PORTFOLIO_VIEW import AGG_STOCK_INTRADAY_REALISED_PORTFOLIO_VIEW
from utils.sql_utils.views.FIN_STOCK_INTRADAY_REALISED_PORTFOLIO_VIEW import FIN_STOCK_INTRADAY_REALISED_PORTFOLIO_VIEW
from utils.sql_utils.views.STOCK_SWING_REALISED_PORTFOLIO_VIEW import STOCK_SWING_REALISED_PORTFOLIO_VIEW
from utils.sql_utils.views.AGG_STOCK_SWING_REALISED_PORTFOLIO_VIEW import AGG_STOCK_SWING_REALISED_PORTFOLIO_VIEW
from utils.sql_utils.views.FIN_STOCK_SWING_REALISED_PORTFOLIO_VIEW import FIN_STOCK_SWING_REALISED_PORTFOLIO_VIEW
from utils.sql_utils.views.STOCK_SWING_UNREALISED_PORTFOLIO_VIEW import STOCK_SWING_UNREALISED_PORTFOLIO_VIEW
from utils.sql_utils.views.AGG_STOCK_SWING_UNREALISED_PORTFOLIO_VIEW import AGG_STOCK_SWING_UNREALISED_PORTFOLIO_VIEW
from utils.sql_utils.views.FIN_STOCK_SWING_UNREALISED_PORTFOLIO_VIEW import FIN_STOCK_SWING_UNREALISED_PORTFOLIO_VIEW
from utils.sql_utils.views.CONSOLIDATED_PORTFOLIO_VIEW import CONSOLIDATED_PORTFOLIO_VIEW
from utils.sql_utils.views.AGG_CONSOLIDATED_PORTFOLIO_VIEW import AGG_CONSOLIDATED_PORTFOLIO_VIEW
from utils.sql_utils.views.FIN_CONSOLIDATED_PORTFOLIO_VIEW import FIN_CONSOLIDATED_PORTFOLIO_VIEW
from utils.sql_utils.views.CONSOLIDATED_ALLOCATION_VIEW import CONSOLIDATED_ALLOCATION_VIEW
from utils.sql_utils.views.AGG_CONSOLIDATED_ALLOCATION_VIEW import AGG_CONSOLIDATED_ALLOCATION_VIEW
from utils.sql_utils.views.FIN_CONSOLIDATED_ALLOCATION_VIEW import FIN_CONSOLIDATED_ALLOCATION_VIEW
from utils.sql_utils.views.SIMULATED_PORTFOLIO_VIEW import SIMULATED_PORTFOLIO_VIEW
from utils.sql_utils.views.AGG_SIMULATED_PORTFOLIO_VIEW import AGG_SIMULATED_PORTFOLIO_VIEW
from utils.sql_utils.views.FIN_SIMULATED_PORTFOLIO_VIEW import FIN_SIMULATED_PORTFOLIO_VIEW

def create_price_table():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("""
CREATE TABLE IF NOT EXISTS PRICE_TABLE 
(
    ID                       INTEGER PRIMARY KEY AUTOINCREMENT,
    ALT_SYMBOL               TEXT    NOT NULL,
    PORTFOLIO_TYPE           TEXT    NOT NULL,
    VALUE_DATE               DATE    NOT NULL,
    VALUE_TIME               TIME,
    PRICE                    NUMERIC NOT NULL,
    PRICE_TYP_CD             TEXT    NOT NULL,
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
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_alt_symbol ON PRICE_TABLE (ALT_SYMBOL);')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_symbol_date_type ON PRICE_TABLE (ALT_SYMBOL, VALUE_DATE, PRICE_TYP_CD);')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_portfolio_date ON PRICE_TABLE (PORTFOLIO_TYPE, VALUE_DATE);')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_not_deleted ON PRICE_TABLE (RECORD_DELETED_FLAG);')

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

def get_name_from_metadata(portfolio_type):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(f"SELECT DISTINCT NAME FROM METADATA WHERE PORTFOLIO_TYPE = '{portfolio_type}' ORDER BY NAME")
    rows = cursor.fetchall()
    conn.close()
    if rows:
        name_list = [{'name': row[0]} for row in rows]
        return jsonify(name_list)

def get_all_symbols_list_from_metadata_store(portfolio_type = None):
    portfolio_type_filter = f"AND PORTFOLIO_TYPE = '{portfolio_type}'" if portfolio_type else ""
    symbol_data = fetch_queries_as_dictionaries(f"""
SELECT DISTINCT
    EXCHANGE_SYMBOL
    ,YAHOO_SYMBOL
    ,ALT_SYMBOL
    ,PORTFOLIO_TYPE
FROM
    METADATA_STORE
WHERE 
    1 =  1
    {portfolio_type_filter}
    AND RECORD_DELETED_FLAG = 0;
    """)
    return symbol_data

def get_yahoo_symbol_from_metadata_store(alt_symbol):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(f"SELECT DISTINCT YAHOO_SYMBOL FROM METADATA_STORE WHERE ALT_SYMBOL = '{alt_symbol}';")
    rows = cursor.fetchall()
    yahoo_symbol = None
    conn.close()
    if rows:
        yahoo_symbol = rows[0]
    return yahoo_symbol

def get_price_from_price_table(alt_symbol = None, purchase_date = None):
    alt_symbol_filter    = f"AND ALT_SYMBOL = '{alt_symbol}'"    if alt_symbol    else ""
    purchase_date_filter = f"AND VALUE_DATE = '{purchase_date}'" if purchase_date else ""
    price_data = fetch_queries_as_dictionaries(f"""
SELECT DISTINCT
    PRICE
    ,ALT_SYMBOL
    ,VALUE_DATE
FROM
    PRICE_TABLE
WHERE
    1 = 1 
    {alt_symbol_filter}
    {purchase_date_filter}
    AND PRICE_TYP_CD = 'CLOSE_PRICE'
    AND RECORD_DELETED_FLAG = 0
    ORDER BY ALT_SYMBOL, VALUE_DATE;
    """)
    return price_data[0]

def create_mf_order_table():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("""
CREATE TABLE IF NOT EXISTS MF_ORDER
(
    ID                       INTEGER         PRIMARY KEY AUTOINCREMENT,
    NAME                     TEXT (200),
    PURCHASED_ON             DATE,
    INVESTED_AMOUNT          NUMERIC (10, 4),
    STAMP_FEES_AMOUNT        NUMERIC (10, 4),
    AMC_AMOUNT               NUMERIC (10, 4),
    NAV_DURING_PURCHASE      NUMERIC (10, 4),
    UNITS                    NUMERIC (10, 4),
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

def get_proc_date_from_processing_date_table():
    processing_date_data = fetch_queries_as_dictionaries(f"""
SELECT DISTINCT
    PROC_TYP_CD
    ,PROC_DATE
    ,NEXT_PROC_DATE
    ,PREV_PROC_DATE
FROM
PROCESSING_DATE;
    """)
    return processing_date_data

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
    FREQUENCY                  TEXT,
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

def update_proc_date_in_processing_date_table(proc_typ_cd, proc_date, next_proc_date, prev_proc_date):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(f"UPDATE PROCESSING_DATE SET PROC_DATE = '{proc_date}', NEXT_PROC_DATE = '{next_proc_date}', PREV_PROC_DATE = '{prev_proc_date}' WHERE PROC_TYP_CD = '{proc_typ_cd}';")
    conn.commit()
    conn.close()

def get_all_tables_list():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT DISTINCT NAME FROM METADATA ORDER BY NAME")
    rows = cursor.fetchall()
    conn.close()
    tables_list = []
    if rows:
        for row in rows:
            tables_list.append(row)
        return jsonify(tables_list)
    return

def get_max_value_date_for_alt_symbol(process_flag = None, consider_for_returns = None, portfolio_type = None):
    process_flag_filter         = f"AND MS.PROCESS_FLAG         = '{process_flag}'"         if process_flag         else ""
    consider_for_returns_filter = f"AND MS.CONSIDER_FOR_RETURNS = '{consider_for_returns}'" if consider_for_returns else ""
    portfolio_type_filter       = f"AND MS.PORTFOLIO_TYPE       = '{portfolio_type}'"       if portfolio_type       else ""
    max_value_date_data = fetch_queries_as_dictionaries(f"""
SELECT
    MS.ALT_SYMBOL
    ,MS.EXCHANGE_SYMBOL
    ,MS.YAHOO_SYMBOL
    ,MS.PORTFOLIO_TYPE
    ,MAX(PT.VALUE_DATE) AS MAX_VALUE_DATE
FROM
    METADATA_STORE MS
LEFT OUTER JOIN
    PRICE_TABLE PT
ON
    MS.ALT_SYMBOL = PT.ALT_SYMBOL
WHERE
    1 = 1
    {process_flag_filter}
    {consider_for_returns_filter}
    {portfolio_type_filter}
GROUP BY 1,2,3,4;
    """)
    return max_value_date_data

def get_max_value_date_by_portfolio_type(portfolio_type = None):
    if portfolio_type:
        max_value_date_for_each_portfolio = fetch_queries_as_dictionaries(f"SELECT MS.ALT_SYMBOL, MS.EXCHANGE_SYMBOL, MS.YAHOO_SYMBOL, MS.PORTFOLIO_TYPE, MAX(PT.VALUE_DATE)  FROM METADATA_STORE MS LEFT OUTER JOIN PRICE_TABLE PT ON MS.ALT_SYMBOL = PT.ALT_SYMBOL WHERE MS.PORTFOLIO_TYPE = '{portfolio_type}' GROUP BY 1,2,3,4;")
    else:
        max_value_date_for_each_portfolio = fetch_queries_as_dictionaries(f"SELECT MS.ALT_SYMBOL, MS.EXCHANGE_SYMBOL, MS.YAHOO_SYMBOL, MS.PORTFOLIO_TYPE, MAX(PT.VALUE_DATE)  FROM METADATA_STORE MS LEFT OUTER JOIN PRICE_TABLE PT ON MS.ALT_SYMBOL = PT.ALT_SYMBOL GROUP BY 1,2,3,4;")
    return max_value_date_for_each_portfolio

def get_max_next_processing_date_from_table(table_name):
    max_next_proc_date = fetch_queries_as_dictionaries(f"SELECT MAX(NEXT_PROCESSING_DATE) FROM {table_name};")
    return max_next_proc_date

def duplicate_check_on_price_table():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT ALT_SYMBOL, VALUE_DATE, COUNT(*) C FROM PRICE_TABLE WHERE RECORD_DELETED_FLAG = 0 GROUP BY 1,2 HAVING C > 1;")
    rows = cursor.fetchall()
    conn.close()
    if rows:
        duplicate_data = [{'alt_symbol' : row[0], 'value_date': row[1], 'count': row[2]} for row in rows]
        return jsonify(duplicate_data)
    return None

def create_mf_portfolio_views_in_db():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("DROP VIEW IF EXISTS MUTUAL_FUND_PORTFOLIO_VIEW;")
    cursor.execute(MUTUAL_FUND_PORTFOLIO_VIEW)
    cursor.execute("DROP VIEW IF EXISTS AGG_MUTUAL_FUND_PORTFOLIO_VIEW;")
    cursor.execute(AGG_MUTUAL_FUND_PORTFOLIO_VIEW)
    cursor.execute("DROP VIEW IF EXISTS FIN_MUTUAL_FUND_PORTFOLIO_VIEW;")
    cursor.execute(FIN_MUTUAL_FUND_PORTFOLIO_VIEW)
    conn.commit()
    conn.close()

def create_stock_portfolio_views_in_db():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Intraday Realised Trade Views
    cursor.execute("DROP VIEW IF EXISTS STOCK_INTRADAY_REALISED_PORTFOLIO_VIEW;")
    cursor.execute(STOCK_INTRADAY_REALISED_PORTFOLIO_VIEW)
    cursor.execute("DROP VIEW IF EXISTS AGG_STOCK_INTRADAY_REALISED_PORTFOLIO_VIEW;")
    cursor.execute(AGG_STOCK_INTRADAY_REALISED_PORTFOLIO_VIEW)
    cursor.execute("DROP VIEW IF EXISTS FIN_STOCK_INTRADAY_REALISED_PORTFOLIO_VIEW;")
    cursor.execute(FIN_STOCK_INTRADAY_REALISED_PORTFOLIO_VIEW)

    # Swing Realised Trade Views
    cursor.execute("DROP VIEW IF EXISTS STOCK_SWING_REALISED_PORTFOLIO_VIEW;")
    cursor.execute(STOCK_SWING_REALISED_PORTFOLIO_VIEW)
    cursor.execute("DROP VIEW IF EXISTS AGG_STOCK_SWING_REALISED_PORTFOLIO_VIEW;")
    cursor.execute(AGG_STOCK_SWING_REALISED_PORTFOLIO_VIEW)
    cursor.execute("DROP VIEW IF EXISTS FIN_STOCK_SWING_REALISED_PORTFOLIO_VIEW;")
    cursor.execute(FIN_STOCK_SWING_REALISED_PORTFOLIO_VIEW)

    # Swing Unrealised Trade Views
    cursor.execute("DROP VIEW IF EXISTS STOCK_SWING_UNREALISED_PORTFOLIO_VIEW;")
    cursor.execute(STOCK_SWING_UNREALISED_PORTFOLIO_VIEW)
    cursor.execute("DROP VIEW IF EXISTS AGG_STOCK_SWING_UNREALISED_PORTFOLIO_VIEW;")
    cursor.execute(AGG_STOCK_SWING_UNREALISED_PORTFOLIO_VIEW)
    cursor.execute("DROP VIEW IF EXISTS FIN_STOCK_SWING_UNREALISED_PORTFOLIO_VIEW;")
    cursor.execute(FIN_STOCK_SWING_UNREALISED_PORTFOLIO_VIEW)
    
    conn.commit()
    conn.close()

def create_simulated_portfolio_views_in_db():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Simualated Mutual Fund View
    cursor.execute("DROP VIEW IF EXISTS SIMULATED_PORTFOLIO_VIEW;")
    cursor.execute(SIMULATED_PORTFOLIO_VIEW)
    cursor.execute("DROP VIEW IF EXISTS AGG_SIMULATED_PORTFOLIO_VIEW;")
    cursor.execute(AGG_SIMULATED_PORTFOLIO_VIEW)
    cursor.execute("DROP VIEW IF EXISTS FIN_SIMULATED_PORTFOLIO_VIEW;")
    cursor.execute(FIN_SIMULATED_PORTFOLIO_VIEW)

def create_consolidated_portfolio_views_in_db():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute("DROP VIEW IF EXISTS CONSOLIDATED_PORTFOLIO_VIEW;")
    cursor.execute(CONSOLIDATED_PORTFOLIO_VIEW)
    cursor.execute("DROP VIEW IF EXISTS AGG_CONSOLIDATED_PORTFOLIO_VIEW;")
    cursor.execute(AGG_CONSOLIDATED_PORTFOLIO_VIEW)
    cursor.execute("DROP VIEW IF EXISTS FIN_CONSOLIDATED_PORTFOLIO_VIEW;")
    cursor.execute(FIN_CONSOLIDATED_PORTFOLIO_VIEW)

    cursor.execute("DROP VIEW IF EXISTS CONSOLIDATED_ALLOCATION_VIEW;")
    cursor.execute(CONSOLIDATED_ALLOCATION_VIEW)
    cursor.execute("DROP VIEW IF EXISTS AGG_CONSOLIDATED_ALLOCATION_VIEW;")
    cursor.execute(AGG_CONSOLIDATED_ALLOCATION_VIEW)
    cursor.execute("DROP VIEW IF EXISTS FIN_CONSOLIDATED_ALLOCATION_VIEW;")
    cursor.execute(FIN_CONSOLIDATED_ALLOCATION_VIEW)

    conn.commit()
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

def get_holiday_date_from_holiday_dates_table(current_year = '1900'):
    current_year_filter = f"AND HOLIDAY_DATE >= '{current_year}-01-01'" if current_year else ""
    holiday_date_data = fetch_queries_as_dictionaries(f"""
SELECT
    HOLIDAY_DATE
    ,HOLIDAY_NAME
    ,HOLIDAY_DAY
FROM
    HOLIDAY_DATES
WHERE
    1 = 1
    AND RECORD_DELETED_FLAG = 0
    {current_year_filter}
    ORDER BY HOLIDAY_DATE;
    """)
    return holiday_date_data

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

def get_working_date_from_working_dates_table(current_year = '1900'):
    current_year_filter = f"AND WORKING_DATE >= '{current_year}-01-01'" if current_year else ""
    working_day_data = fetch_queries_as_dictionaries(f"""
SELECT
    WORKING_DATE
    ,WORKING_DAY_NAME
    ,WORKING_DAY
FROM
    WORKING_DATES
WHERE
    1 = 1 
    AND RECORD_DELETED_FLAG = 0 
    {current_year_filter}
    """)
    return working_day_data

def get_first_purchase_date_from_mf_order_date_table():
    first_mf_purchase_data = fetch_queries_as_dictionaries("""
SELECT
    MIN(PURCHASED_ON) AS MF_FIRST_PURHCASE_DATE
FROM
    MF_ORDER
WHERE
    RECORD_DELETED_FLAG = 0;
    """)
    return first_mf_purchase_data[0]

def get_date_setup_from_holiday_calendar(input_date):
    holiday_calendar_payload = fetch_queries_as_dictionaries(f"""
SELECT DISTINCT
    PROCESSING_DATE
    ,NEXT_PROCESSING_DATE
    ,PREVIOUS_PROCESSING_DATE
FROM HOLIDAY_CALENDAR
WHERE
    PROCESSING_DATE <= '{input_date}'
ORDER BY PROCESSING_DATE DESC
LIMIT 1;
    """)
    return holiday_calendar_payload[0]

def get_mf_returns():
    mf_returns_data = fetch_queries_as_dictionaries("""
SELECT
    PROCESSING_DATE
    ,"FIN_%_P/L"
    ,"FIN_%_DAY_P/L"
FROM
    FIN_MUTUAL_FUND_RETURNS
WHERE
    RECORD_DELETED_FLAG = 0
ORDER BY PROCESSING_DATE;
    """)
    return mf_returns_data

def create_trade_table():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("""
CREATE TABLE IF NOT EXISTS TRADES (
    TRADE_ID                 VARCHAR (200),
    FEE_ID                   VARCHAR (200),
    TRADE_SET_ID             VARCHAR (200),
    STOCK_NAME               VARCHAR (200),
    STOCK_ISIN               VARCHAR (100),
    TRADE_DATE               DATE,
    ORDER_NUMBER             VARCHAR (100),
    ORDER_TIME               TIME,
    TRADE_NUMBER             VARCHAR (100),
    TRADE_TIME               TIME,
    BUY_OR_SELL              CHAR (1),
    STOCK_QUANTITY           INTEGER,
    BROKERAGE_PER_TRADE      NUMERIC (10, 4),
    NET_TRADE_PRICE_PER_UNIT NUMERIC (10, 4),
    NET_TOTAL_BEFORE_LEVIES  NUMERIC (10, 4),
    TRADE_SET                INTEGER,
    TRADE_POSITION           VARCHAR (10),
    TRADE_ENTRY_DATE         DATE,
    TRADE_ENTRY_TIME         TIME,
    TRADE_EXIT_DATE          DATE,
    TRADE_EXIT_TIME          TIME,
    TRADE_TYPE               TEXT (20),
    LEVERAGE                 INTEGER,
    PROCESSING_DATE          DATE,
    PREVIOUS_PROCESSING_DATE DATE,
    NEXT_PROCESSING_DATE     DATE,
    UPDATE_PROCESS_NAME      TEXT,
    UPDATE_PROCESS_ID        INTEGER,
    PROCESS_NAME             TEXT,
    PROCESS_ID               INTEGER,
    START_DATE               DATE,
    END_DATE                 DATE,
    RECORD_DELETED_FLAG      INTEGER         DEFAULT 0
);
    """)

def create_fee_table():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("""
CREATE TABLE IF NOT EXISTS FEE_COMPONENT (
    FEE_ID                       VARCHAR (200),
    TRADE_DATE                   DATE,
    NET_OBLIGATION               NUMERIC (10, 4),
    BROKERAGE                    NUMERIC (10, 4),
    EXCHANGE_TRANSACTION_CHARGES NUMERIC (10, 4),
    IGST                         NUMERIC (10, 4),
    SECURITIES_TRANSACTION_TAX   NUMERIC (10, 4),
    SEBI_TURN_OVER_FEES          NUMERIC (10, 4),
    AUTO_SQUARE_OFF_CHARGES      NUMERIC (10, 4),
    DEPOSITORY_CHARGES           NUMERIC (10, 4),
    PROCESSING_DATE              DATE,
    PREVIOUS_PROCESSING_DATE     DATE,
    NEXT_PROCESSING_DATE         DATE,
    UPDATE_PROCESS_NAME          TEXT,
    UPDATE_PROCESS_ID            INTEGER,
    PROCESS_NAME                 TEXT,
    PROCESS_ID                   INTEGER,
    START_DATE                   DATE,
    END_DATE                     DATE,
    RECORD_DELETED_FLAG          INTEGER         DEFAULT 0
);
    """)

def get_realised_intraday_and_swing_stock_returns():
    realised_returns_data = fetch_queries_as_dictionaries("""
SELECT
    SUB.TRADE_DATE
    ,SUB."NET_%_P/L"
FROM
(
SELECT
    FIN_SWING.TRADE_DATE                    AS TRADE_DATE
    ,FIN_SWING."%_P/L_WITH_LEVERAGE"        AS "NET_%_P/L"
FROM
    FIN_REALISED_INTRADAY_STOCK_RETURNS FIN_SWING
WHERE
    FIN_SWING.RECORD_DELETED_FLAG = 0 
UNION ALL
SELECT 
    FIN_INTRA.TRADE_CLOSE_DATE              AS TRADE_DATE
    ,FIN_INTRA."NET_%_P/L"                  AS "NET_%_P/L"
FROM
    FIN_REALISED_SWING_STOCK_RETURNS FIN_INTRA
WHERE
    FIN_INTRA.RECORD_DELETED_FLAG = 0
) SUB
ORDER BY SUB.TRADE_DATE, SUB."NET_%_P/L";
    """)
    return realised_returns_data

def get_open_trades_from_trades_table():
    open_trades_payload = fetch_queries_as_dictionaries(f"""
SELECT
    TRADE_ID
    ,STOCK_NAME
    ,TRADE_DATE
    ,STOCK_QUANTITY
    ,BUY_OR_SELL
FROM
    TRADES
WHERE
    TRADE_EXIT_DATE IS NULL
    AND TRADE_ID NOT IN (SELECT DISTINCT TRD.TRADE_ID FROM TRADES TRD INNER JOIN FIN_REALISED_SWING_STOCK_RETURNS FSSRPV ON TRD.FEE_ID = FSSRPV.OPENING_FEE_ID AND FSSRPV.TRADES_CLOSE_STATUS = 'TRADES_COMPLETELY_CLOSED')
    AND TRADE_ID NOT IN (SELECT DISTINCT TRD.TRADE_ID FROM TRADES TRD INNER JOIN FIN_REALISED_SWING_STOCK_RETURNS FSSRPV ON TRD.FEE_ID = FSSRPV.CLOSING_FEE_ID AND FSSRPV.TRADES_CLOSE_STATUS = 'TRADES_COMPLETELY_CLOSED');
    """)
    return open_trades_payload

def create_close_trades_table():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('''
CREATE TABLE IF NOT EXISTS CLOSE_TRADES
(
    ID                           INTEGER       PRIMARY KEY AUTOINCREMENT,
    STOCK_SYMBOL                 VARCHAR (100),
    OPENING_TRADE_ID             VARCHAR (200),
    OPENING_TRADE_DATE           DATE,
    OPENING_TRADE_STOCK_QUANTITY INTEGER,
    OPENING_TRADE_BUY_OR_SELL    VARCHAR (10),
    CLOSING_TRADE_ID             VARCHAR (200),
    CLOSING_TRADE_DATE           DATE,
    CLOSING_TRADE_STOCK_QUANTITY INTEGER,
    CLOSING_TRADE_BUY_OR_SELL    VARCHAR (10),
    PROCESSING_DATE              DATE,
    PREVIOUS_PROCESSING_DATE     DATE,
    NEXT_PROCESSING_DATE         DATE,
    UPDATE_PROCESS_NAME          TEXT,
    UPDATE_PROCESS_ID            INTEGER,
    PROCESS_NAME                 TEXT,
    PROCESS_ID                   INTEGER,
    START_DATE                   DATE,
    END_DATE                     DATE,
    RECORD_DELETED_FLAG          INTEGER        DEFAULT 0
);
    ''')

def get_first_swing_trade_date_from_trades_table():
    first_swing_trade_data = fetch_queries_as_dictionaries("""
SELECT
    MIN(TRADE_DATE) AS FIRST_SWING_TRADE_DATE
FROM
    TRADES
WHERE
    TRADE_EXIT_DATE IS NULL;
    """)
    return first_swing_trade_data[0]

def get_unrealised_swing_stock_returns():
    unrealised_returns = fetch_queries_as_dictionaries("""
SELECT
    PROCESSING_DATE
    ,"FIN_%_P/L"
    ,"FIN_%_DAY_P/L"
FROM
    FIN_UNREALISED_STOCK_RETURNS
WHERE
    RECORD_DELETED_FLAG = 0
ORDER BY PROCESSING_DATE;
    """)
    return unrealised_returns

def get_first_purchase_date_from_all_portfolios():
    first_purchase_data = fetch_queries_as_dictionaries("""
SELECT 
    MIN(SUB.FIRST_PURCHASE_DATE) AS FIRST_PURCHASE_DATE
FROM 
(
SELECT
    MIN(MF.PURCHASED_ON) AS FIRST_PURCHASE_DATE
FROM
    MF_ORDER MF
UNION ALL
SELECT
    MIN(TRD.TRADE_DATE)  AS FIRST_PURCHASE_DATE
FROM
    TRADES TRD
) SUB;
    """)
    return first_purchase_data[0]

def get_consolidated_hist_returns_from_consolidated_hist_returns_table():
    consolidated_returns = fetch_queries_as_dictionaries("""
SELECT
    PROCESSING_DATE
    ,"%_FIN_TOTAL_P/L"
    ,"%_FIN_DAY_P/L"
FROM
    FIN_CONSOLIDATED_RETURNS
WHERE
    RECORD_DELETED_FLAG = 0
ORDER BY PROCESSING_DATE;
    """)
    return consolidated_returns

def get_all_from_consolidated_returns_table():
    agg_consolidated_returns_data = fetch_queries_as_dictionaries("""
SELECT
    PORTFOLIO_TYPE                   AS PORTFOLIO_TYPE
    ,PROCESSING_DATE                 AS PROCESSING_DATE
    ,PREVIOUS_PROCESSING_DATE        AS PREVIOUS_PROCESSING_DATE
    ,NEXT_PROCESSING_DATE            AS NEXT_PROCESSING_DATE
    ,ROUND(AGG_INVESTED_AMOUNT, 2)   AS AGG_INVESTED_AMOUNT
    ,ROUND(AGG_CURRENT_VALUE, 2)     AS AGG_CURRENT_VALUE
    ,ROUND(AGG_PREVIOUS_VALUE, 2)    AS AGG_PREVIOUS_VALUE
    ,ROUND("AGG_TOTAL_P/L", 2)       AS "AGG_TOTAL_P/L"
    ,ROUND("%_AGG_TOTAL_P/L", 2)     AS "%_AGG_TOTAL_P/L"
    ,ROUND("AGG_DAY_P/L", 2)         AS "AGG_DAY_P/L"
    ,ROUND("%_AGG_DAY_P/L", 2)       AS "%_AGG_DAY_P/L"
FROM
    AGG_CONSOLIDATED_RETURNS
WHERE
    RECORD_DELETED_FLAG = 0
 ORDER BY PROCESSING_DATE;
    """)
    latest_agg_consolidated_returns_data = fetch_queries_as_dictionaries("""
SELECT
    PORTFOLIO_TYPE                   AS PORTFOLIO_TYPE
    ,PROCESSING_DATE                 AS PROCESSING_DATE
    ,PREVIOUS_PROCESSING_DATE        AS PREVIOUS_PROCESSING_DATE
    ,NEXT_PROCESSING_DATE            AS NEXT_PROCESSING_DATE
    ,ROUND(AGG_INVESTED_AMOUNT, 2)   AS AGG_INVESTED_AMOUNT
    ,ROUND(AGG_CURRENT_VALUE, 2)     AS AGG_CURRENT_VALUE
    ,ROUND(AGG_PREVIOUS_VALUE, 2)    AS AGG_PREVIOUS_VALUE
    ,ROUND("AGG_TOTAL_P/L", 2)       AS "AGG_TOTAL_P/L"
    ,ROUND("%_AGG_TOTAL_P/L", 2)     AS "%_AGG_TOTAL_P/L"
    ,ROUND("AGG_DAY_P/L", 2)         AS "AGG_DAY_P/L"
    ,ROUND("%_AGG_DAY_P/L", 2)       AS "%_AGG_DAY_P/L"
FROM
    AGG_CONSOLIDATED_RETURNS
WHERE
    RECORD_DELETED_FLAG = 0
    AND PROCESSING_DATE = (SELECT MAX(PROCESSING_DATE) FROM AGG_CONSOLIDATED_RETURNS)
 ORDER BY PROCESSING_DATE;
    """)
    consolidated_returns_data = fetch_queries_as_dictionaries("""
SELECT
    PROCESSING_DATE                  AS PROCESSING_DATE
    ,PREVIOUS_PROCESSING_DATE        AS PREVIOUS_PROCESSING_DATE
    ,NEXT_PROCESSING_DATE            AS NEXT_PROCESSING_DATE
    ,ROUND(FIN_INVESTED_AMOUNT, 2)   AS INVESTED_AMOUNT
    ,ROUND(FIN_CURRENT_VALUE, 2)     AS CURRENT_VALUE
    ,ROUND(FIN_PREVIOUS_VALUE, 2)    AS PREVIOUS_VALUE
    ,ROUND("FIN_TOTAL_P/L", 2)       AS "TOTAL_P/L"
    ,ROUND("%_FIN_TOTAL_P/L", 2)     AS "%_TOTAL_P/L"
    ,ROUND("FIN_DAY_P/L", 2)         AS "DAY_P/L"
    ,ROUND("%_FIN_DAY_P/L", 2)       AS "%_DAY_P/L"
FROM
    FIN_CONSOLIDATED_RETURNS
WHERE
    RECORD_DELETED_FLAG = 0
ORDER BY PROCESSING_DATE;
    """)
    latest_consolidated_returns_data = fetch_queries_as_dictionaries("""
SELECT
    PROCESSING_DATE                  AS PROCESSING_DATE
    ,PREVIOUS_PROCESSING_DATE        AS PREVIOUS_PROCESSING_DATE
    ,NEXT_PROCESSING_DATE            AS NEXT_PROCESSING_DATE
    ,ROUND(FIN_INVESTED_AMOUNT, 2)   AS INVESTED_AMOUNT
    ,ROUND(FIN_CURRENT_VALUE, 2)     AS CURRENT_VALUE
    ,ROUND(FIN_PREVIOUS_VALUE, 2)    AS PREVIOUS_VALUE
    ,ROUND("FIN_TOTAL_P/L", 2)       AS "TOTAL_P/L"
    ,ROUND("%_FIN_TOTAL_P/L", 2)     AS "%_TOTAL_P/L"
    ,ROUND("FIN_DAY_P/L", 2)         AS "DAY_P/L"
    ,ROUND("%_FIN_DAY_P/L", 2)       AS "%_DAY_P/L"
FROM
    FIN_CONSOLIDATED_RETURNS
WHERE
    RECORD_DELETED_FLAG = 0
    AND PROCESSING_DATE = (SELECT MAX(PROCESSING_DATE) FROM CONSOLIDATED_RETURNS)
ORDER BY PROCESSING_DATE;
    """)
    data = {'agg_consolidated_returns_data'        : agg_consolidated_returns_data
            ,'consolidated_returns_data'           : consolidated_returns_data
            ,'latest_agg_consolidated_returns_data': latest_agg_consolidated_returns_data
            ,'latest_consolidated_returns_data'    : latest_consolidated_returns_data}
    return data

def get_consolidated_hist_allocation_portfolio_from_consolidated_hist_allocation_portfolio_table():
    consolidated_allocation = fetch_queries_as_dictionaries("""
SELECT
    PROCESSING_DATE
    ,PORTFOLIO_TYPE
    ,"FIN_ALLOC_%_PORTFOLIO_INVESTED_AMOUNT"
FROM
    FIN_CONSOLIDATED_ALLOCATION
WHERE
    RECORD_DELETED_FLAG = 0
ORDER BY PROCESSING_DATE;
    """)
    return consolidated_allocation

def get_all_from_consolidated_hist_allocation_table():
    fin_allocation_data = fetch_queries_as_dictionaries("""
SELECT 
    PORTFOLIO_TYPE
    ,FIN_INVESTED_AMOUNT
    ,"P/L"
FROM
    FIN_CONSOLIDATED_ALLOCATION
WHERE
    RECORD_DELETED_FLAG = 0
ORDER BY PROCESSING_DATE;
    """)
    agg_allocation_data = fetch_queries_as_dictionaries("""
SELECT 
    PORTFOLIO_CATEGORY
    ,INVESTED_AMOUNT
    ,"P/L"
FROM
    AGG_CONSOLIDATED_ALLOCATION
WHERE
    RECORD_DELETED_FLAG = 0
ORDER BY PROCESSING_DATE;
    """)
    allocation_data = fetch_queries_as_dictionaries("""
SELECT 
    PORTFOLIO_NAME
    ,INVESTED_AMOUNT
    ,"P/L"
FROM
    CONSOLIDATED_ALLOCATION
WHERE
    RECORD_DELETED_FLAG = 0
ORDER BY PROCESSING_DATE;
    """)

    # Latest Data Fetch

    latest_fin_allocation_data = fetch_queries_as_dictionaries("""
SELECT 
    PORTFOLIO_TYPE
    ,FIN_INVESTED_AMOUNT
    ,"P/L"
FROM
    FIN_CONSOLIDATED_ALLOCATION
WHERE
    RECORD_DELETED_FLAG = 0
    AND PROCESSING_DATE = (SELECT MAX(PROCESSING_DATE) FROM FIN_CONSOLIDATED_ALLOCATION)
ORDER BY PROCESSING_DATE;
    """)
    latest_agg_allocation_data = fetch_queries_as_dictionaries("""
SELECT 
    PORTFOLIO_CATEGORY
    ,INVESTED_AMOUNT
    ,"P/L"
FROM
    AGG_CONSOLIDATED_ALLOCATION
WHERE
    RECORD_DELETED_FLAG = 0
    AND PROCESSING_DATE = (SELECT MAX(PROCESSING_DATE) FROM AGG_CONSOLIDATED_ALLOCATION)
ORDER BY PROCESSING_DATE;
    """)
    latest_allocation_data = fetch_queries_as_dictionaries("""
SELECT 
    PORTFOLIO_NAME
    ,INVESTED_AMOUNT
    ,"P/L"
FROM
    CONSOLIDATED_ALLOCATION
WHERE
    RECORD_DELETED_FLAG = 0
    AND PROCESSING_DATE = (SELECT MAX(PROCESSING_DATE) FROM CONSOLIDATED_ALLOCATION)
ORDER BY PROCESSING_DATE;
    """)

    data = {'fin_allocation_data'        : fin_allocation_data,
            'agg_allocation_data'        : agg_allocation_data,
            'allocation_data'            : allocation_data,
            'latest_fin_allocation_data' : latest_fin_allocation_data,
            'latest_agg_allocation_data' : latest_agg_allocation_data,
            'latest_allocation_data'     : latest_allocation_data}
    return data

def create_simulated_portfolio_table():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("""
CREATE TABLE IF NOT EXISTS SIMULATED_RETURNS
(
    ID                       INTEGER PRIMARY KEY AUTOINCREMENT,
    SIM_FUND_NAME            TEXT,
    SIM_BASE_TYPE            TEXT,
    SIM_BASE_NAME            TEXT,
    SIM_ALLOCATION_CATEGORY  TEXT,
    SIM_PURCHASE_DATE        DATE,
    SIM_NAV_DURING_PURCHASE  REAL,
    SIM_HOLDING_DAYS         REAL,
    SIM_CURRENT_NAV          REAL,
    SIM_INVESTED_AMOUNT      REAL,
    SIM_FUND_UNITS           REAL,
    SIM_CURRENT_AMOUNT       REAL,
    "SIM_P/L"                REAL,
    "SIM_%_P/L"              REAL,
    SIM_PREVIOUS_NAV         REAL,
    SIM_PREVIOUS_AMOUNT      REAL,
    "SIM_DAY_P/L"            REAL,
    "SIM_%_DAY_P/L"          REAL,
    PROCESSING_DATE          DATE,
    PREVIOUS_PROCESSING_DATE DATE,
    NEXT_PROCESSING_DATE     DATE,
    UPDATE_PROCESS_NAME      TEXT,
    UPDATE_PROCESS_ID        INTEGER,
    PROCESS_NAME             TEXT,
    PROCESS_ID               INTEGER,
    START_DATE               DATE,
    END_DATE                 DATE,
    RECORD_DELETED_FLAG      INTEGER
);
    """)

def create_agg_simulated_portfolio_table():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("""
CREATE TABLE IF NOT EXISTS AGG_SIMULATED_RETURNS
(
    ID                          INTEGER PRIMARY KEY AUTOINCREMENT,
    AGG_SIM_FUND_NAME           TEXT,
    AGG_SIM_BASE_TYPE           TEXT,
    AGG_SIM_ALLOCATION_CATEGORY TEXT,
    AGG_SIM_INVESTED_AMOUNT     REAL,
    AGG_SIM_FUND_UNITS          REAL,
    AGG_SIM_CURRENT_AMOUNT      REAL,
    AGG_SIM_PREVIOUS_AMOUNT     REAL,
    "AGG_SIM_P/L"               REAL,
    "AGG_SIM_DAY_P/L"           REAL,
    "AGG_%_SIM_P/L"             REAL,
    "AGG_%_SIM_DAY_P/L"         REAL,
    AGG_SIM_AVG_PRICE           REAL,
    PROCESSING_DATE             DATE,
    PREVIOUS_PROCESSING_DATE    DATE,
    NEXT_PROCESSING_DATE        DATE,
    UPDATE_PROCESS_NAME         TEXT,
    UPDATE_PROCESS_ID           INTEGER,
    PROCESS_NAME                TEXT,
    PROCESS_ID                  INTEGER,
    START_DATE                  DATE,
    END_DATE                    DATE,
    RECORD_DELETED_FLAG         INTEGER
);
    """)

def create_fin_simulated_portfolio_table():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("""
CREATE TABLE IF NOT EXISTS FIN_SIMULATED_RETURNS
(
    ID                          INTEGER PRIMARY KEY AUTOINCREMENT,
    FIN_SIM_FUND_NAME           TEXT,
    FIN_SIM_ALLOCATION_CATEGORY TEXT,
    FIN_SIM_INVESTED_AMOUNT     REAL,
    FIN_SIM_FUND_UNITS          REAL,
    FIN_SIM_CURRENT_AMOUNT      REAL,
    FIN_SIM_PREVIOUS_AMOUNT     REAL,
    "FIN_SIM_P/L"               REAL,
    "FIN_SIM_DAY_P/L"           REAL,
    "FIN_%_SIM_P/L"             REAL,
    "FIN_%_SIM_DAY_P/L"         REAL,
    FIN_SIM_AVG_PRICE           REAL,
    PROCESSING_DATE             DATE,
    PREVIOUS_PROCESSING_DATE    DATE,
    NEXT_PROCESSING_DATE        DATE,
    UPDATE_PROCESS_NAME         TEXT,
    UPDATE_PROCESS_ID           INTEGER,
    PROCESS_NAME                TEXT,
    PROCESS_ID                  INTEGER,
    START_DATE                  DATE,
    END_DATE                    DATE,
    RECORD_DELETED_FLAG         INTEGER
);
    """)

def get_simulated_returns_from_fin_simulated_returns_table():
    simulated_returns_dict = fetch_queries_as_dictionaries("""
SELECT
    CONS.PROCESSING_DATE
    ,CONS."%_FIN_TOTAL_P/L"
    ,CONS."%_FIN_DAY_P/L"
    ,SIM."FIN_%_SIM_P/L"
    ,SIM."FIN_%_SIM_DAY_P/L"
FROM
    FIN_CONSOLIDATED_RETURNS CONS
INNER JOIN
    FIN_SIMULATED_RETURNS SIM
ON
    CONS.PROCESSING_DATE = SIM.PROCESSING_DATE
WHERE
    CONS.RECORD_DELETED_FLAG = 0
    AND SIM.RECORD_DELETED_FLAG = 0
ORDER BY CONS.PROCESSING_DATE;
    """)
    return simulated_returns_dict

def create_mutual_fund_returns_table():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("""
CREATE TABLE IF NOT EXISTS MUTUAL_FUND_RETURNS
(
    FUND_NAME                TEXT,
    FUND_AMC                 TEXT,
    FUND_TYPE                TEXT,
    FUND_CATEGORY            TEXT,
    ALLOCATION_CATEGORY      TEXT,
    FUND_PURCHASE_DATE       DATE,
    NAV_DURING_PURCHASE      REAL,
    HOLDING_DAYS             REAL,
    CURRENT_NAV              REAL,
    INVESTED_AMOUNT          REAL,
    AMC_AMOUNT               REAL,
    STAMP_FEES_AMOUNT        REAL,
    FUND_UNITS               REAL,
    CURRENT_AMOUNT           REAL,
    "P/L"                    REAL,
    "%_P/L"                  REAL,
    PREVIOUS_NAV             REAL,
    PREVIOUS_AMOUNT          REAL,
    "DAY_P/L"                REAL,
    "%_DAY_P/L"              REAL,
    PROCESSING_DATE          DATE,
    PREVIOUS_PROCESSING_DATE DATE,
    NEXT_PROCESSING_DATE     DATE,
    UPDATE_PROCESS_NAME      TEXT,
    UPDATE_PROCESS_ID        INTEGER,
    PROCESS_NAME             TEXT,
    PROCESS_ID               INTEGER,
    START_DATE               DATE,
    END_DATE                 DATE,
    RECORD_DELETED_FLAG      INTEGER
);
    """)

def create_agg_mutual_fund_returns_table():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("""
CREATE TABLE IF NOT EXISTS AGG_MUTUAL_FUND_RETURNS
(
    FUND_NAME                TEXT,
    FUND_AMC                 TEXT,
    FUND_TYPE                TEXT,
    FUND_CATEGORY            TEXT,
    ALLOCATION_CATEGORY      TEXT,
    AGG_INVESTED_AMOUNT      REAL,
    AGG_AMC_AMOUNT           REAL,
    AGG_FUND_UNITS           REAL,
    AGG_CURRENT_AMOUNT       REAL,
    AGG_PREVIOUS_AMOUNT      REAL,
    "AGG_P/L"                REAL,
    "AGG_DAY_P/L"            REAL,
    "AGG_%_P/L"              REAL,
    "AGG_%_DAY_P/L"          REAL,
    AGG_AVG_PRICE            REAL,
    PROCESSING_DATE          DATE,
    PREVIOUS_PROCESSING_DATE DATE,
    NEXT_PROCESSING_DATE     DATE,
    UPDATE_PROCESS_NAME      TEXT,
    UPDATE_PROCESS_ID        INTEGER,
    PROCESS_NAME             TEXT,
    PROCESS_ID               INTEGER,
    START_DATE               DATE,
    END_DATE                 DATE,
    RECORD_DELETED_FLAG      INTEGER
);
    """)

def create_fin_mutual_fund_returns_table():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("""
CREATE TABLE IF NOT EXISTS FIN_MUTUAL_FUND_RETURNS
(
    "FIN_P/L"                REAL,
    FIN_AMC_AMOUNT           REAL,
    FIN_CURRENT_AMOUNT       REAL,
    FIN_PREVIOUS_AMOUNT      REAL,
    "FIN_%_P/L"              REAL,
    "FIN_DAY_P/L"            REAL,
    "FIN_%_DAY_P/L"          REAL,
    PROCESSING_DATE          DATE,
    PREVIOUS_PROCESSING_DATE DATE,
    NEXT_PROCESSING_DATE     DATE,
    UPDATE_PROCESS_NAME      TEXT,
    UPDATE_PROCESS_ID        INTEGER,
    PROCESS_NAME             TEXT,
    PROCESS_ID               INTEGER,
    START_DATE               DATE,
    END_DATE                 DATE,
    RECORD_DELETED_FLAG      INTEGER
);
    """)

def create_unrealised_stock_returns_table():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("""
CREATE TABLE IF NOT EXISTS UNREALISED_STOCK_RETURNS
(
    STOCK_NAME               TEXT,
    ALLOCATION_CATEGORY      TEXT,
    TRADE_DATE               DATE,
    STOCK_QUANTITY           REAL,
    TRADE_PRICE              REAL,
    CURRENT_PRICE            REAL,
    INVESTED_AMOUNT          REAL,
    TOTAL_FEES               REAL,
    TOTAL_INVESTED_AMOUNT    REAL,
    CURRENT_VALUE            REAL,
    "P/L"                    REAL,
    "%_P/L"                  REAL,
    "NET_P/L"                REAL,
    "%_NET_P/L"              REAL,
    PREVIOUS_PRICE           REAL,
    PREVIOUS_VALUE           REAL,
    "DAY_P/L"                REAL,
    "%_DAY_P/L"              REAL,
    PROCESSING_DATE          DATE,
    PREVIOUS_PROCESSING_DATE DATE,
    NEXT_PROCESSING_DATE     DATE,
    UPDATE_PROCESS_NAME      TEXT,
    UPDATE_PROCESS_ID        INTEGER,
    PROCESS_NAME             TEXT,
    PROCESS_ID               INTEGER,
    START_DATE               DATE,
    END_DATE                 DATE,
    RECORD_DELETED_FLAG      INTEGER
);
    """)

def create_agg_unrealised_stock_returns_table():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("""
CREATE TABLE IF NOT EXISTS AGG_UNREALISED_STOCK_RETURNS
(
    STOCK_NAME                TEXT,
    ALLOCATION_CATEGORY       TEXT,
    CURRENT_PRICE             REAL,
    PREVIOUS_PRICE            REAL,
    AGG_STOCK_QUANTITY        REAL,
    AVG_TRADE_PRICE           REAL,
    AGG_INVESTED_AMOUNT       REAL,
    AGG_TOTAL_FEES            REAL,
    AGG_TOTAL_INVESTED_AMOUNT REAL,
    AGG_CURRENT_VALUE         REAL,
    "AGG_P/L"                 REAL,
    "AGG_%_P/L"               REAL,
    "AGG_NET_P/L"             REAL,
    "AGG_%_NET_P/L"           REAL,
    AGG_PREVIOUS_VALUE        REAL,
    "AGG_DAY_P/L"             REAL,
    "AGG_%_DAY_P/L"           REAL,
    PROCESSING_DATE           DATE,
    PREVIOUS_PROCESSING_DATE  DATE,
    NEXT_PROCESSING_DATE      DATE,
    UPDATE_PROCESS_NAME       TEXT,
    UPDATE_PROCESS_ID         INTEGER,
    PROCESS_NAME              TEXT,
    PROCESS_ID                INTEGER,
    START_DATE                DATE,
    END_DATE                  DATE,
    RECORD_DELETED_FLAG       INTEGER
);
    """)

def create_fin_unrealised_stock_returns_table():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("""
CREATE TABLE IF NOT EXISTS FIN_UNREALISED_STOCK_RETURNS
(
    FIN_INVESTED_AMOUNT       REAL,
    FIN_TOTAL_FEES            REAL,
    FIN_TOTAL_INVESTED_AMOUNT REAL,
    FIN_CURRENT_VALUE         REAL,
    FIN_PREVIOUS_VALUE        REAL,
    "FIN_P/L"                 REAL,
    "FIN_%_P/L"               REAL,
    "FIN_NET_P/L"             REAL,
    "FIN_NET_%_P/L"           REAL,
    "FIN_DAY_P/L"             REAL,
    "FIN_%_DAY_P/L"           REAL,
    PROCESSING_DATE           DATE,
    PREVIOUS_PROCESSING_DATE  DATE,
    NEXT_PROCESSING_DATE      DATE,
    UPDATE_PROCESS_NAME       TEXT,
    UPDATE_PROCESS_ID         INTEGER,
    PROCESS_NAME              TEXT,
    PROCESS_ID                INTEGER,
    START_DATE                DATE,
    END_DATE                  DATE,
    RECORD_DELETED_FLAG       INTEGER
);
    """)

def create_realised_intraday_stock_returns_table():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("""
CREATE TABLE IF NOT EXISTS REALISED_INTRADAY_STOCK_RETURNS
(
    TRADE_DATE                 DATE,
    STOCK_NAME                 TEXT,
    TRADE_SET_ID               TEXT,
    TRADE_SET                  INTEGER,
    LEVERAGE                   INTEGER,
    TRADE_TYPE                 TEXT,
    TRADE_POSITION             TEXT,
    FEE_ID                     TEXT,
    STOCK_QUANTITY             INTEGER,
    PERCEIVED_BUY_PRICE        REAL,
    PERCEIVED_SELL_PRICE       REAL,
    ACTUAL_BUY_PRICE           REAL,
    ACTUAL_SELL_PRICE          REAL,
    "P/L"                      REAL,
    PERCEIVED_DEPLOYED_CAPITAL REAL,
    ACTUAL_DEPLOYED_CAPITAL    REAL,
    "%_P/L_WITHOUT_LEVERAGE"   REAL,
    "%_P/L_WITH_LEVERAGE"      REAL,
    PROCESSING_DATE            DATE,
    PREVIOUS_PROCESSING_DATE   DATE,
    NEXT_PROCESSING_DATE       DATE,
    UPDATE_PROCESS_NAME        TEXT,
    UPDATE_PROCESS_ID          INTEGER,
    PROCESS_NAME               TEXT,
    PROCESS_ID                 INTEGER,
    START_DATE                 DATE,
    END_DATE                   DATE,
    RECORD_DELETED_FLAG        INTEGER
);
    """)

def create_agg_realised_intraday_stock_returns_table():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("""
CREATE TABLE IF NOT EXISTS AGG_REALISED_INTRADAY_STOCK_RETURNS
(
    TRADE_DATE                     TEXT,
    STOCK_NAME                     TEXT,
    LEVERAGE                       REAL,
    TRADE_TYPE                     TEXT,
    FEE_ID                         TEXT,
    AGG_PERCEIVED_DEPLOYED_CAPITAL REAL,
    AGG_ACTUAL_DEPLOYED_CAPITAL    REAL,
    "AGG_P/L"                      REAL,
    "%_P/L_WITHOUT_LEVERAGE"       REAL,
    "%_P/L_WITH_LEVERAGE"          REAL,
    PROCESSING_DATE                DATE,
    PREVIOUS_PROCESSING_DATE       DATE,
    NEXT_PROCESSING_DATE           DATE,
    UPDATE_PROCESS_NAME            TEXT,
    UPDATE_PROCESS_ID              INTEGER,
    PROCESS_NAME                   TEXT,
    PROCESS_ID                     INTEGER,
    START_DATE                     DATE,
    END_DATE                       DATE,
    RECORD_DELETED_FLAG            INTEGER
);
    """)

def create_fin_realised_intraday_stock_returns_table():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("""
CREATE TABLE IF NOT EXISTS FIN_REALISED_INTRADAY_STOCK_RETURNS
(
    TRADE_DATE                                DATE,
    TRADE_TYPE                                TEXT,
    FEE_ID                                    TEXT,
    AGG_PERCEIVED_DEPLOYED_CAPITAL            REAL,
    AGG_ACTUAL_DEPLOYED_CAPITAL               REAL,
    "AGG_P/L"                                 REAL,
    "%_P/L_WITHOUT_LEVERAGE"                  REAL,
    "%_P/L_WITH_LEVERAGE"                     REAL,
    NET_OBLIGATION                            REAL,
    "AGG_P/L_NET_OBLIGATION_MATCH_STATUS"     TEXT,
    BROKERAGE                                 REAL,
    EXCHANGE_TRANSACTION_CHARGES              REAL,
    IGST                                      REAL,
    SECURITIES_TRANSACTION_TAX                REAL,
    SEBI_TURN_OVER_FEES                       REAL,
    TOTAL_FEES                                REAL,
    "NET_P/L"                                 REAL,
    "NET_%_P/L_WITHOUT_LEVERAGE"              REAL,
    "NET_%_P/L_WITH_LEVERAGE"                 REAL,
    TOTAL_CHARGES                             REAL,
    "NET_P/L_MINUS_CHARGES"                   REAL,
    "NET_%_P/L_WITHOUT_LEVERAGE_INCL_CHARGES" REAL,
    "NET_%_P/L_WITH_LEVERAGE_INCL_CHARGES"    REAL,
    PROCESSING_DATE                           DATE,
    PREVIOUS_PROCESSING_DATE                  DATE,
    NEXT_PROCESSING_DATE                      DATE,
    UPDATE_PROCESS_NAME                       TEXT,
    UPDATE_PROCESS_ID                         INTEGER,
    PROCESS_NAME                              TEXT,
    PROCESS_ID                                INTEGER,
    START_DATE                                DATE,
    END_DATE                                  DATE,
    RECORD_DELETED_FLAG                       INTEGER
);
    """)

def create_realised_swing_stock_returns_table():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("""
CREATE TABLE IF NOT EXISTS REALISED_SWING_STOCK_RETURNS
(
    STOCK_NAME               TEXT,
    OPENING_TRADE_ID         TEXT,
    CLOSING_TRADE_ID         TEXT,
    OPENING_FEE_ID           TEXT,
    CLOSING_FEE_ID           TEXT,
    OPENING_STOCK_QUANTITY   REAL,
    CLOSING_STOCK_QUANTITY   REAL,
    TRADE_OPEN_DATE          DATE,
    TRADE_CLOSE_DATE         DATE,
    BUY_PRICE                REAL,
    SELL_PRICE               REAL,
    SELL_MINUS_BUY_PRICE     REAL,
    "P/L"                    REAL,
    PROCESSING_DATE          DATE,
    PREVIOUS_PROCESSING_DATE DATE,
    NEXT_PROCESSING_DATE     DATE,
    UPDATE_PROCESS_NAME      TEXT,
    UPDATE_PROCESS_ID        INTEGER,
    PROCESS_NAME             TEXT,
    PROCESS_ID               INTEGER,
    START_DATE               DATE,
    END_DATE                 DATE,
    RECORD_DELETED_FLAG      INTEGER
);
    """)

def create_agg_realised_swing_stock_returns_table():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("""
CREATE TABLE IF NOT EXISTS AGG_REALISED_SWING_STOCK_RETURNS
(
    STOCK_NAME                 TEXT,
    OPENING_FEE_ID             INTEGER,
    CLOSING_FEE_ID             INTEGER,
    TRADE_OPEN_DATE            TEXT,
    TRADE_CLOSE_DATE           TEXT,
    AGG_OPENING_STOCK_QUANTITY REAL,
    AGG_CLOSING_STOCK_QUANTITY REAL,
    REMAINING_STOCK_BALANCE    REAL,
    TRADES_CLOSE_STATUS        TEXT,
    AGG_OPENING_BUY_PRICE      REAL,
    AGG_CLOSING_SELL_PRICE     REAL,
    "AGG_P/L"                  REAL,
    "AGG_%_P/L"                REAL,
    PROCESSING_DATE            DATE,
    PREVIOUS_PROCESSING_DATE   DATE,
    NEXT_PROCESSING_DATE       DATE,
    UPDATE_PROCESS_NAME        TEXT,
    UPDATE_PROCESS_ID          INTEGER,
    PROCESS_NAME               TEXT,
    PROCESS_ID                 INTEGER,
    START_DATE                 DATE,
    END_DATE                   DATE,
    RECORD_DELETED_FLAG        INTEGER
);
    """)

def create_fin_realised_swing_stock_returns_table():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("""
CREATE TABLE IF NOT EXISTS FIN_REALISED_SWING_STOCK_RETURNS
(
    STOCK_NAME                 TEXT,
    OPENING_FEE_ID             INTEGER,
    CLOSING_FEE_ID             INTEGER,
    TRADE_OPEN_DATE            TEXT,
    TRADE_CLOSE_DATE           TEXT,
    AGG_OPENING_STOCK_QUANTITY REAL,
    AGG_CLOSING_STOCK_QUANTITY REAL,
    REMAINING_STOCK_BALANCE    REAL,
    TRADES_CLOSE_STATUS        TEXT,
    AGG_OPENING_BUY_PRICE      REAL,
    OPEN_NET_OBLIGATION        REAL,
    OPEN_MATCH_STATUS          TEXT,
    OPEN_TOTAL_BROKERAGE       REAL,
    OPEN_TOTAL_CHARGES         REAL,
    OPEN_TOTAL_FEES            REAL,
    AGG_CLOSING_SELL_PRICE     REAL,
    CLOSE_NET_OBLIGATION       REAL,
    CLOSE_MATCH_STATUS         TEXT,
    CLOSE_TOTAL_BROKERAGE      REAL,
    CLOSE_TOTAL_CHARGES        REAL,
    CLOSE_TOTAL_FEES           REAL,
    TOTAL_FEES                 REAL,
    "AGG_P/L"                  REAL,
    "AGG_%_P/L"                REAL,
    "NET_P/L"                  REAL,
    "NET_%_P/L"                REAL,
    PROCESSING_DATE            DATE,
    PREVIOUS_PROCESSING_DATE   DATE,
    NEXT_PROCESSING_DATE       DATE,
    UPDATE_PROCESS_NAME        TEXT,
    UPDATE_PROCESS_ID          INTEGER,
    PROCESS_NAME               TEXT,
    PROCESS_ID                 INTEGER,
    START_DATE                 DATE,
    END_DATE                   DATE,
    RECORD_DELETED_FLAG        INTEGER
);
    """)

def create_consolidated_returns_table():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("""
CREATE TABLE IF NOT EXISTS CONSOLIDATED_RETURNS
(
    PORTFOLIO_TYPE           TEXT,
    INVESTED_AMOUNT          REAL,
    CURRENT_VALUE            REAL,
    PREVIOUS_VALUE           REAL,
    "TOTAL_P/L"              REAL,
    "DAY_P/L"                REAL,
    "%_TOTAL_P/L"            REAL,
    "%_DAY_P/L"              REAL,
    PROCESSING_DATE          DATE,
    PREVIOUS_PROCESSING_DATE DATE,
    NEXT_PROCESSING_DATE     DATE,
    UPDATE_PROCESS_NAME      TEXT,
    UPDATE_PROCESS_ID        INTEGER,
    PROCESS_NAME             TEXT,
    PROCESS_ID               INTEGER,
    START_DATE               DATE,
    END_DATE                 DATE,
    RECORD_DELETED_FLAG      INTEGER
);
    """)

def create_agg_consolidated_returns_table():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("""
CREATE TABLE IF NOT EXISTS AGG_CONSOLIDATED_RETURNS
(
    PORTFOLIO_TYPE           TEXT,
    AGG_INVESTED_AMOUNT      REAL,
    AGG_CURRENT_VALUE        REAL,
    AGG_PREVIOUS_VALUE       REAL,
    "AGG_TOTAL_P/L"          REAL,
    "AGG_DAY_P/L"            REAL,
    "%_AGG_TOTAL_P/L"        REAL,
    "%_AGG_DAY_P/L"          REAL,
    PROCESSING_DATE          DATE,
    PREVIOUS_PROCESSING_DATE DATE,
    NEXT_PROCESSING_DATE     DATE,
    UPDATE_PROCESS_NAME      TEXT,
    UPDATE_PROCESS_ID        INTEGER,
    PROCESS_NAME             TEXT,
    PROCESS_ID               INTEGER,
    START_DATE               DATE,
    END_DATE                 DATE,
    RECORD_DELETED_FLAG      INTEGER
);
    """)

def create_fin_consolidated_returns_table():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("""
CREATE TABLE IF NOT EXISTS FIN_CONSOLIDATED_RETURNS
(
    FIN_INVESTED_AMOUNT      REAL,
    FIN_CURRENT_VALUE        REAL,
    FIN_PREVIOUS_VALUE       REAL,
    "FIN_TOTAL_P/L"          REAL,
    "FIN_DAY_P/L"            REAL,
    "%_FIN_TOTAL_P/L"        REAL,
    "%_FIN_DAY_P/L"          REAL,
    PROCESSING_DATE          DATE,
    PREVIOUS_PROCESSING_DATE DATE,
    NEXT_PROCESSING_DATE     DATE,
    UPDATE_PROCESS_NAME      TEXT,
    UPDATE_PROCESS_ID        INTEGER,
    PROCESS_NAME             TEXT,
    PROCESS_ID               INTEGER,
    START_DATE               DATE,
    END_DATE                 DATE,
    RECORD_DELETED_FLAG      INTEGER
);
    """)

def create_consolidated_allocation_table():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("""
CREATE TABLE IF NOT EXISTS CONSOLIDATED_ALLOCATION
(
    PORTFOLIO_TYPE                                                TEXT,
    PORTFOLIO_NAME                                                TEXT,
    PORTFOLIO_HOUSE                                               TEXT,
    PORTFOLIO_SUB_TYPE                                            TEXT,
    PORTFOLIO_CATEGORY                                            TEXT,
    INVESTED_AMOUNT                                               REAL,
    INVESTED_AMOUNT_EXCLUDING_FEES                                REAL,
    QUANTITY                                                      REAL,
    CURRENT_VALUE                                                 REAL,
    PREVIOUS_VALUE                                                REAL,
    "P/L"                                                         REAL,
    "%_P_L"                                                       REAL,
    "DAY_P/L"                                                     REAL,
    "%_DAY_P/L"                                                   REAL,
    AVERAGE_PRICE                                                 REAL,
    AGG_INVESTED_AMOUNT                                           REAL,
    AGG_INVESTED_AMOUNT_EXCLUDING_FEES                            REAL,
    AGG_QUANTITY                                                  REAL,
    AGG_CURRENT_AMOUNT                                            REAL,
    AGG_PREVIOUS_AMOUNT                                           REAL,
    "AGG_P/L"                                                     REAL,
    "AGG_DAY_P/L"                                                 REAL,
    "AGG_ALLOC_%_PORTFOLIO_INVESTED_AMOUNT_TYP_CD"                TEXT,
    "AGG_ALLOC_%_PORTFOLIO_INVESTED_AMOUNT"                       REAL,
    "AGG_ALLOC_%_PORTFOLIO_INVESTED_AMOUNT_EXCLUDING_FEES_TYP_CD" TEXT,
    "AGG_ALLOC_%_PORTFOLIO_INVESTED_AMOUNT_EXCLUDING_FEES"        REAL,
    "AGG_ALLOC_%_QUANTITY_TYP_CD"                                 TEXT,
    "AGG_ALLOC_%_QUANTITY"                                        REAL,
    "AGG_ALLOC_%_CURRENT_VALUE_TYP_CD"                            TEXT,
    "AGG_ALLOC_%_CURRENT_VALUE"                                   REAL,
    "AGG_ALLOC_%_PREVIOUS_VALUE_TYP_CD"                           TEXT,
    "AGG_ALLOC_%_PREVIOUS_VALUE"                                  REAL,
    "AGG_ALLOC_%_P/L_TYP_CD"                                      TEXT,
    "AGG_ALLOC_%_P/L"                                             REAL,
    "AGG_ALLOC_%_DAY_P/L_TYP_CD"                                  TEXT,
    "AGG_ALLOC_%_DAY_P/L"                                         REAL,
    PROCESSING_DATE                                               DATE,
    PREVIOUS_PROCESSING_DATE                                      DATE,
    NEXT_PROCESSING_DATE                                          DATE,
    UPDATE_PROCESS_NAME                                           TEXT,
    UPDATE_PROCESS_ID                                             INTEGER,
    PROCESS_NAME                                                  TEXT,
    PROCESS_ID                                                    INTEGER,
    START_DATE                                                    DATE,
    END_DATE                                                      DATE,
    RECORD_DELETED_FLAG                                           INTEGER
);
    """)

def create_agg_consolidated_allocation_table():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("""
CREATE TABLE IF NOT EXISTS AGG_CONSOLIDATED_ALLOCATION
(
    PORTFOLIO_TYPE                                                TEXT,
    PORTFOLIO_NAME                                                TEXT,
    PORTFOLIO_HOUSE                                               TEXT,
    PORTFOLIO_SUB_TYPE                                            TEXT,
    PORTFOLIO_CATEGORY                                            TEXT,
    INVESTED_AMOUNT                                               REAL,
    INVESTED_AMOUNT_EXCLUDING_FEES                                REAL,
    QUANTITY                                                      REAL,
    CURRENT_VALUE                                                 REAL,
    PREVIOUS_VALUE                                                REAL,
    "P/L"                                                         REAL,
    "%_P_L"                                                       REAL,
    "DAY_P/L"                                                     REAL,
    "%_DAY_P/L"                                                   REAL,
    AVERAGE_PRICE                                                 REAL,
    FIN_INVESTED_AMOUNT                                           REAL,
    FIN_INVESTED_AMOUNT_EXCLUDING_FEES                            REAL,
    FIN_QUANTITY                                                  REAL,
    FIN_CURRENT_AMOUNT                                            REAL,
    FIN_PREVIOUS_AMOUNT                                           REAL,
    "FIN_P/L"                                                     REAL,
    "FIN_DAY_P/L"                                                 REAL,
    "FIN_ALLOC_%_PORTFOLIO_INVESTED_AMOUNT_TYP_CD"                TEXT,
    "FIN_ALLOC_%_PORTFOLIO_INVESTED_AMOUNT"                       REAL,
    "FIN_ALLOC_%_PORTFOLIO_INVESTED_AMOUNT_EXCLUDING_FEES_TYP_CD" TEXT,
    "FIN_ALLOC_%_PORTFOLIO_INVESTED_AMOUNT_EXCLUDING_FEES"        REAL,
    "FIN_ALLOC_%_QUANTITY_TYP_CD"                                 TEXT,
    "FIN_ALLOC_%_QUANTITY"                                        REAL,
    "FIN_ALLOC_%_CURRENT_VALUE_TYP_CD"                            TEXT,
    "FIN_ALLOC_%_CURRENT_VALUE"                                   REAL,
    "FIN_ALLOC_%_PREVIOUS_VALUE_TYP_CD"                           TEXT,
    "FIN_ALLOC_%_PREVIOUS_VALUE"                                  REAL,
    "FIN_ALLOC_%_P/L_TYP_CD"                                      TEXT,
    "FIN_ALLOC_%_P/L"                                             REAL,
    "FIN_ALLOC_%_DAY_P/L_TYP_CD"                                  TEXT,
    "FIN_ALLOC_%_DAY_P/L"                                         REAL,
    PROCESSING_DATE                                               DATE,
    PREVIOUS_PROCESSING_DATE                                      DATE,
    NEXT_PROCESSING_DATE                                          DATE,
    UPDATE_PROCESS_NAME                                           TEXT,
    UPDATE_PROCESS_ID                                             INTEGER,
    PROCESS_NAME                                                  TEXT,
    PROCESS_ID                                                    INTEGER,
    START_DATE                                                    DATE,
    END_DATE                                                      DATE,
    RECORD_DELETED_FLAG                                           INTEGER
);
    """)

def create_fin_consolidated_allocation_table():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("""
CREATE TABLE IF NOT EXISTS FIN_CONSOLIDATED_ALLOCATION
(
    PORTFOLIO_TYPE                                                TEXT,
    FIN_INVESTED_AMOUNT                                           REAL,
    FIN_INVESTED_AMOUNT_EXCLUDING_FEES                            REAL,
    FIN_QUANTITY                                                  REAL,
    FIN_CURRENT_VALUE                                             REAL,
    FIN_PREVIOUS_VALUE                                            REAL,
    "P/L"                                                         REAL,
    "DAY_P/L"                                                     REAL,
    AVERAGE_PRICE                                                 REAL,
    FIN_AGG_INVESTED_AMOUNT                                       REAL,
    FIN_AGG_INVESTED_AMOUNT_EXCLUDING_FEES                        REAL,
    FIN_AGG_QUANTITY                                              REAL,
    FIN_AGG_CURRENT_AMOUNT                                        REAL,
    FIN_AGG_PREVIOUS_AMOUNT                                       REAL,
    "FIN_AGG_P/L"                                                 REAL,
    "FIN_AGG_DAY_P/L"                                             REAL,
    "FIN_ALLOC_%_PORTFOLIO_INVESTED_AMOUNT_TYP_CD"                TEXT,
    "FIN_ALLOC_%_PORTFOLIO_INVESTED_AMOUNT"                       REAL,
    "FIN_ALLOC_%_PORTFOLIO_INVESTED_AMOUNT_EXCLUDING_FEES_TYP_CD" TEXT,
    "FIN_ALLOC_%_PORTFOLIO_INVESTED_AMOUNT_EXCLUDING_FEES"        REAL,
    "FIN_ALLOC_%_QUANTITY_TYP_CD"                                 TEXT,
    "FIN_ALLOC_%_QUANTITY"                                        REAL,
    "FIN_ALLOC_%_CURRENT_VALUE_TYP_CD"                            TEXT,
    "FIN_ALLOC_%_CURRENT_VALUE"                                   REAL,
    "FIN_ALLOC_%_PREVIOUS_VALUE_TYP_CD"                           TEXT,
    "FIN_ALLOC_%_PREVIOUS_VALUE"                                  REAL,
    "FIN_ALLOC_%_P/L_TYP_CD"                                      TEXT,
    "FIN_ALLOC_%_P/L"                                             REAL,
    "FIN_ALLOC_%_DAY_P/L_TYP_CD"                                  TEXT,
    "FIN_ALLOC_%_DAY_P/L"                                         REAL,
    PROCESSING_DATE                                               DATE,
    PREVIOUS_PROCESSING_DATE                                      DATE,
    NEXT_PROCESSING_DATE                                          DATE,
    UPDATE_PROCESS_NAME                                           TEXT,
    UPDATE_PROCESS_ID                                             INTEGER,
    PROCESS_NAME                                                  TEXT,
    PROCESS_ID                                                    INTEGER,
    START_DATE                                                    DATE,
    END_DATE                                                      DATE,
    RECORD_DELETED_FLAG                                           INTEGER
);
    """)

def get_component_info_from_db(component = None):
    type_filter = f"AND TYPE = '{component}'" if component else ""
    component_info_dict = {}
    component_list = fetch_queries_as_dictionaries(f"""
SELECT
    NAME AS COMPONENT_NAME
FROM
    SQLITE_MASTER
WHERE
    1 = 1
    {type_filter}
    AND NAME NOT LIKE 'sqlite_%'
ORDER BY 1;
    """)
    for component in component_list:
        component_column_data = fetch_queries_as_dictionaries(f"""
PRAGMA 
    TABLE_INFO({component['COMPONENT_NAME']});
    """)
        component_info_dict[component['COMPONENT_NAME']] = component_column_data
    return component_info_dict

def get_missing_prices_from_price_table():
    missing_prices_data = fetch_queries_as_dictionaries("""
SELECT
    META.ALT_SYMBOL
    ,HC.PROCESSING_DATE AS VALUE_DATE
    ,PR.PRICE
    ,META.PORTFOLIO_TYPE
FROM
    METADATA_STORE META
LEFT OUTER JOIN
    HOLIDAY_CALENDAR HC
ON
    HC.PROCESSING_DATE        <= CURRENT_DATE
    AND HC.PROCESSING_DATE    >= META.LAUNCHED_ON
    AND HC.PROCESSING_DATE    >= STRFTIME('%Y-01-01')
    AND HC.RECORD_DELETED_FLAG = 0
LEFT OUTER JOIN
    PRICE_TABLE PR
ON
    PR.ALT_SYMBOL              = META.ALT_SYMBOL
    AND PR.VALUE_DATE          = HC.PROCESSING_DATE
    AND PR.RECORD_DELETED_FLAG = 0
WHERE
    PR.PRICE IS NULL
    AND HC.PROCESSING_DATE NOT IN (SELECT DISTINCT WORKING_DATE FROM WORKING_DATES)
GROUP BY 1,2,3,4
ORDER BY 2;
    """)
    return missing_prices_data