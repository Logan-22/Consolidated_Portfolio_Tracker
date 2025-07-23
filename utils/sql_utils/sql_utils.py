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
from utils.sql_utils.views.AGG_CONSOLIDATED_ALLOCATION_VIEW import AGG_CONSOLIDATED_ALLOCATION_VIEW
from utils.sql_utils.views.FIN_CONSOLIDATED_ALLOCATION_VIEW import FIN_CONSOLIDATED_ALLOCATION_VIEW
from utils.sql_utils.views.FIN_CONSOLIDATED_ALLOCATION_PORTFOLIO_VIEW import FIN_CONSOLIDATED_ALLOCATION_PORTFOLIO_VIEW
from utils.sql_utils.views.SIMULATED_PORTFOLIO_VIEW import SIMULATED_PORTFOLIO_VIEW
from utils.sql_utils.views.AGG_SIMULATED_PORTFOLIO_VIEW import AGG_SIMULATED_PORTFOLIO_VIEW
from utils.sql_utils.views.FIN_SIMULATED_PORTFOLIO_VIEW import FIN_SIMULATED_PORTFOLIO_VIEW

def create_price_table():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('''
            CREATE TABLE IF NOT EXISTS PRICE_TABLE 
            (
            ID INTEGER PRIMARY KEY AUTOINCREMENT,
            ALT_SYMBOL TEXT NOT NULL,
            PORTFOLIO_TYPE TEXT NOT NULL,
            VALUE_DATE DATE NOT NULL,
            VALUE_TIME TIME,
            PRICE NUMERIC NOT NULL,
            PRICE_TYP_CD TEXT NOT NULL,
            START_DATE DATE,
            END_DATE DATE,
            RECORD_DELETED_FLAG INTEGER DEFAULT 0
            );
    ''')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_alt_symbol ON PRICE_TABLE (ALT_SYMBOL);')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_symbol_date_type ON PRICE_TABLE (ALT_SYMBOL, VALUE_DATE, PRICE_TYP_CD);')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_portfolio_date ON PRICE_TABLE (PORTFOLIO_TYPE, VALUE_DATE);')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_not_deleted ON PRICE_TABLE (RECORD_DELETED_FLAG);')

def upsert_into_price_table(alt_symbol, portfolio_type, value_date, value_time, price, price_typ_cd, start_date, end_date, record_deleted_flag):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(f"SELECT ALT_SYMBOL, PORTFOLIO_TYPE, VALUE_DATE, VALUE_TIME, PRICE, PRICE_TYP_CD FROM PRICE_TABLE WHERE ALT_SYMBOL = '{alt_symbol}' AND VALUE_DATE = '{value_date}' AND VALUE_TIME = '{value_time}' AND RECORD_DELETED_FLAG = 0;")
    rows = cursor.fetchall()
    if rows:
        for row in rows:
            alt_symbol_from_query     = row[0]
            portfolio_type_from_query = row[1]
            value_date_from_query     = row[2]
            value_time_from_query     = row[3]
            price_from_query          = row[4]
            price_typ_cd_from_query   = row[5]
            if alt_symbol_from_query == alt_symbol and portfolio_type_from_query == portfolio_type and value_date_from_query == value_date and value_time_from_query == value_time and price_typ_cd_from_query == price_typ_cd and price_from_query != price:
                cursor.execute(f"UPDATE PRICE_TABLE SET END_DATE = '{start_date}', RECORD_DELETED_FLAG = 1 WHERE ALT_SYMBOL = '{alt_symbol}' AND PORTFOLIO_TYPE = '{portfolio_type}' AND VALUE_DATE = '{value_date}' AND VALUE_TIME = '{value_time}' AND PRICE_TYP_CD = '{price_typ_cd}';")
                cursor.execute("INSERT INTO PRICE_TABLE (ALT_SYMBOL, PORTFOLIO_TYPE, VALUE_DATE, VALUE_TIME, PRICE, PRICE_TYP_CD, START_DATE, END_DATE, RECORD_DELETED_FLAG) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                                (alt_symbol, portfolio_type, value_date, value_time, price, price_typ_cd, start_date, end_date, record_deleted_flag))
    else:
        cursor.execute("INSERT INTO PRICE_TABLE (ALT_SYMBOL, PORTFOLIO_TYPE, VALUE_DATE, VALUE_TIME, PRICE, PRICE_TYP_CD, START_DATE, END_DATE, RECORD_DELETED_FLAG) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                      (alt_symbol, portfolio_type, value_date, value_time, price, price_typ_cd, start_date, end_date, record_deleted_flag))
    conn.commit()
    conn.close()

def delete_alt_symbol_from_price_table(alt_symbol):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(f"DELETE FROM PRICE_TABLE WHERE ALT_SYMBOL = '{alt_symbol}';")

def create_metadata_store_table():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS METADATA_STORE (
            ID INTEGER PRIMARY KEY AUTOINCREMENT,
            EXCHANGE_SYMBOL TEXT(100),
            YAHOO_SYMBOL TEXT(100),
            ALT_SYMBOL TEXT(100),
            ALLOCATION_CATEGORY TEXT(100),
            PORTFOLIO_TYPE TEXT(50),
            AMC TEXT(50),
            MF_TYPE TEXT(100),
            FUND_CATEGORY TEXT(100),
            LAUNCHED_ON DATE,
            EXIT_LOAD NUMERIC(2,2),
            EXPENSE_RATIO NUMERIC(2,2),
            FUND_MANAGER TEXT(100),
            FUND_MANAGER_STARTED_ON DATE,
            ISIN TEXT(100),
            PROCESS_FLAG INTEGER,
            CONSIDER_FOR_HIST_RETURNS INTEGER,
            START_DATE DATE,
            END_DATE DATE,
            RECORD_DELETED_FLAG INTEGER
        )''')

def create_metadata_key_columns_table():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS METADATA_KEY_COLUMNS (
            ID INTEGER PRIMARY KEY AUTOINCREMENT,
            PROCESS_NAME TEXT(100),
            KEYCOLUMN_NAME TEXT(100),
            CONSIDER_FOR_PROCESSING INTEGER
        );''')

def insert_metadata_store_entry(exchange_symbol, yahoo_symbol, alt_symbol, allocation_category, portfolio_type, amc, mf_type, fund_category, launched_on, exit_load, expense_ratio, fund_manager, fund_manager_started_on, isin):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    today = datetime.date.today()
    today = today.strftime("%Y-%m-%d")
    cursor.execute("INSERT INTO METADATA_STORE (EXCHANGE_SYMBOL, YAHOO_SYMBOL, ALT_SYMBOL, ALLOCATION_CATEGORY, PORTFOLIO_TYPE, AMC, MF_TYPE, FUND_CATEGORY, LAUNCHED_ON, EXIT_LOAD, EXPENSE_RATIO, FUND_MANAGER, FUND_MANAGER_STARTED_ON, ISIN, PROCESS_FLAG, CONSIDER_FOR_HIST_RETURNS, START_DATE, END_DATE, RECORD_DELETED_FLAG) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                   (exchange_symbol, yahoo_symbol, alt_symbol, allocation_category, portfolio_type, amc, mf_type, fund_category, launched_on, exit_load, expense_ratio, fund_manager, fund_manager_started_on, isin, 1, 1, today, '9998-12-31', 0  ))
    conn.commit()
    conn.close()

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
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    if portfolio_type:
        portfolio_type = portfolio_type.replace("+", " ")
        cursor.execute(f"SELECT DISTINCT EXCHANGE_SYMBOL, YAHOO_SYMBOL, ALT_SYMBOL, PORTFOLIO_TYPE FROM METADATA_STORE WHERE PORTFOLIO_TYPE = '{portfolio_type}' ORDER BY EXCHANGE_SYMBOL;")
    else:
        cursor.execute("SELECT DISTINCT EXCHANGE_SYMBOL, YAHOO_SYMBOL, ALT_SYMBOL, PORTFOLIO_TYPE FROM METADATA_STORE ORDER BY EXCHANGE_SYMBOL;")
    rows = cursor.fetchall()
    conn.close()
    if rows:
        all_symbols_list = [{'exchange_symbol': row[0], 'yahoo_symbol': row[1], 'alt_symbol': row[2], 'portfolio_type': row[3]} for row in rows]
        return jsonify(all_symbols_list)
    all_symbols_list = [{'exchange_symbol': None, 'yahoo_symbol': None, 'alt_symbol': None, 'portfolio_type': None}]
    return jsonify(all_symbols_list)

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
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    if alt_symbol and purchase_date:
        cursor.execute(f"SELECT DISTINCT PRICE, ALT_SYMBOL, VALUE_DATE FROM PRICE_TABLE WHERE ALT_SYMBOL = '{alt_symbol}' AND VALUE_DATE = '{purchase_date}' AND PRICE_TYP_CD = 'CLOSE_PRICE' AND RECORD_DELETED_FLAG = 0 ORDER BY ALT_SYMBOL, VALUE_DATE")
    elif alt_symbol and not purchase_date:
        cursor.execute(f"SELECT DISTINCT PRICE, ALT_SYMBOL, VALUE_DATE FROM PRICE_TABLE WHERE ALT_SYMBOL = '{alt_symbol}' ANDPRICE_TYP_CD = 'CLOSE_PRICE' AND RECORD_DELETED_FLAG = 0 ORDER BY ALT_SYMBOL, VALUE_DATE")
    elif not alt_symbol and not purchase_date:
        cursor.execute(f"SELECT DISTINCT PRICE, ALT_SYMBOL, VALUE_DATE FROM PRICE_TABLE WHERE PRICE_TYP_CD = 'CLOSE_PRICE' AND RECORD_DELETED_FLAG = 0 ORDER BY ALT_SYMBOL, VALUE_DATE")
    rows = cursor.fetchall()
    conn.close()
    if rows:
        price_data = [{'price' : row[0], 'alt_symbol': row[1], 'value_date': row[2]} for row in rows]
        return jsonify(price_data)
    price_data = [{'price' : None, 'alt_symbol': None, 'value_date': None}]
    return jsonify(price_data)
    
def create_mf_order_table():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS MF_ORDER (
            ID INTEGER PRIMARY KEY AUTOINCREMENT,
            NAME TEXT(200),
            PURCHASED_ON DATE,
            INVESTED_AMOUNT NUMERIC(10,4),
            STAMP_FEES_AMOUNT NUMERIC(10,4),
            AMC_AMOUNT NUMERIC(10,4),
            NAV_DURING_PURCHASE NUMERIC(10,4),
            UNITS NUMERIC(10,4),
            START_DATE DATE,
            END_DATE DATE,
            RECORD_DELETED_FLAG INTEGER
        )''')
    
def insert_mf_order_entry(alt_symbol, purchase_date, invested_amount, stamp_fees_amount, amc_amount, nav_during_purchase, units):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("INSERT INTO MF_ORDER (NAME, PURCHASED_ON, INVESTED_AMOUNT, STAMP_FEES_AMOUNT, AMC_AMOUNT, NAV_DURING_PURCHASE, UNITS, START_DATE, END_DATE, RECORD_DELETED_FLAG) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                   (alt_symbol, purchase_date, invested_amount, stamp_fees_amount, amc_amount, nav_during_purchase, units, purchase_date, '9998-12-31', 0  ))
    conn.commit()
    conn.close()

def get_proc_date_from_processing_date_table():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    proc_dates = {}
    cursor.execute(f"SELECT DISTINCT PROC_TYP_CD, PROC_DATE, NEXT_PROC_DATE, PREV_PROC_DATE FROM PROCESSING_DATE;")
    rows = cursor.fetchall()
    conn.close()
    if rows:
        for row in rows:
            proc_dates[row[0]] = row
    return jsonify(proc_dates)

def create_processing_date_table():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS PROCESSING_DATE (
            ID INTEGER PRIMARY KEY AUTOINCREMENT,
            PROC_TYP_CD VARCHAR(100),
            PROC_DATE DATE,
            NEXT_PROC_DATE DATE,
            PREV_PROC_DATE DATE
        )''')
    cursor.execute(f"SELECT COUNT(*) FROM PROCESSING_DATE;")
    rows = cursor.fetchall()
    if rows:
        count = rows[0][0]
    if count == 0:
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
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS PROCESSING_TYPE (
            ID INTEGER PRIMARY KEY AUTOINCREMENT,
            PROC_TYP_CD VARCHAR(100),
            PROC_TYPE VARCHAR(100),
            PROC_DESCRIPTION VARCHAR(100)
        )''')
    cursor.execute(f"SELECT COUNT(*) FROM PROCESSING_TYPE;")
    rows = cursor.fetchall()
    if rows:
        count = rows[0][0]
    if count == 0:
        cursor.execute("INSERT INTO PROCESSING_TYPE (PROC_TYP_CD, PROC_TYPE, PROC_DESCRIPTION) VALUES (?, ?, ?)",
                      ('SIMULATED_RETURNS', 'nifty_50', 'NIFTY 50'))
        conn.commit()
    conn.close()

def create_metadata_process_table():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS METADATA_PROCESS (
            ID INTEGER PRIMARY KEY AUTOINCREMENT,
            PROCESS_NAME TEXT,
            PROCESS_TYPE TEXT,
            PROC_TYP_CD_LIST TEXT,
            INPUT_VIEW TEXT,
            TARGET_TABLE TEXT,
            PROCESS_DESCRIPTION TEXT,
            AUTO_TRIGGER_ON_LAUNCH INTEGER,
            PROCESS_DECOMMISSIONED INTEGER,
            FREQUENCY TEXT,
            DEFAULT_START_DATE_TYPE_CD TEXT
        );''')
    conn.close()

def create_metadata_process_group_table():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS METADATA_PROCESS_GROUP (
            ID INTEGER PRIMARY KEY AUTOINCREMENT,
            PROCESS_GROUP TEXT,
            PROCESS_NAME TEXT,
            CONSIDER_FOR_PROCESS INTEGER
        );''')
    conn.close()

def create_execution_logs_table():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS EXECUTION_LOGS (
            ID INTEGER PRIMARY KEY AUTOINCREMENT,
            PROCESS_NAME TEXT,
            PROCESS_ID INTEGER,
            STATUS TEXT,
            LOG TEXT,
            PROCESSING_START_DATE DATE,
            PROCESSING_END_DATE DATE,
            PAYLOAD_COUNT INTEGER,
            INSERTED_COUNT INTEGER,
            UPDATED_COUNT INTEGER,
            NO_CHANGE_COUNT INTEGER,
            SKIPPED_COUNT INTEGER,
            NULL_COUNT INTEGER,
            SKIPPED_DUE_TO_SCHEMA TEXT,
            START_TS TEXT,
            END_TS TEXT
        );''')
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
    
def get_max_value_date_for_alt_symbol(process_flag = None, consider_for_hist_returns = None, portfolio_type = None):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    if consider_for_hist_returns and portfolio_type:
        cursor.execute(f"SELECT MS.ALT_SYMBOL, MS.EXCHANGE_SYMBOL, MS.YAHOO_SYMBOL, MS.PORTFOLIO_TYPE, MAX(PT.VALUE_DATE)  FROM METADATA_STORE MS LEFT OUTER JOIN PRICE_TABLE PT ON MS.ALT_SYMBOL = PT.ALT_SYMBOL WHERE MS.CONSIDER_FOR_HIST_RETURNS = {consider_for_hist_returns} AND MS.PORTFOLIO_TYPE = '{portfolio_type}' GROUP BY 1,2,3,4;")
    elif process_flag:
        cursor.execute(f"SELECT MS.ALT_SYMBOL, MS.EXCHANGE_SYMBOL, MS.YAHOO_SYMBOL, MS.PORTFOLIO_TYPE, MAX(PT.VALUE_DATE)  FROM METADATA_STORE MS LEFT OUTER JOIN PRICE_TABLE PT ON MS.ALT_SYMBOL = PT.ALT_SYMBOL WHERE MS.PROCESS_FLAG = {process_flag} GROUP BY 1,2,3,4;")
    elif consider_for_hist_returns:
        cursor.execute(f"SELECT MS.ALT_SYMBOL, MS.EXCHANGE_SYMBOL, MS.YAHOO_SYMBOL, MS.PORTFOLIO_TYPE, MAX(PT.VALUE_DATE)  FROM METADATA_STORE MS LEFT OUTER JOIN PRICE_TABLE PT ON MS.ALT_SYMBOL = PT.ALT_SYMBOL WHERE MS.CONSIDER_FOR_HIST_RETURNS = {consider_for_hist_returns} GROUP BY 1,2,3,4;")
    elif portfolio_type:
        cursor.execute(f"SELECT MS.ALT_SYMBOL, MS.EXCHANGE_SYMBOL, MS.YAHOO_SYMBOL, MS.PORTFOLIO_TYPE, MAX(PT.VALUE_DATE)  FROM METADATA_STORE MS LEFT OUTER JOIN PRICE_TABLE PT ON MS.ALT_SYMBOL = PT.ALT_SYMBOL WHERE MS.PORTFOLIO_TYPE = '{portfolio_type}' GROUP BY 1,2,3,4;")
    else:
        cursor.execute("SELECT MS.ALT_SYMBOL, MS.EXCHANGE_SYMBOL, MS.YAHOO_SYMBOL, MS.PORTFOLIO_TYPE, MAX(PT.VALUE_DATE)  FROM METADATA_STORE MS LEFT OUTER JOIN PRICE_TABLE PT ON MS.ALT_SYMBOL = PT.ALT_SYMBOL GROUP BY 1,2,3,4;")
    rows = cursor.fetchall()
    conn.close()
    if rows:
        max_value_date_data = [{'alt_symbol' : row[0],'exchange_symbol': row[1], 'yahoo_symbol': row[2], 'portfolio_type': row[3], 'max_value_date': row[4] or None } for row in rows]
        return jsonify(max_value_date_data)
    return [{'alt_symbol' : None,'exchange_symbol': None, 'yahoo_symbol': None, 'portfolio_type': None, 'max_value_date': None }]

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

    cursor.execute("DROP VIEW IF EXISTS AGG_CONSOLIDATED_ALLOCATION_VIEW;")
    cursor.execute(AGG_CONSOLIDATED_ALLOCATION_VIEW)
    cursor.execute("DROP VIEW IF EXISTS FIN_CONSOLIDATED_ALLOCATION_VIEW;")
    cursor.execute(FIN_CONSOLIDATED_ALLOCATION_VIEW)
    cursor.execute("DROP VIEW IF EXISTS FIN_CONSOLIDATED_ALLOCATION_PORTFOLIO_VIEW;")
    cursor.execute(FIN_CONSOLIDATED_ALLOCATION_PORTFOLIO_VIEW)

    conn.commit()
    conn.close()

def create_holiday_date_table():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS HOLIDAY_DATES (
            ID INTEGER PRIMARY KEY AUTOINCREMENT,
            HOLIDAY_DATE DATE,
            HOLIDAY_NAME VARCHAR(200),
            HOLIDAY_DAY VARCHAR(20),
            START_DATE DATE,
            END_DATE DATE,
            RECORD_DELETED_FLAG INTEGER
        )''')
    
def insert_into_holiday_dates_table(holiday_date, holiday_name, holiday_day):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("INSERT INTO HOLIDAY_DATES (HOLIDAY_DATE, HOLIDAY_NAME, HOLIDAY_DAY,  START_DATE, END_DATE, RECORD_DELETED_FLAG) VALUES (?, ?, ?, ?, ?, ?)",
                   (holiday_date, holiday_name, holiday_day, holiday_date, '9998-12-31', 0))
    conn.commit()
    conn.close()

def get_holiday_date_from_holiday_dates_table(current_year = '1900'):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    if current_year:
        cursor.execute(f"SELECT HOLIDAY_DATE, HOLIDAY_NAME, HOLIDAY_DAY FROM HOLIDAY_DATES WHERE RECORD_DELETED_FLAG = 0 AND START_DATE >= '{current_year}-01-01' ORDER BY HOLIDAY_DATE;")
    else:
        cursor.execute(f"SELECT HOLIDAY_DATE, HOLIDAY_NAME, HOLIDAY_DAY FROM HOLIDAY_DATES WHERE RECORD_DELETED_FLAG = 0 ORDER BY HOLIDAY_DATE;")
    rows = cursor.fetchall()
    conn.close()
    if rows:
        data = [{'holiday_date': row[0], 'holiday_name': row[1], 'holiday_day': row[2]} for row in rows]
        return jsonify(data)
    data = [{'holiday_date': None, 'holiday_name': None, 'holiday_day': None}]
    return jsonify(data)

def truncate_holiday_calendar_table():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('DROP TABLE IF EXISTS HOLIDAY_CALENDAR')

def create_holiday_calendar_table():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS HOLIDAY_CALENDAR (
            ID INTEGER PRIMARY KEY AUTOINCREMENT,
            PROCESSING_DATE DATE,
            PROCESSING_DAY VARCHAR(15),
            NEXT_PROCESSING_DATE DATE,
            NEXT_PROCESSING_DAY VARCHAR(15),
            PREV_PROCESSING_DATE DATE,
            PREV_PROCESSING_DAY VARCHAR(15),
            START_DATE DATE,
            END_DATE DATE,
            RECORD_DELETED_FLAG INTEGER
        )''')
    
def insert_into_holiday_calendar_table(counter_date, counter_day, next_counter_date, next_counter_day, prev_counter_date, prev_counter_day):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("INSERT INTO HOLIDAY_CALENDAR (PROCESSING_DATE, PROCESSING_DAY, NEXT_PROCESSING_DATE, NEXT_PROCESSING_DAY, PREV_PROCESSING_DATE, PREV_PROCESSING_DAY, START_DATE, END_DATE, RECORD_DELETED_FLAG) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                   (counter_date, counter_day, next_counter_date, next_counter_day, prev_counter_date, prev_counter_day, counter_date, '9998-12-31', 0))
    conn.commit()
    conn.close()

def create_working_date_table():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS WORKING_DATES (
            ID INTEGER PRIMARY KEY AUTOINCREMENT,
            WORKING_DATE DATE,
            WORKING_DAY_NAME VARCHAR(200),
            WORKING_DAY VARCHAR(20),
            START_DATE DATE,
            END_DATE DATE,
            RECORD_DELETED_FLAG INTEGER
        )''')
    
def insert_into_working_date_table(working_date, working_day_name, working_day):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("INSERT INTO WORKING_DATES (WORKING_DATE, WORKING_DAY_NAME, WORKING_DAY,  START_DATE, END_DATE, RECORD_DELETED_FLAG) VALUES (?, ?, ?, ?, ?, ?)",
                   (working_date, working_day_name, working_day, working_date, '9998-12-31', 0))
    conn.commit()
    conn.close()

def get_working_date_from_working_dates_table(current_year = '1900'):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    if current_year:
        cursor.execute(f"SELECT WORKING_DATE, WORKING_DAY_NAME, WORKING_DAY FROM WORKING_DATES WHERE RECORD_DELETED_FLAG = 0 AND START_DATE >= '{current_year}-01-01' ORDER BY WORKING_DATE;")
    else:
        cursor.execute(f"SELECT WORKING_DATE, WORKING_DAY_NAME, WORKING_DAY FROM WORKING_DATES WHERE RECORD_DELETED_FLAG = 0 ORDER BY WORKING_DATE;")
    rows = cursor.fetchall()
    conn.close()
    if rows:
        data = [{'working_date': row[0], 'working_day_name': row[1], 'working_day': row[2]} for row in rows]
        return jsonify(data)
    data = [{'working_date': None, 'working_day_name': None, 'working_day': None}]
    return jsonify(data)

def truncate_mf_hist_returns_table():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('DROP TABLE IF EXISTS MF_HIST_RETURNS')

def create_mf_hist_returns_table():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS MF_HIST_RETURNS (
            ID INTEGER PRIMARY KEY AUTOINCREMENT,
            PROCESSING_DATE DATE,
            PREV_PROCESSING_DATE DATE,
            AMOUNT_INVESTED_AS_ON_PROCESSING_DATE NUMBER(10,4),
            AMOUNT_AS_ON_PROCESSING_DATE NUMBER(10,4),
            AMOUNT_AS_ON_PREV_PROCESSING_DATE NUMBER(10,4),
            "TOTAL_P/L" NUMBER(10,4),
            "DAY_P/L" NUMBER(10,4),
            "%TOTAL_P/L" NUMBER(10,4),
            "%DAY_P/L" NUMBER(10,4),
            NEXT_PROCESSING_DATE DATE,
            START_DATE DATE,
            END_DATE DATE,
            RECORD_DELETED_FLAG INTEGER
        )''')

def get_first_purchase_date_from_mf_order_date_table():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(f"SELECT MIN(PURCHASED_ON) FROM MF_ORDER WHERE RECORD_DELETED_FLAG = 0;")
    rows = cursor.fetchall()
    conn.close()
    if rows:
        data = {'first_purchase_date': rows[0][0]}
        return jsonify(data)
    return

def get_date_setup_from_holiday_calendar(counter_date):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(f"SELECT DISTINCT PROCESSING_DATE, NEXT_PROCESSING_DATE, PREV_PROCESSING_DATE FROM HOLIDAY_CALENDAR WHERE PROCESSING_DATE = '{counter_date}';")
    rows = cursor.fetchall()
    conn.close()
    if rows:
        data = [{'processing_date': row[0], 'next_processing_date': row[1], 'prev_processing_date': row[2]} for row in rows]
        return jsonify(data)
    return

def get_metrics_from_fin_mutual_fund_portfolio_view():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(f'SELECT "FIN_P/L", "FIN_AMC_AMOUNT", "FIN_CURRENT_AMOUNT", "FIN_PREVIOUS_AMOUNT", "FIN_%P/L", "FIN_DAY_P/L", "FIN_%DAY_P/L" FROM FIN_MUTUAL_FUND_PORTFOLIO_VIEW;')
    rows = cursor.fetchall()
    conn.close()
    if rows:
        data = [{'total_p_l': row[0], 'amount_invested_as_on_processing_date': row[1], 'amount_as_on_processing_date': row[2], 'amount_as_on_prev_processing_date': row[3], 'perc_total_p_l': row[4], 'day_p_l': row[5], 'perc_day_p_l': row[6]} for row in rows]
        return jsonify(data)
    return

def insert_into_mf_hist_returns(processing_date, next_processing_date, prev_processing_date, total_p_l, amount_invested_as_on_processing_date, amount_as_on_processing_date, amount_as_on_prev_processing_date, perc_total_p_l, day_p_l, perc_day_p_l):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('INSERT INTO MF_HIST_RETURNS (PROCESSING_DATE, PREV_PROCESSING_DATE, AMOUNT_INVESTED_AS_ON_PROCESSING_DATE, AMOUNT_AS_ON_PROCESSING_DATE, AMOUNT_AS_ON_PREV_PROCESSING_DATE, "TOTAL_P/L", "DAY_P/L", "%TOTAL_P/L", "%DAY_P/L", NEXT_PROCESSING_DATE, START_DATE, END_DATE, RECORD_DELETED_FLAG) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
                   (processing_date, prev_processing_date, amount_invested_as_on_processing_date, amount_as_on_processing_date, amount_as_on_prev_processing_date, total_p_l, day_p_l, perc_total_p_l, perc_day_p_l, next_processing_date, processing_date, '9998-12-31', 0))
    conn.commit()
    conn.close()

def get_mf_returns():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(f'SELECT PROCESSING_DATE, "%TOTAL_P/L", "%DAY_P/L" FROM MF_HIST_RETURNS WHERE RECORD_DELETED_FLAG = 0 ORDER BY PROCESSING_DATE;')
    rows = cursor.fetchall()
    conn.close()
    if rows:
        data = [{'processing_date': row[0], 'perc_total_p_l': row[1], 'perc_day_p_l': row[2]} for row in rows]
        return jsonify(data)
    data = [{'processing_date': None, 'perc_total_p_l': None, 'perc_day_p_l': None}]
    return jsonify(data)

def get_max_next_proc_date_from_mf_hist_returns_table():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(f'SELECT MAX(NEXT_PROCESSING_DATE) FROM MF_HIST_RETURNS WHERE RECORD_DELETED_FLAG = 0;')
    rows = cursor.fetchall()
    conn.close()
    if rows:
        data = {'max_next_processing_date': rows[0][0]}
        return jsonify(data)
    data = {'max_next_processing_date': None}
    return jsonify(data)

def create_trade_table():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS TRADES (
            TRADE_ID VARCHAR(200),
            FEE_ID VARCHAR(200),
            TRADE_SET_ID VARCHAR(200),
            STOCK_NAME VARCHAR(200),
            STOCK_ISIN VARCHAR(100),
            TRADE_DATE DATE,
            ORDER_NUMBER VARCHAR(100),
            ORDER_TIME TIME,
            TRADE_NUMBER VARCHAR(100),
            TRADE_TIME TIME,
            BUY_OR_SELL CHAR(1),
            STOCK_QUANTITY INTEGER,
            BROKERAGE_PER_TRADE NUMERIC(10,4),
            NET_TRADE_PRICE_PER_UNIT NUMERIC(10,4),
            NET_TOTAL_BEFORE_LEVIES NUMERIC(10,4),
            TRADE_SET INTEGER,
            TRADE_POSITION VARCHAR(10),
            TRADE_ENTRY_DATE DATE,
            TRADE_ENTRY_TIME TIME,
            TRADE_EXIT_DATE DATE,
            TRADE_EXIT_TIME TIME,
            TRADE_TYPE TEXT(20),
            LEVERAGE INTEGER,
            CLOSING_TRADE_ID VARCHAR(200),
            START_DATE DATE,
            END_DATE DATE,
            RECORD_DELETED_FLAG INTEGER
        )''')
    
def create_fee_table():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS FEE_COMPONENT (
            FEE_ID VARCHAR(200),
            TRADE_DATE DATE,
            NET_OBLIGATION NUMERIC(10,4),
            BROKERAGE NUMERIC(10,4),
            EXCHANGE_TRANSACTION_CHARGES NUMERIC(10,4),
            IGST NUMERIC(10,4),
            SECURITIES_TRANSACTION_TAX NUMERIC(10,4),
            SEBI_TURN_OVER_FEES NUMERIC(10,4),
            AUTO_SQUARE_OFF_CHARGES NUMERIC(10,4),
            DEPOSITORY_CHARGES NUMERIC(10,4),
            START_DATE DATE,
            END_DATE DATE,
            RECORD_DELETED_FLAG INTEGER
        )''')
    

def upsert_trade_entry_in_db(trade_id, fee_id, trade_set_id, stock_symbol, stock_isin, trade_date, order_number, order_time, trade_number, trade_time, buy_or_sell, stock_quantity, brokerage_per_trade, net_trade_price_per_unit, net_total_before_levies, trade_set, trade_position, trade_entry_date, trade_entry_time, trade_exit_date, trade_exit_time, trade_type, leverage):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    today = datetime.date.today()
    today = today.strftime("%Y-%m-%d")
    cursor.execute(f"""SELECT STOCK_NAME, STOCK_ISIN, TRADE_DATE, ORDER_NUMBER, ORDER_TIME, TRADE_NUMBER, TRADE_TIME, BUY_OR_SELL,
       STOCK_QUANTITY, BROKERAGE_PER_TRADE, NET_TRADE_PRICE_PER_UNIT, NET_TOTAL_BEFORE_LEVIES, TRADE_SET,
       TRADE_POSITION, TRADE_ENTRY_DATE, TRADE_ENTRY_TIME, TRADE_EXIT_DATE, TRADE_EXIT_TIME, TRADE_TYPE, LEVERAGE
       FROM TRADES WHERE TRADE_ID = '{trade_id}' AND RECORD_DELETED_FLAG = 0;""")
    rows = cursor.fetchall()
    if rows:
        for row in rows:
            stock_name_from_query               = row[0]
            stock_isin_from_query               = row[1]
            trade_date_from_query               = row[2]
            order_number_from_query             = row[3]
            order_time_from_query               = row[4]
            trade_number_from_query             = row[5]
            trade_time_from_query               = row[6]
            buy_or_sell_from_query              = row[7]
            stock_quantity_from_query           = row[8]
            brokerage_per_trade_from_query      = row[9]
            net_trade_price_per_unit_from_query = row[10]
            net_total_before_levies_from_query  = row[11]
            trade_set_from_query                = row[12]
            trade_position_from_query           = row[13]
            trade_entry_date_from_query         = row[14]
            trade_entry_time_from_query         = row[15]
            trade_exit_date_from_query          = row[16]
            trade_exit_time_from_query          = row[17]
            trade_type_from_query               = row[18]
            leverage_from_query                 = row[19]
            if (stock_name_from_query != stock_symbol or stock_isin_from_query != stock_isin or trade_date_from_query != trade_date or order_number_from_query != order_number or order_time_from_query != order_time or trade_number_from_query != trade_number or trade_time_from_query != trade_time or buy_or_sell_from_query != buy_or_sell or stock_quantity_from_query != stock_quantity or brokerage_per_trade_from_query != brokerage_per_trade or net_trade_price_per_unit_from_query != net_trade_price_per_unit or net_total_before_levies_from_query != net_total_before_levies or trade_set_from_query != trade_set or trade_position_from_query != trade_position or trade_entry_date_from_query != trade_entry_date or trade_entry_time_from_query != trade_entry_time or trade_exit_date_from_query != trade_exit_date or trade_exit_time_from_query != trade_exit_time or trade_type_from_query != trade_type or leverage_from_query != leverage):
                cursor.execute(f"UPDATE TRADES SET END_DATE = '{today}', RECORD_DELETED_FLAG = 1 WHERE TRADE_ID = '{trade_id}';")
                cursor.execute("""INSERT INTO  TRADES (TRADE_ID, FEE_ID, TRADE_SET_ID, STOCK_NAME, STOCK_ISIN, TRADE_DATE, ORDER_NUMBER, ORDER_TIME, TRADE_NUMBER, TRADE_TIME, BUY_OR_SELL,
                                  STOCK_QUANTITY, BROKERAGE_PER_TRADE, NET_TRADE_PRICE_PER_UNIT, NET_TOTAL_BEFORE_LEVIES, TRADE_SET,
                                  TRADE_POSITION, TRADE_ENTRY_DATE, TRADE_ENTRY_TIME, TRADE_EXIT_DATE, TRADE_EXIT_TIME, TRADE_TYPE, LEVERAGE, START_DATE, END_DATE, RECORD_DELETED_FLAG) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                                  (str(trade_id), str(fee_id), str(trade_set_id), stock_symbol, stock_isin, trade_date, order_number, order_time, trade_number, trade_time, buy_or_sell, stock_quantity, brokerage_per_trade, net_trade_price_per_unit, net_total_before_levies, trade_set, trade_position, trade_entry_date, trade_entry_time, trade_exit_date, trade_exit_time, trade_type, leverage, today, '9998-12-31', 0))
                conn.commit()
    else:
        cursor.execute("""INSERT INTO TRADES (TRADE_ID, FEE_ID, TRADE_SET_ID, STOCK_NAME, STOCK_ISIN, TRADE_DATE, ORDER_NUMBER, ORDER_TIME, TRADE_NUMBER, TRADE_TIME, BUY_OR_SELL,
                          STOCK_QUANTITY, BROKERAGE_PER_TRADE, NET_TRADE_PRICE_PER_UNIT, NET_TOTAL_BEFORE_LEVIES, TRADE_SET,
                          TRADE_POSITION, TRADE_ENTRY_DATE, TRADE_ENTRY_TIME, TRADE_EXIT_DATE, TRADE_EXIT_TIME, TRADE_TYPE, LEVERAGE, CLOSING_TRADE_ID, START_DATE, END_DATE, RECORD_DELETED_FLAG) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                          (str(trade_id), str(fee_id), str(trade_set_id), stock_symbol, stock_isin, trade_date, order_number, order_time, trade_number, trade_time, buy_or_sell, stock_quantity, brokerage_per_trade, net_trade_price_per_unit, net_total_before_levies, trade_set, trade_position, trade_entry_date, trade_entry_time, trade_exit_date, trade_exit_time, trade_type, leverage, None, trade_date, '9998-12-31', 0))
        conn.commit()
    conn.close()

def upsert_fee_entry_in_db(fee_id, trade_date, net_obligation, brokerage, exc_trans_charges, igst, sec_trans_tax, sebi_turn_fees, auto_square_off_charges, depository_charges):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    today = datetime.date.today()
    today = today.strftime("%Y-%m-%d")
    cursor.execute(f"""SELECT TRADE_DATE, NET_OBLIGATION, BROKERAGE, EXCHANGE_TRANSACTION_CHARGES, IGST, SECURITIES_TRANSACTION_TAX, SEBI_TURN_OVER_FEES, AUTO_SQUARE_OFF_CHARGES, DEPOSITORY_CHARGES
       FROM FEE_COMPONENT WHERE FEE_ID = '{fee_id}' AND RECORD_DELETED_FLAG = 0;""")
    rows = cursor.fetchall()
    if rows:
        for row in rows:
            trade_date_from_query                   = row[0]
            net_obligation_from_query               = row[1]
            brokerage_from_query                    = row[2]
            exchange_transaction_charges_from_query = row[3]
            igst_from_query                         = row[4]
            sec_trans_tax_from_query                = row[5]
            sebi_turn_fees_from_query               = row[6]
            auto_square_off_charges_from_query      = row[7]
            depository_charges_from_query           = row[8]

            if (trade_date_from_query != trade_date or net_obligation_from_query != float(net_obligation) or brokerage_from_query != float(brokerage) or exchange_transaction_charges_from_query != float(exc_trans_charges) or igst_from_query != float(igst) or sec_trans_tax_from_query != float(sec_trans_tax) or sebi_turn_fees_from_query != float(sebi_turn_fees) or auto_square_off_charges_from_query != float(auto_square_off_charges) or depository_charges_from_query != float(depository_charges)):
                cursor.execute(f"UPDATE FEE_COMPONENT SET END_DATE = '{today}', RECORD_DELETED_FLAG = 1 WHERE FEE_ID = '{fee_id}';")
                cursor.execute("INSERT INTO  FEE_COMPONENT (FEE_ID, TRADE_DATE, NET_OBLIGATION, BROKERAGE, EXCHANGE_TRANSACTION_CHARGES, IGST, SECURITIES_TRANSACTION_TAX, SEBI_TURN_OVER_FEES, AUTO_SQUARE_OFF_CHARGES, DEPOSITORY_CHARGES, START_DATE, END_DATE, RECORD_DELETED_FLAG) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                              (str(fee_id), trade_date, net_obligation, brokerage, exc_trans_charges, igst, sec_trans_tax, sebi_turn_fees, auto_square_off_charges, depository_charges, today, '9998-12-31', 0))
                conn.commit()
    else:
        cursor.execute("INSERT INTO FEE_COMPONENT (FEE_ID, TRADE_DATE, NET_OBLIGATION, BROKERAGE, EXCHANGE_TRANSACTION_CHARGES, IGST, SECURITIES_TRANSACTION_TAX, SEBI_TURN_OVER_FEES, AUTO_SQUARE_OFF_CHARGES, DEPOSITORY_CHARGES, START_DATE, END_DATE, RECORD_DELETED_FLAG) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                      (str(fee_id), trade_date, net_obligation, brokerage, exc_trans_charges, igst, sec_trans_tax, sebi_turn_fees, auto_square_off_charges, depository_charges, trade_date, '9998-12-31', 0))
        conn.commit()
    conn.close()

def truncate_realised_intraday_stock_hist_returns_table():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('DROP TABLE IF EXISTS REALISED_INTRADAY_STOCK_HIST_RETURNS')

def create_realised_intraday_stock_hist_returns_table():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS REALISED_INTRADAY_STOCK_HIST_RETURNS (
            ID INTEGER PRIMARY KEY AUTOINCREMENT,
            TRADE_DATE DATE,
            TRADE_TYPE VARCHAR(20),
            FEE_ID VARCHAR(100),
            PERCEIVED_DEPLOYED_CAPITAL NUMBER(10,4),
            ACTUAL_DEPLOYED_CAPITAL NUMBER(10,4),
            "TOTAL_P/L" NUMBER(10,4),
            "%_P/L_WITHOUT_LEVERAGE" NUMBER(10,2),
            "%_P/L_WITH_LEVERAGE" NUMBER(10,2),
            NET_OBLIGATION NUMBER(10,4),
            "TOTAL_P/L_VS_NET_OBLIGATION_MATCH_STATUS" VARCHAR(10),
            BROKERAGE NUMBER(10,4),
            EXCHANGE_TRANSACTION_CHARGES NUMBER(10,4),
            IGST NUMBER(10,4),
            SECURITIES_TRANSACTION_TAX NUMBER(10,4),
            SEBI_TURN_OVER_FEES NUMBER(10,4),
            TOTAL_FEES NUMBER(10,4),
            "NET_P/L" NUMBER(10,4),
            "NET_%_P/L_WITHOUT_LEVERAGE" NUMBER(10,2),
            "NET_%_P/L_WITH_LEVERAGE" NUMBER(10,2),
            TOTAL_CHARGES NUMBER(10,4),
            "NET_P/L_MINUS_CHARGES" NUMBER(10,4),
            "NET_%_P/L_WITHOUT_LEVERAGE_INCLUDING_CHARGES" NUMBER(10,2),
            "NET_%_P/L_WITH_LEVERAGE_INCLUDING_CHARGES" NUMBER(10,2),
            START_DATE DATE,
            END_DATE DATE,
            RECORD_DELETED_FLAG INTEGER
        )''')
    
def insert_into_realised_intraday_stock_hist_returns(start_date = '1900-01-01', end_date = '9998-12-31'):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(f'''INSERT INTO REALISED_INTRADAY_STOCK_HIST_RETURNS (TRADE_DATE, TRADE_TYPE, FEE_ID, PERCEIVED_DEPLOYED_CAPITAL, ACTUAL_DEPLOYED_CAPITAL, "TOTAL_P/L", "%_P/L_WITHOUT_LEVERAGE", "%_P/L_WITH_LEVERAGE", NET_OBLIGATION, "TOTAL_P/L_VS_NET_OBLIGATION_MATCH_STATUS", BROKERAGE, EXCHANGE_TRANSACTION_CHARGES, IGST, SECURITIES_TRANSACTION_TAX, SEBI_TURN_OVER_FEES, TOTAL_FEES, "NET_P/L", "NET_%_P/L_WITHOUT_LEVERAGE", "NET_%_P/L_WITH_LEVERAGE", TOTAL_CHARGES, "NET_P/L_MINUS_CHARGES", "NET_%_P/L_WITHOUT_LEVERAGE_INCLUDING_CHARGES", "NET_%_P/L_WITH_LEVERAGE_INCLUDING_CHARGES", START_DATE, END_DATE, RECORD_DELETED_FLAG) 
                   SELECT TRADE_DATE, TRADE_TYPE, FEE_ID, AGG_PERCEIVED_DEPLOYED_CAPITAL, AGG_ACTUAL_DEPLOYED_CAPITAL, "AGG_P/L", "%_P/L_WITHOUT_LEVERAGE", "%_P/L_WITH_LEVERAGE", NET_OBLIGATION, "AGG_P/L_NET_OBLIGATION_MATCH_STATUS", BROKERAGE, EXCHANGE_TRANSACTION_CHARGES, IGST, SECURITIES_TRANSACTION_TAX, SEBI_TURN_OVER_FEES, TOTAL_FEES, "NET_P/L", "NET_%_P/L_WITHOUT_LEVERAGE", "NET_%_P/L_WITH_LEVERAGE", TOTAL_CHARGES, "NET_P/L_MINUS_CHARGES", "NET_%_P/L_WITHOUT_LEVERAGE_INCL_CHARGES", "NET_%_P/L_WITH_LEVERAGE_INCL_CHARGES", TRADE_DATE, '9998-12-31', 0 FROM FIN_STOCK_INTRADAY_REALISED_PORTFOLIO_VIEW WHERE TRADE_DATE > '{start_date}' AND TRADE_DATE <= '{end_date}';''')
    conn.commit()
    conn.close()

def get_stock_hist_returns_from_realised_intraday_stock_and_swing_stock_hist_returns_table():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(f'''SELECT * FROM
(
SELECT
    RSHR.TRADE_DATE                    AS TRADE_DATE
    ,RSHR."%_P/L_WITH_LEVERAGE"        AS "NET_%_P/L"
FROM
    REALISED_INTRADAY_STOCK_HIST_RETURNS RSHR
WHERE
    RSHR.RECORD_DELETED_FLAG = 0 
UNION ALL
SELECT 
    RSSHR.TRADE_CLOSE_DATE             AS TRADE_DATE
    ,RSSHR."NET_%_P/L"                 AS "NET_%_P/L"
FROM
    REALISED_SWING_STOCK_HIST_RETURNS RSSHR
WHERE
    RECORD_DELETED_FLAG = 0
)
ORDER BY TRADE_DATE, "NET_%_P/L"
;''')
    rows = cursor.fetchall()
    conn.close()
    if rows:
        data = [{'trade_date': row[0], 'perc_p_l_with_leverage': row[1]} for row in rows]
        return jsonify(data)
    data = [{'trade_date': None, 'perc_p_l_with_leverage': None} for row in rows]
    return jsonify(data)

def get_max_trade_date_from_realised_intraday_stock_hist_returns_table():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(f'SELECT MAX(TRADE_DATE) FROM REALISED_INTRADAY_STOCK_HIST_RETURNS WHERE RECORD_DELETED_FLAG = 0;')
    rows = cursor.fetchall()
    conn.close()
    if rows:
        data = {'max_trade_date': rows[0][0]}
        return jsonify(data)
    data = {'max_trade_date': None}
    return jsonify(data)

def get_open_trades_from_trades_table():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(f"""SELECT TRADE_ID, STOCK_NAME, TRADE_DATE, STOCK_QUANTITY, BUY_OR_SELL FROM TRADES WHERE TRADE_EXIT_DATE IS NULL
                       AND TRADE_ID NOT IN(SELECT DISTINCT TRD.TRADE_ID FROM TRADES TRD INNER JOIN FIN_STOCK_SWING_REALISED_PORTFOLIO_VIEW FSSRPV ON TRD.FEE_ID = FSSRPV.OPENING_FEE_ID AND FSSRPV.TRADES_CLOSE_STATUS = 'TRADES_COMPLETELY_CLOSED')
                       AND TRADE_ID NOT IN(SELECT DISTINCT TRD.TRADE_ID FROM TRADES TRD INNER JOIN FIN_STOCK_SWING_REALISED_PORTFOLIO_VIEW FSSRPV ON TRD.FEE_ID = FSSRPV.CLOSING_FEE_ID AND FSSRPV.TRADES_CLOSE_STATUS = 'TRADES_COMPLETELY_CLOSED') ;""")
    rows = cursor.fetchall()
    conn.close()
    if rows:

        data = [{'trade_id': row[0], 'stock_name': row[1], 'trade_date': row[2], 'stock_quantity': row[3], 'buy_or_sell': row[4]} for row in rows]
        return jsonify(data)
    data = [{'trade_id': None, 'stock_name': None, 'trade_date': None, 'stock_quantity': None, 'buy_or_sell': None}]
    return jsonify(data)

def create_close_trades_table():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS CLOSE_TRADES (
            ID INTEGER PRIMARY KEY AUTOINCREMENT,
            STOCK_SYMBOL VARCHAR(100),
            OPENING_TRADE_ID VARCHAR(200),
            OPENING_TRADE_DATE DATE,
            OPENING_TRADE_STOCK_QUANTITY INTEGER,
            OPENING_TRADE_BUY_OR_SELL VARCHAR(10),
            CLOSING_TRADE_ID VARCHAR(200),
            CLOSING_TRADE_DATE DATE,
            CLOSING_TRADE_STOCK_QUANTITY INTEGER,
            CLOSING_TRADE_BUY_OR_SELL VARCHAR(10),
            START_DATE DATE,
            END_DATE DATE,
            RECORD_DELETED_FLAG INTEGER
        )''')
    
def insert_into_close_trades_table(opening_trade_id, opening_alt_symbol, opening_trade_date, opening_trade_stock_quantity, opening_trade_buy_or_sell, closing_trade_id, closing_alt_symbol, closing_trade_date, closing_trade_stock_quantity, closing_trade_buy_or_sell):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('INSERT INTO CLOSE_TRADES (STOCK_SYMBOL, OPENING_TRADE_ID, OPENING_TRADE_DATE, OPENING_TRADE_STOCK_QUANTITY, OPENING_TRADE_BUY_OR_SELL, CLOSING_TRADE_ID, CLOSING_TRADE_DATE, CLOSING_TRADE_STOCK_QUANTITY, CLOSING_TRADE_BUY_OR_SELL, START_DATE, END_DATE, RECORD_DELETED_FLAG) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
                   (opening_alt_symbol, opening_trade_id, opening_trade_date, opening_trade_stock_quantity, opening_trade_buy_or_sell, closing_trade_id, closing_trade_date, closing_trade_stock_quantity, closing_trade_buy_or_sell, closing_trade_date, '9998-12-31', 0  ))
    conn.commit()
    conn.close()

def truncate_realised_swing_stock_hist_returns_table():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('DROP TABLE IF EXISTS REALISED_SWING_STOCK_HIST_RETURNS')

def create_realised_swing_stock_hist_returns_table():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS REALISED_SWING_STOCK_HIST_RETURNS (
            ID INTEGER PRIMARY KEY AUTOINCREMENT,
            STOCK_NAME VARCHAR(20),
            OPENING_FEE_ID VARCHAR(200),
            CLOSING_FEE_ID VARCHAR(200),
            TRADE_OPEN_DATE DATE,
            TRADE_CLOSE_DATE DATE,
            OPENING_STOCK_QUANTITY INTEGER,
            CLOSING_STOCK_QUANTITY INTEGER,
            REMAINING_STOCK_BALANCE INTEGER,
            TRADES_CLOSE_STATUS VARCHAR(30),
            OPENING_BUY_PRICE NUMBER(10,4),
            OPEN_NET_OBLIGATION NUMBER(10,4),
            OPEN_MATCH_STATUS VARCHAR(30),
            OPEN_TOTAL_BROKERAGE NUMBER(10,4),
            OPEN_TOTAL_CHARGES NUMBER(10,4),
            OPEN_TOTAL_FEES NUMBER(10,4),
            CLOSING_SELL_PRICE NUMBER(10,4),
            CLOSE_NET_OBLIGATION NUMBER(10,4),
            CLOSE_MATCH_STATUS VARCHAR(30),
            CLOSE_TOTAL_BROKERAGE NUMBER(10,4),
            CLOSE_TOTAL_CHARGES NUMBER(10,4),
            CLOSE_TOTAL_FEES NUMBER(10,4),
            TOTAL_FEES NUMBER(10,4),
            "P/L" NUMBER(10,4),
            "%_P/L" NUMBER(10,2),
            "NET_P/L" NUMBER(10,4),
            "NET_%_P/L" NUMBER(10,2),
            START_DATE DATE,
            END_DATE DATE,
            RECORD_DELETED_FLAG INTEGER
        )''')
    
def insert_into_realised_swing_stock_hist_returns(start_date = '1900-01-01', end_date = '9998-12-31'):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(f'''INSERT INTO REALISED_SWING_STOCK_HIST_RETURNS (STOCK_NAME, OPENING_FEE_ID, CLOSING_FEE_ID, TRADE_OPEN_DATE, TRADE_CLOSE_DATE, OPENING_STOCK_QUANTITY, CLOSING_STOCK_QUANTITY, REMAINING_STOCK_BALANCE, TRADES_CLOSE_STATUS, OPENING_BUY_PRICE, OPEN_NET_OBLIGATION, OPEN_MATCH_STATUS, OPEN_TOTAL_BROKERAGE, OPEN_TOTAL_CHARGES, OPEN_TOTAL_FEES,
                       CLOSING_SELL_PRICE, CLOSE_NET_OBLIGATION, CLOSE_MATCH_STATUS, CLOSE_TOTAL_BROKERAGE, CLOSE_TOTAL_CHARGES, CLOSE_TOTAL_FEES, TOTAL_FEES, "P/L", "%_P/L", "NET_P/L", "NET_%_P/L", START_DATE, END_DATE, RECORD_DELETED_FLAG) 
                       SELECT STOCK_NAME, OPENING_FEE_ID, CLOSING_FEE_ID, TRADE_OPEN_DATE, TRADE_CLOSE_DATE, AGG_OPENING_STOCK_QUANTITY, AGG_CLOSING_STOCK_QUANTITY, REMAINING_STOCK_BALANCE, TRADES_CLOSE_STATUS, AGG_OPENING_BUY_PRICE, OPEN_NET_OBLIGATION, OPEN_MATCH_STATUS, OPEN_TOTAL_BROKERAGE, OPEN_TOTAL_CHARGES, OPEN_TOTAL_FEES,
                       AGG_CLOSING_SELL_PRICE, CLOSE_NET_OBLIGATION, CLOSE_MATCH_STATUS, CLOSE_TOTAL_BROKERAGE, CLOSE_TOTAL_CHARGES, CLOSE_TOTAL_FEES, TOTAL_FEES, "AGG_P/L", "AGG_%_P/L", "NET_P/L", "NET_%_P/L", TRADE_CLOSE_DATE, '9998-12-31', 0 FROM FIN_STOCK_SWING_REALISED_PORTFOLIO_VIEW WHERE TRADE_CLOSE_DATE > '{start_date}' AND TRADE_CLOSE_DATE <= '{end_date}';''')
    conn.commit()
    conn.close()

def get_max_trade_close_date_from_realised_swing_stock_hist_returns_table():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(f'SELECT MAX(TRADE_CLOSE_DATE) FROM REALISED_SWING_STOCK_HIST_RETURNS WHERE RECORD_DELETED_FLAG = 0;')
    rows = cursor.fetchall()
    conn.close()
    if rows:
        data = {'max_trade_close_date': rows[0][0]}
        return jsonify(data)
    data = {'max_trade_close_date': None}
    return jsonify(data)

def truncate_unrealised_swing_stock_hist_returns_table():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('DROP TABLE IF EXISTS UNREALISED_SWING_STOCK_HIST_RETURNS;')

def create_unrealised_swing_stock_hist_returns_table():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS UNREALISED_SWING_STOCK_HIST_RETURNS (
            ID INTEGER PRIMARY KEY AUTOINCREMENT,
            PROCESSING_DATE DATE,
            PREV_PROCESSING_DATE DATE,
            AMOUNT_INVESTED_AS_ON_PROCESSING_DATE NUMBER(10,4),
            TOTAL_FEES NUMBER(10,4),
            TOTAL_AMOUNT_INVESTED_AS_ON_PROCESSING_DATE NUMBER(10,4),
            CURRENT_VALUE NUMBER(10,4),
            PREVIOUS_VALUE NUMBER(10,4),
            "P/L" NUMBER(10,4),
            "%_P/L" NUMBER(10,4),
            "NET_P/L" NUMBER(10,4),
            "%_NET_P/L" NUMBER(10,4),
            "DAY_P/L" NUMBER(10,4),
            "%_DAY_P/L" NUMBER(10,4),
            NEXT_PROCESSING_DATE DATE,
            START_DATE DATE,
            END_DATE DATE,
            RECORD_DELETED_FLAG INTEGER
        );''')

def get_first_swing_trade_date_from_trades_table():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT MIN(TRADE_DATE) FROM TRADES WHERE TRADE_EXIT_DATE IS NULL")
    rows = cursor.fetchall()
    conn.close()
    if rows:
        data = {'first_trade_date': rows[0][0]}
        return jsonify(data)
    return None

def get_metrics_from_fin_stock_swing_unrealised_portfolio_view():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('SELECT FIN_INVESTED_AMOUNT, FIN_TOTAL_FEES, FIN_TOTAL_INVESTED_AMOUNT, FIN_CURRENT_VALUE, FIN_PREVIOUS_VALUE, "FIN_P/L", "FIN_%_P/L", "FIN_NET_P/L", "FIN_NET_%_P/L", "FIN_DAY_P/L", "FIN_%_DAY_P/L" FROM FIN_STOCK_SWING_UNREALISED_PORTFOLIO_VIEW;')
    rows = cursor.fetchall()
    conn.close()
    if rows:
        data = [{'fin_invested_amount': row[0], 'fin_total_fees': row[1], 'fin_total_invested_amount': row[2], 'fin_current_value': row[3], 'fin_previous_value': row[4], 'fin_p_l': row[5], 'perc_fin_p_l': row[6], 'fin_net_p_l': row[7], 'perc_fin_net_p_l': row[8], 'fin_day_p_l': row[9], 'perc_fin_day_p_l': row[10]} for row in rows]
        return jsonify(data)
    data = [{'fin_invested_amount': 0, 'fin_total_fees': 0, 'fin_total_invested_amount': 0, 'fin_current_value': 0, 'fin_previous_value': 0, 'fin_p_l': 0, 'perc_fin_p_l': 0, 'fin_net_p_l': 0, 'perc_fin_net_p_l': 0, 'fin_day_p_l': 0, 'perc_fin_day_p_l': 0}]
    return(jsonify(data))

def insert_into_unrealised_swing_stock_hist_returns(processing_date, next_processing_date, prev_processing_date, fin_invested_amount, fin_total_fees, fin_total_invested_amount, fin_current_value, fin_previous_value, fin_p_l, perc_fin_p_l, fin_net_p_l, perc_fin_net_p_l, fin_day_p_l, perc_fin_day_p_l):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('INSERT INTO UNREALISED_SWING_STOCK_HIST_RETURNS (PROCESSING_DATE, PREV_PROCESSING_DATE, AMOUNT_INVESTED_AS_ON_PROCESSING_DATE, TOTAL_FEES, TOTAL_AMOUNT_INVESTED_AS_ON_PROCESSING_DATE, CURRENT_VALUE, PREVIOUS_VALUE, "P/L", "%_P/L", "NET_P/L", "%_NET_P/L", "DAY_P/L", "%_DAY_P/L", NEXT_PROCESSING_DATE, START_DATE, END_DATE, RECORD_DELETED_FLAG) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? ,?)',
                   (processing_date, prev_processing_date, fin_invested_amount, fin_total_fees, fin_total_invested_amount, fin_current_value, fin_previous_value, fin_p_l, perc_fin_p_l, fin_net_p_l, perc_fin_net_p_l, fin_day_p_l, perc_fin_day_p_l, next_processing_date, processing_date, '9998-12-31', 0))
    conn.commit()
    conn.close()

def get_unrealised_swing_stock_returns():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(f'SELECT PROCESSING_DATE, "%_NET_P/L", "%_DAY_P/L" FROM UNREALISED_SWING_STOCK_HIST_RETURNS WHERE RECORD_DELETED_FLAG = 0 ORDER BY PROCESSING_DATE;')
    rows = cursor.fetchall()
    conn.close()
    if rows:
        data = [{'processing_date': row[0], 'perc_net_p_l': row[1], 'perc_day_p_l': row[2]} for row in rows]
        return jsonify(data)
    data = [{'processing_date': None, 'perc_net_p_l': None, 'perc_day_p_l': None}]
    return jsonify(data)

def get_max_next_proc_date_from_unrealised_swing_stock_hist_returns_table():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(f'SELECT MAX(NEXT_PROCESSING_DATE) FROM UNREALISED_SWING_STOCK_HIST_RETURNS WHERE RECORD_DELETED_FLAG = 0;')
    rows = cursor.fetchall()
    conn.close()
    if rows:
        data = {'max_next_processing_date': rows[0][0]}
        return jsonify(data)
    data = {'max_next_processing_date': None}
    return jsonify(data)

def truncate_consolidated_hist_returns_table():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('DROP TABLE IF EXISTS CONSOLIDATED_HIST_RETURNS;')

def create_consolidated_hist_returns_table():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS CONSOLIDATED_HIST_RETURNS (
            ID INTEGER PRIMARY KEY AUTOINCREMENT,
            PROCESSING_DATE DATE,
            PREV_PROCESSING_DATE DATE,
            AMOUNT_INVESTED_AS_ON_PROCESSING_DATE NUMBER(10,4),
            CURRENT_VALUE NUMBER(10,4),
            PREVIOUS_VALUE NUMBER(10,4),
            "P/L" NUMBER(10,4),
            "%_P/L" NUMBER(10,4),
            "DAY_P/L" NUMBER(10,4),
            "%_DAY_P/L" NUMBER(10,4),
            NEXT_PROCESSING_DATE DATE,
            START_DATE DATE,
            END_DATE DATE,
            RECORD_DELETED_FLAG INTEGER
        );''')
    
def truncate_agg_consolidated_hist_returns_table():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('DROP TABLE IF EXISTS AGG_CONSOLIDATED_HIST_RETURNS;')

def create_agg_consolidated_hist_returns_table():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS AGG_CONSOLIDATED_HIST_RETURNS (
            ID INTEGER PRIMARY KEY AUTOINCREMENT,
            PORTFOLIO_TYPE VARCHAR(100),
            PROCESSING_DATE DATE,
            PREV_PROCESSING_DATE DATE,
            AMOUNT_INVESTED_AS_ON_PROCESSING_DATE NUMBER(10,4),
            CURRENT_VALUE NUMBER(10,4),
            PREVIOUS_VALUE NUMBER(10,4),
            "P/L" NUMBER(10,4),
            "%_P/L" NUMBER(10,4),
            "DAY_P/L" NUMBER(10,4),
            "%_DAY_P/L" NUMBER(10,4),
            NEXT_PROCESSING_DATE DATE,
            START_DATE DATE,
            END_DATE DATE,
            RECORD_DELETED_FLAG INTEGER
        );''')


def get_first_purchase_date_from_all_portfolios():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT MIN(SUB.FIRST_PURCHASE_DATE) FROM (SELECT MIN(MF.PURCHASED_ON) AS FIRST_PURCHASE_DATE FROM MF_ORDER MF UNION ALL SELECT MIN(TRD.TRADE_DATE)  AS FIRST_PURCHASE_DATE FROM TRADES TRD) SUB")
    rows = cursor.fetchall()
    conn.close()
    if rows:
        data = {'first_purchase_date': rows[0][0]}
        return jsonify(data)
    return

def get_metrics_from_fin_consolidated_portfolio_view():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('SELECT PROCESSING_DATE, PREV_PROCESSING_DATE, NEXT_PROCESSING_DATE, FIN_INVESTED_AMOUNT, FIN_CURRENT_VALUE, FIN_PREVIOUS_VALUE, "FIN_TOTAL_P/L", "FIN_DAY_P/L", "%_FIN_TOTAL_P/L", "%_FIN_DAY_P/L" FROM FIN_CONSOLIDATED_PORTFOLIO_VIEW;')
    rows = cursor.fetchall()
    conn.close()
    if rows:
        data = [{'processing_date': row[0], 'prev_processing_date': row[1], 'next_processing_date': row[2], 'fin_invested_amount': row[3], 'fin_current_value': row[4], 'fin_previous_value': row[5], 'fin_total_p_l': row[6], 'fin_day_p_l': row[7], 'perc_fin_total_p_l': row[8], 'perc_fin_day_p_l': row[9]} for row in rows]
        return jsonify(data)
    data = [{'processing_date': None, 'prev_processing_date': None, 'next_processing_date': None, 'fin_invested_amount': None, 'fin_current_value': None, 'fin_previous_value': None, 'fin_total_p_l': None, 'fin_day_p_l': None, 'perc_fin_total_p_l': None, 'perc_fin_day_p_l': None}]
    return jsonify(data)

def get_metrics_from_agg_consolidated_portfolio_view():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('SELECT PORTFOLIO_TYPE, PROCESSING_DATE, PREV_PROCESSING_DATE, NEXT_PROCESSING_DATE, AGG_INVESTED_AMOUNT, AGG_CURRENT_VALUE, AGG_PREVIOUS_VALUE, "AGG_TOTAL_P/L", "AGG_DAY_P/L", "%_AGG_TOTAL_P/L", "%_AGG_DAY_P/L" FROM AGG_CONSOLIDATED_PORTFOLIO_VIEW;')
    rows = cursor.fetchall()
    conn.close()
    if rows:
        data = [{'portfolio_type': row[0],'processing_date': row[1], 'prev_processing_date': row[2], 'next_processing_date': row[3], 'agg_invested_amount': row[4], 'agg_current_value': row[5], 'agg_previous_value': row[6], 'agg_total_p_l': row[7], 'agg_day_p_l': row[8], 'perc_agg_total_p_l': row[9], 'perc_agg_day_p_l': row[10]} for row in rows]
        return jsonify(data)
    data = [{'portfolio_type': None,'processing_date': None, 'prev_processing_date': None, 'next_processing_date': None, 'agg_invested_amount': None, 'agg_current_value': None, 'agg_previous_value': None, 'agg_total_p_l': None, 'agg_day_p_l': None, 'perc_agg_total_p_l': None, 'perc_agg_day_p_l': None}]
    return jsonify(data)

def insert_into_consolidated_hist_returns(processing_date_from_fin, prev_processing_date_from_fin, next_processing_date_from_fin, fin_invested_amount, fin_current_value, fin_previous_value, fin_total_p_l, fin_day_p_l, perc_fin_total_p_l, perc_fin_day_p_l):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('INSERT INTO CONSOLIDATED_HIST_RETURNS (PROCESSING_DATE, PREV_PROCESSING_DATE, AMOUNT_INVESTED_AS_ON_PROCESSING_DATE, CURRENT_VALUE, PREVIOUS_VALUE, "P/L", "%_P/L", "DAY_P/L", "%_DAY_P/L", NEXT_PROCESSING_DATE, START_DATE, END_DATE, RECORD_DELETED_FLAG) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
                   (processing_date_from_fin, prev_processing_date_from_fin, fin_invested_amount, fin_current_value, fin_previous_value, fin_total_p_l, perc_fin_total_p_l, fin_day_p_l, perc_fin_day_p_l, next_processing_date_from_fin, processing_date_from_fin, '9998-12-31', 0))
    conn.commit()
    conn.close()

def insert_into_agg_consolidated_hist_returns(portfolio_type, processing_date_from_agg, prev_processing_date_from_agg, next_processing_date_from_agg, agg_invested_amount, agg_current_value, agg_previous_value, agg_total_p_l, agg_day_p_l, perc_agg_total_p_l, perc_agg_day_p_l):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('INSERT INTO AGG_CONSOLIDATED_HIST_RETURNS (PORTFOLIO_TYPE, PROCESSING_DATE, PREV_PROCESSING_DATE, AMOUNT_INVESTED_AS_ON_PROCESSING_DATE, CURRENT_VALUE, PREVIOUS_VALUE, "P/L", "%_P/L", "DAY_P/L", "%_DAY_P/L", NEXT_PROCESSING_DATE, START_DATE, END_DATE, RECORD_DELETED_FLAG) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
                   (portfolio_type, processing_date_from_agg, prev_processing_date_from_agg, agg_invested_amount, agg_current_value, agg_previous_value, agg_total_p_l, perc_agg_total_p_l, agg_day_p_l, perc_agg_day_p_l, next_processing_date_from_agg, processing_date_from_agg, '9998-12-31', 0))
    conn.commit()
    conn.close()

def get_consolidated_hist_returns_from_consolidated_hist_returns_table():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(f'SELECT PROCESSING_DATE, "%_P/L", "%_DAY_P/L" FROM CONSOLIDATED_HIST_RETURNS WHERE RECORD_DELETED_FLAG = 0 ORDER BY PROCESSING_DATE;')
    rows = cursor.fetchall()
    conn.close()
    if rows:
        data = [{'processing_date': row[0], 'perc_total_p_l': row[1], 'perc_day_p_l': row[2]} for row in rows]
        return jsonify(data)
    data = [{'processing_date': None, 'perc_total_p_l': None, 'perc_day_p_l': None}]
    return jsonify(data)

def get_max_next_proc_date_from_consolidated_hist_returns_table():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(f'SELECT MAX(NEXT_PROCESSING_DATE) FROM CONSOLIDATED_HIST_RETURNS WHERE RECORD_DELETED_FLAG = 0;')
    rows = cursor.fetchall()
    conn.close()
    if rows:
        data = {'max_next_processing_date': rows[0][0]}
        return jsonify(data)
    data = {'max_next_processing_date': None}
    return jsonify(data)

def get_max_proc_date_from_all_hist_tables():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT MIN(SUB.PROCESSING_DATE) FROM (SELECT MAX(MF.PROCESSING_DATE)  AS PROCESSING_DATE  FROM MF_HIST_RETURNS MF UNION ALL\
                                                          SELECT MAX(UR.PROCESSING_DATE)  AS PROCESSING_DATE  FROM UNREALISED_SWING_STOCK_HIST_RETURNS UR) SUB;")
    rows = cursor.fetchall()
    conn.close()
    if rows:
        min_of_max_proc_date_from_hist_tables = {'min_of_max_proc_date_from_hist_tables': rows[0][0]}
        return jsonify(min_of_max_proc_date_from_hist_tables)
    return jsonify({'min_of_max_proc_date_from_hist_tables' : None})

def get_all_from_consolidated_hist_returns_table():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('SELECT PORTFOLIO_TYPE, PROCESSING_DATE, PREV_PROCESSING_DATE, NEXT_PROCESSING_DATE, ROUND(AMOUNT_INVESTED_AS_ON_PROCESSING_DATE,2), ROUND(CURRENT_VALUE,2), ROUND(PREVIOUS_VALUE,2), ROUND("P/L",2), ROUND("%_P/L",2), ROUND("DAY_P/L",2), ROUND("%_DAY_P/L",2) FROM AGG_CONSOLIDATED_HIST_RETURNS WHERE RECORD_DELETED_FLAG = 0 ORDER BY PROCESSING_DATE;')
    agg_rows = cursor.fetchall()
    if agg_rows:
        agg_data = [{'portfolio_type': row[0],'processing_date': row[1], 'prev_processing_date': row[2], 'next_processing_date': row[3], 'agg_total_invested_amount': row[4], 'agg_current_value': row[5], 'agg_previous_value': row[6], 'agg_total_p_l': row[7], 'perc_agg_total_p_l': row[8], 'agg_day_p_l': row[9], 'perc_agg_day_p_l': row[10]} for row in agg_rows]
    else:
        agg_data = [{'portfolio_type': None,'processing_date': None, 'prev_processing_date': None, 'next_processing_date': None, 'agg_total_invested_amount': None, 'agg_current_value': None, 'agg_previous_value': None, 'agg_total_p_l': None, 'perc_agg_total_p_l': None, 'agg_day_p_l': None, 'perc_agg_day_p_l': None}]
    cursor.execute('SELECT PROCESSING_DATE, PREV_PROCESSING_DATE, NEXT_PROCESSING_DATE, ROUND(AMOUNT_INVESTED_AS_ON_PROCESSING_DATE,2), ROUND(CURRENT_VALUE,2), ROUND(PREVIOUS_VALUE,2), ROUND("P/L",2), ROUND("%_P/L",2), ROUND("DAY_P/L",2), ROUND("%_DAY_P/L",2)  FROM CONSOLIDATED_HIST_RETURNS WHERE RECORD_DELETED_FLAG = 0 ORDER BY PROCESSING_DATE;')
    cons_rows = cursor.fetchall()
    if cons_rows:
        cons_data = [{'processing_date': row[0], 'prev_processing_date': row[1], 'next_processing_date': row[2], 'fin_invested_amount': row[3], 'fin_current_value': row[4], 'fin_previous_value': row[5], 'fin_total_p_l': row[6], 'perc_fin_total_p_l': row[7], 'fin_day_p_l': row[8], 'perc_fin_day_p_l': row[9]} for row in cons_rows]
    else:
        cons_data = [{'processing_date': None, 'prev_processing_date': None, 'next_processing_date': None, 'fin_invested_amount': None, 'fin_current_value': None, 'fin_previous_value': None, 'fin_total_p_l': None, 'perc_fin_total_p_l': None, 'fin_day_p_l': None, 'perc_fin_day_p_l': None}]

    # Latest Data Fetch
    cursor.execute('SELECT PORTFOLIO_TYPE, PROCESSING_DATE, PREV_PROCESSING_DATE, NEXT_PROCESSING_DATE, ROUND(AMOUNT_INVESTED_AS_ON_PROCESSING_DATE,2), ROUND(CURRENT_VALUE,2), ROUND(PREVIOUS_VALUE,2), ROUND("P/L",2), ROUND("%_P/L",2), ROUND("DAY_P/L",2), ROUND("%_DAY_P/L",2) FROM AGG_CONSOLIDATED_HIST_RETURNS WHERE RECORD_DELETED_FLAG = 0  AND PROCESSING_DATE = (SELECT MAX(PROCESSING_DATE) FROM AGG_CONSOLIDATED_HIST_RETURNS);')
    latest_agg_rows = cursor.fetchall()
    if latest_agg_rows:
        latest_agg_data = [{'portfolio_type': row[0],'processing_date': row[1], 'prev_processing_date': row[2], 'next_processing_date': row[3], 'agg_total_invested_amount': row[4], 'agg_current_value': row[5], 'agg_previous_value': row[6], 'agg_total_p_l': row[7], 'perc_agg_total_p_l': row[8], 'agg_day_p_l': row[9], 'perc_agg_day_p_l': row[10]} for row in latest_agg_rows]
    else:
        latest_agg_data = [{'portfolio_type': None,'processing_date': None, 'prev_processing_date': None, 'next_processing_date': None, 'agg_total_invested_amount': None, 'agg_current_value': None, 'agg_previous_value': None, 'agg_total_p_l': None, 'perc_agg_total_p_l': None, 'agg_day_p_l': None, 'perc_agg_day_p_l': None}]
    cursor.execute('SELECT PROCESSING_DATE, PREV_PROCESSING_DATE, NEXT_PROCESSING_DATE, ROUND(AMOUNT_INVESTED_AS_ON_PROCESSING_DATE,2), ROUND(CURRENT_VALUE,2), ROUND(PREVIOUS_VALUE,2), ROUND("P/L",2), ROUND("%_P/L",2), ROUND("DAY_P/L",2), ROUND("%_DAY_P/L",2)  FROM CONSOLIDATED_HIST_RETURNS WHERE RECORD_DELETED_FLAG = 0 AND PROCESSING_DATE = (SELECT MAX(PROCESSING_DATE) FROM CONSOLIDATED_HIST_RETURNS);')
    latest_cons_rows = cursor.fetchall()
    if latest_cons_rows:
        latest_cons_data = [{'processing_date': row[0], 'prev_processing_date': row[1], 'next_processing_date': row[2], 'fin_invested_amount': row[3], 'fin_current_value': row[4], 'fin_previous_value': row[5], 'fin_total_p_l': row[6], 'perc_fin_total_p_l': row[7], 'fin_day_p_l': row[8], 'perc_fin_day_p_l': row[9]} for row in latest_cons_rows]
    else:
        latest_cons_data = [{'processing_date': None, 'prev_processing_date': None, 'next_processing_date': None, 'fin_invested_amount': None, 'fin_current_value': None, 'fin_previous_value': None, 'fin_total_p_l': None, 'perc_fin_total_p_l': None, 'fin_day_p_l': None, 'perc_fin_day_p_l': None}]
    conn.close()
    data = {'agg_data': agg_data, 'cons_data': cons_data, 'latest_agg_data': latest_agg_data, 'latest_cons_data' : latest_cons_data}
    return jsonify(data)

def get_max_next_proc_date_from_consolidated_hist_allocation_table():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('SELECT MAX(NEXT_PROCESSING_DATE) FROM CONSOLIDATED_HIST_ALLOCATION_PORTFOLIO WHERE RECORD_DELETED_FLAG = 0;')
    rows = cursor.fetchall()
    conn.close()
    if rows:
        data = {'max_next_processing_date': rows[0][0]}
        return jsonify(data)
    data = {'max_next_processing_date': None}
    return jsonify(data)

def truncate_consolidated_hist_allocation_table():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('DROP TABLE IF EXISTS CONSOLIDATED_HIST_ALLOCATION;')

def create_consolidated_hist_allocation_table():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS CONSOLIDATED_HIST_ALLOCATION (
            ID INTEGER PRIMARY KEY AUTOINCREMENT,
            PORTFOLIO_TYPE VARCHAR(100),
            PORTFOLIO_NAME VARCHAR(100),
            PORTFOLIO_HOUSE VARCHAR(100),
            PORTFOLIO_SUB_TYPE VARCHAR(100),
            PORTFOLIO_CATEGORY VARCHAR(100),
            PROCESSING_DATE DATE,
            PREV_PROCESSING_DATE DATE,
            AMOUNT_INVESTED_AS_ON_PROCESSING_DATE NUMBER(10,4),
            AMOUNT_INVESTED_EXCLUDING_FEES_AS_ON_PROCESSING_DATE NUMBER(10,4),
            QUANTITY INTEGER,
            CURRENT_VALUE NUMBER(10,4),
            PREVIOUS_VALUE NUMBER(10,4),
            "P/L" NUMBER(10,4),
            "%_P/L" NUMBER(10,4),
            "DAY_P/L" NUMBER(10,4),
            "%_DAY_P/L" NUMBER(10,4),
            AVERAGE_PRICE NUMBER(10,4),
            FIN_INVESTED_AMOUNT NUMBER(10,4),
            FIN_INVESTED_AMOUNT_EXCLUDING_FEES NUMBER(10,4),
            FIN_QUANTITY INTEGER,
            FIN_CURRENT_AMOUNT NUMBER(10,4),
            FIN_PREVIOUS_AMOUNT NUMBER(10,4),
            "FIN_P/L" NUMBER(10,4),
            "FIN_DAY_P/L" NUMBER(10,4),
            "FIN_ALLOC_%_PORTFOLIO_INVESTED_AMOUNT_TYP_CD" VARCHAR(100),
            "FIN_ALLOC_%_PORTFOLIO_INVESTED_AMOUNT" NUMBER(10,2),
            "FIN_ALLOC_%_PORTFOLIO_INVESTED_AMOUNT_EXCLUDING_FEES_TYP_CD" VARCHAR(100),
            "FIN_ALLOC_%_PORTFOLIO_INVESTED_AMOUNT_EXCLUDING_FEES" NUMBER(10,2),
            "FIN_ALLOC_%_QUANTITY_TYP_CD" VARCHAR(100),
            "FIN_ALLOC_%_QUANTITY" NUMBER(10,2),
            "FIN_ALLOC_%_CURRENT_VALUE_TYP_CD" VARCHAR(100),
            "FIN_ALLOC_%_CURRENT_VALUE" NUMBER(10,2),
            "FIN_ALLOC_%_PREVIOUS_VALUE_TYP_CD" VARCHAR(100),
            "FIN_ALLOC_%_PREVIOUS_VALUE" NUMBER(10,2),
            "FIN_ALLOC_%_P/L_TYP_CD" VARCHAR(100),
            "FIN_ALLOC_%_P/L" NUMBER(10,2),
            "FIN_ALLOC_%_DAY_P/L_TYP_CD" VARCHAR(100),
            "FIN_ALLOC_%_DAY_P/L" NUMBER(10,2),
            NEXT_PROCESSING_DATE DATE,
            START_DATE DATE,
            END_DATE DATE,
            RECORD_DELETED_FLAG INTEGER
        );''')

def truncate_consolidated_hist_allocation_portfolio_table():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('DROP TABLE IF EXISTS CONSOLIDATED_HIST_ALLOCATION_PORTFOLIO;')

def create_consolidated_hist_allocation_portfolio_table():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS CONSOLIDATED_HIST_ALLOCATION_PORTFOLIO (
            ID INTEGER PRIMARY KEY AUTOINCREMENT,
            PORTFOLIO_TYPE VARCHAR(100),
            PROCESSING_DATE DATE,
            PREV_PROCESSING_DATE DATE,
            AMOUNT_INVESTED_AS_ON_PROCESSING_DATE NUMBER(10,4),
            AMOUNT_INVESTED_EXCLUDING_FEES_AS_ON_PROCESSING_DATE NUMBER(10,4),
            QUANTITY INTEGER,
            CURRENT_VALUE NUMBER(10,4),
            PREVIOUS_VALUE NUMBER(10,4),
            "P/L" NUMBER(10,4),
            "DAY_P/L" NUMBER(10,4),
            AVERAGE_PRICE NUMBER(10,4),
            FIN_INVESTED_AMOUNT NUMBER(10,4),
            FIN_INVESTED_AMOUNT_EXCLUDING_FEES NUMBER(10,4),
            FIN_QUANTITY INTEGER,
            FIN_CURRENT_AMOUNT NUMBER(10,4),
            FIN_PREVIOUS_AMOUNT NUMBER(10,4),
            "FIN_P/L" NUMBER(10,4),
            "FIN_DAY_P/L" NUMBER(10,4),
            "FIN_ALLOC_%_PORTFOLIO_INVESTED_AMOUNT_TYP_CD" VARCHAR(100),
            "FIN_ALLOC_%_PORTFOLIO_INVESTED_AMOUNT" NUMBER(10,2),
            "FIN_ALLOC_%_PORTFOLIO_INVESTED_AMOUNT_EXCLUDING_FEES_TYP_CD" VARCHAR(100),
            "FIN_ALLOC_%_PORTFOLIO_INVESTED_AMOUNT_EXCLUDING_FEES" NUMBER(10,2),
            "FIN_ALLOC_%_QUANTITY_TYP_CD" VARCHAR(100),
            "FIN_ALLOC_%_QUANTITY" NUMBER(10,2),
            "FIN_ALLOC_%_CURRENT_VALUE_TYP_CD" VARCHAR(100),
            "FIN_ALLOC_%_CURRENT_VALUE" NUMBER(10,2),
            "FIN_ALLOC_%_PREVIOUS_VALUE_TYP_CD" VARCHAR(100),
            "FIN_ALLOC_%_PREVIOUS_VALUE" NUMBER(10,2),
            "FIN_ALLOC_%_P/L_TYP_CD" VARCHAR(100),
            "FIN_ALLOC_%_P/L" NUMBER(10,2),
            "FIN_ALLOC_%_DAY_P/L_TYP_CD" VARCHAR(100),
            "FIN_ALLOC_%_DAY_P/L" NUMBER(10,2),
            NEXT_PROCESSING_DATE DATE,
            START_DATE DATE,
            END_DATE DATE,
            RECORD_DELETED_FLAG INTEGER
        );''')

def truncate_agg_consolidated_hist_allocation_table():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('DROP TABLE IF EXISTS AGG_CONSOLIDATED_HIST_ALLOCATION;')

def create_agg_consolidated_hist_allocation_table():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS AGG_CONSOLIDATED_HIST_ALLOCATION (
            ID INTEGER PRIMARY KEY AUTOINCREMENT,
            PORTFOLIO_TYPE VARCHAR(100),
            PORTFOLIO_NAME VARCHAR(100),
            PORTFOLIO_HOUSE VARCHAR(100),
            PORTFOLIO_SUB_TYPE VARCHAR(100),
            PORTFOLIO_CATEGORY VARCHAR(100),
            PROCESSING_DATE DATE,
            PREV_PROCESSING_DATE DATE,
            AMOUNT_INVESTED_AS_ON_PROCESSING_DATE NUMBER(10,4),
            AMOUNT_INVESTED_EXCLUDING_FEES_AS_ON_PROCESSING_DATE NUMBER(10,4),
            QUANTITY INTEGER,
            CURRENT_VALUE NUMBER(10,4),
            PREVIOUS_VALUE NUMBER(10,4),
            "P/L" NUMBER(10,4),
            "%_P/L" NUMBER(10,4),
            "DAY_P/L" NUMBER(10,4),
            "%_DAY_P/L" NUMBER(10,4),
            AVERAGE_PRICE NUMBER(10,4),
            AGG_INVESTED_AMOUNT NUMBER(10,4),
            AGG_INVESTED_AMOUNT_EXCLUDING_FEES NUMBER(10,4),
            AGG_QUANTITY INTEGER,
            AGG_CURRENT_AMOUNT NUMBER(10,4),
            AGG_PREVIOUS_AMOUNT NUMBER(10,4),
            "AGG_P/L" NUMBER(10,4),
            "AGG_DAY_P/L" NUMBER(10,4),
            "AGG_ALLOC_%_PORTFOLIO_INVESTED_AMOUNT_TYP_CD" VARCHAR(100),
            "AGG_ALLOC_%_PORTFOLIO_INVESTED_AMOUNT" NUMBER(10,2),
            "AGG_ALLOC_%_PORTFOLIO_INVESTED_AMOUNT_EXCLUDING_FEES_TYP_CD" VARCHAR(100),
            "AGG_ALLOC_%_PORTFOLIO_INVESTED_AMOUNT_EXCLUDING_FEES" NUMBER(10,2),
            "AGG_ALLOC_%_QUANTITY_TYP_CD" VARCHAR(100),
            "AGG_ALLOC_%_QUANTITY" NUMBER(10,2),
            "AGG_ALLOC_%_CURRENT_VALUE_TYP_CD" VARCHAR(100),
            "AGG_ALLOC_%_CURRENT_VALUE" NUMBER(10,2),
            "AGG_ALLOC_%_PREVIOUS_VALUE_TYP_CD" VARCHAR(100),
            "AGG_ALLOC_%_PREVIOUS_VALUE" NUMBER(10,2),
            "AGG_ALLOC_%_P/L_TYP_CD" VARCHAR(100),
            "AGG_ALLOC_%_P/L" NUMBER(10,2),
            "AGG_ALLOC_%_DAY_P/L_TYP_CD" VARCHAR(100),
            "AGG_ALLOC_%_DAY_P/L" NUMBER(10,2),
            NEXT_PROCESSING_DATE DATE,
            START_DATE DATE,
            END_DATE DATE,
            RECORD_DELETED_FLAG INTEGER
        );''')

def get_metrics_from_agg_consolidated_allocation_view_and_insert_into_agg_consolidated_allocation_table():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('''
INSERT INTO AGG_CONSOLIDATED_HIST_ALLOCATION (
    PORTFOLIO_TYPE,
    PORTFOLIO_NAME,
    PORTFOLIO_HOUSE,
    PORTFOLIO_SUB_TYPE,
    PORTFOLIO_CATEGORY,
    PROCESSING_DATE,
    PREV_PROCESSING_DATE,
    AMOUNT_INVESTED_AS_ON_PROCESSING_DATE,
    AMOUNT_INVESTED_EXCLUDING_FEES_AS_ON_PROCESSING_DATE,
    QUANTITY,
    CURRENT_VALUE,
    PREVIOUS_VALUE,
    "P/L",
    "%_P/L",
    "DAY_P/L",
    "%_DAY_P/L",
    AVERAGE_PRICE,
    AGG_INVESTED_AMOUNT,
    AGG_INVESTED_AMOUNT_EXCLUDING_FEES,
    AGG_QUANTITY,
    AGG_CURRENT_AMOUNT,
    AGG_PREVIOUS_AMOUNT,
    "AGG_P/L",
    "AGG_DAY_P/L",
    "AGG_ALLOC_%_PORTFOLIO_INVESTED_AMOUNT_TYP_CD",
    "AGG_ALLOC_%_PORTFOLIO_INVESTED_AMOUNT",
    "AGG_ALLOC_%_PORTFOLIO_INVESTED_AMOUNT_EXCLUDING_FEES_TYP_CD",
    "AGG_ALLOC_%_PORTFOLIO_INVESTED_AMOUNT_EXCLUDING_FEES",
    "AGG_ALLOC_%_QUANTITY_TYP_CD",
    "AGG_ALLOC_%_QUANTITY",
    "AGG_ALLOC_%_CURRENT_VALUE_TYP_CD",
    "AGG_ALLOC_%_CURRENT_VALUE",
    "AGG_ALLOC_%_PREVIOUS_VALUE_TYP_CD",
    "AGG_ALLOC_%_PREVIOUS_VALUE",
    "AGG_ALLOC_%_P/L_TYP_CD",
    "AGG_ALLOC_%_P/L",
    "AGG_ALLOC_%_DAY_P/L_TYP_CD",
    "AGG_ALLOC_%_DAY_P/L",
    NEXT_PROCESSING_DATE,
    START_DATE,
    END_DATE,
    RECORD_DELETED_FLAG
)
SELECT
    A.PORTFOLIO_TYPE,
    A.PORTFOLIO_NAME,
    A.PORTFOLIO_HOUSE,
    A.PORTFOLIO_SUB_TYPE,
    A.PORTFOLIO_CATEGORY,
    P.PROC_DATE AS PROCESSING_DATE,
    P.PREV_PROC_DATE AS PREV_PROCESSING_DATE,
    A.INVESTED_AMOUNT AS AMOUNT_INVESTED_AS_ON_PROCESSING_DATE,
    A.INVESTED_AMOUNT_EXCLUDING_FEES AS AMOUNT_INVESTED_EXCLUDING_FEES_AS_ON_PROCESSING_DATE,
    A.QUANTITY,
    A.CURRENT_VALUE,
    A.PREVIOUS_VALUE,
    A."P/L",
    A."%_P_L",
    A."DAY_P/L",
    A."%_DAY_P/L",
    A.AVERAGE_PRICE,
    A.AGG_INVESTED_AMOUNT,
    A.AGG_INVESTED_AMOUNT_EXCLUDING_FEES,
    A.AGG_QUANTITY,
    A.AGG_CURRENT_AMOUNT,
    A.AGG_PREVIOUS_AMOUNT,
    A."AGG_P/L",
    A."AGG_DAY_P/L",
    A."AGG_ALLOC_%_PORTFOLIO_INVESTED_AMOUNT_TYP_CD",
    A."AGG_ALLOC_%_PORTFOLIO_INVESTED_AMOUNT",
    A."AGG_ALLOC_%_PORTFOLIO_INVESTED_AMOUNT_EXCLUDING_FEES_TYP_CD",
    A."AGG_ALLOC_%_PORTFOLIO_INVESTED_AMOUNT_EXCLUDING_FEES",
    A."AGG_ALLOC_%_QUANTITY_TYP_CD",
    A."AGG_ALLOC_%_QUANTITY",
    A."AGG_ALLOC_%_CURRENT_VALUE_TYP_CD",
    A."AGG_ALLOC_%_CURRENT_VALUE",
    A."AGG_ALLOC_%_PREVIOUS_VALUE_TYP_CD",
    A."AGG_ALLOC_%_PREVIOUS_VALUE",
    A."AGG_ALLOC_%_P/L_TYP_CD",
    A."AGG_ALLOC_%_P/L",
    A."AGG_ALLOC_%_DAY_P/L_TYP_CD",
    A."AGG_ALLOC_%_DAY_P/L",
    P.NEXT_PROC_DATE AS NEXT_PROCESSING_DATE,
    P.PROC_DATE AS START_DATE,
    '9998-12-31' AS END_DATE,
    0 AS RECORD_DELETED_FLAG
FROM
    AGG_CONSOLIDATED_ALLOCATION_VIEW A
INNER JOIN
    PROCESSING_DATE P 
ON
    P.PROC_TYP_CD = 'MF_PROC';
''')
    conn.commit()
    conn.close()

def get_metrics_from_fin_consolidated_allocation_view_and_insert_into_fin_consolidated_allocation_table():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('''
INSERT INTO CONSOLIDATED_HIST_ALLOCATION (
    PORTFOLIO_TYPE,
    PORTFOLIO_NAME,
    PORTFOLIO_HOUSE,
    PORTFOLIO_SUB_TYPE,
    PORTFOLIO_CATEGORY,
    PROCESSING_DATE,
    PREV_PROCESSING_DATE,
    AMOUNT_INVESTED_AS_ON_PROCESSING_DATE,
    AMOUNT_INVESTED_EXCLUDING_FEES_AS_ON_PROCESSING_DATE,
    QUANTITY,
    CURRENT_VALUE,
    PREVIOUS_VALUE,
    "P/L",
    "%_P/L",
    "DAY_P/L",
    "%_DAY_P/L",
    AVERAGE_PRICE,
    FIN_INVESTED_AMOUNT,
    FIN_INVESTED_AMOUNT_EXCLUDING_FEES,
    FIN_QUANTITY,
    FIN_CURRENT_AMOUNT,
    FIN_PREVIOUS_AMOUNT,
    "FIN_P/L",
    "FIN_DAY_P/L",
    "FIN_ALLOC_%_PORTFOLIO_INVESTED_AMOUNT_TYP_CD",
    "FIN_ALLOC_%_PORTFOLIO_INVESTED_AMOUNT",
    "FIN_ALLOC_%_PORTFOLIO_INVESTED_AMOUNT_EXCLUDING_FEES_TYP_CD",
    "FIN_ALLOC_%_PORTFOLIO_INVESTED_AMOUNT_EXCLUDING_FEES",
    "FIN_ALLOC_%_QUANTITY_TYP_CD",
    "FIN_ALLOC_%_QUANTITY",
    "FIN_ALLOC_%_CURRENT_VALUE_TYP_CD",
    "FIN_ALLOC_%_CURRENT_VALUE",
    "FIN_ALLOC_%_PREVIOUS_VALUE_TYP_CD",
    "FIN_ALLOC_%_PREVIOUS_VALUE",
    "FIN_ALLOC_%_P/L_TYP_CD",
    "FIN_ALLOC_%_P/L",
    "FIN_ALLOC_%_DAY_P/L_TYP_CD",
    "FIN_ALLOC_%_DAY_P/L",
    NEXT_PROCESSING_DATE,
    START_DATE,
    END_DATE,
    RECORD_DELETED_FLAG
)
SELECT
    V.PORTFOLIO_TYPE,
    V.PORTFOLIO_NAME,
    V.PORTFOLIO_HOUSE,
    V.PORTFOLIO_SUB_TYPE,
    V.PORTFOLIO_CATEGORY,
    PD.PROC_DATE                          AS PROCESSING_DATE,
    PD.PREV_PROC_DATE                     AS PREV_PROCESSING_DATE,
    V.INVESTED_AMOUNT                     AS AMOUNT_INVESTED_AS_ON_PROCESSING_DATE,
    V.INVESTED_AMOUNT_EXCLUDING_FEES      AS AMOUNT_INVESTED_EXCLUDING_FEES_AS_ON_PROCESSING_DATE,
    V.QUANTITY,
    V.CURRENT_VALUE,
    V.PREVIOUS_VALUE,
    V."P/L",
    V."%_P_L",
    V."DAY_P/L",
    V."%_DAY_P/L",
    V.AVERAGE_PRICE,
    V.FIN_INVESTED_AMOUNT,
    V.FIN_INVESTED_AMOUNT_EXCLUDING_FEES,
    V.FIN_QUANTITY,
    V.FIN_CURRENT_AMOUNT,
    V.FIN_PREVIOUS_AMOUNT,
    V."FIN_P/L",
    V."FIN_DAY_P/L",
    V."FIN_ALLOC_%_PORTFOLIO_INVESTED_AMOUNT_TYP_CD",
    V."FIN_ALLOC_%_PORTFOLIO_INVESTED_AMOUNT",
    V."FIN_ALLOC_%_PORTFOLIO_INVESTED_AMOUNT_EXCLUDING_FEES_TYP_CD",
    V."FIN_ALLOC_%_PORTFOLIO_INVESTED_AMOUNT_EXCLUDING_FEES",
    V."FIN_ALLOC_%_QUANTITY_TYP_CD",
    V."FIN_ALLOC_%_QUANTITY",
    V."FIN_ALLOC_%_CURRENT_VALUE_TYP_CD",
    V."FIN_ALLOC_%_CURRENT_VALUE",
    V."FIN_ALLOC_%_PREVIOUS_VALUE_TYP_CD",
    V."FIN_ALLOC_%_PREVIOUS_VALUE",
    V."FIN_ALLOC_%_P/L_TYP_CD",
    V."FIN_ALLOC_%_P/L",
    V."FIN_ALLOC_%_DAY_P/L_TYP_CD",
    V."FIN_ALLOC_%_DAY_P/L",
    PD.NEXT_PROC_DATE                     AS NEXT_PROCESSING_DATE,
    PD.PROC_DATE                          AS START_DATE,
    '9998-12-31'                          AS END_DATE,
    0                                     AS RECORD_DELETED_FLAG
FROM
    FIN_CONSOLIDATED_ALLOCATION_VIEW V
INNER JOIN
    PROCESSING_DATE PD
ON
    PD.PROC_TYP_CD = 'MF_PROC';
''')
    conn.commit()
    conn.close()

def get_metrics_from_fin_consolidated_allocation_portfolio_view_and_insert_into_fin_consolidated_allocation_portfolio_table():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('''
INSERT INTO CONSOLIDATED_HIST_ALLOCATION_PORTFOLIO (
    PORTFOLIO_TYPE,
    PROCESSING_DATE,
    PREV_PROCESSING_DATE,
    AMOUNT_INVESTED_AS_ON_PROCESSING_DATE,
    AMOUNT_INVESTED_EXCLUDING_FEES_AS_ON_PROCESSING_DATE,
    QUANTITY,
    CURRENT_VALUE,
    PREVIOUS_VALUE,
    "P/L",
    "DAY_P/L",
    AVERAGE_PRICE,
    FIN_INVESTED_AMOUNT,
    FIN_INVESTED_AMOUNT_EXCLUDING_FEES,
    FIN_QUANTITY,
    FIN_CURRENT_AMOUNT,
    FIN_PREVIOUS_AMOUNT,
    "FIN_P/L",
    "FIN_DAY_P/L",
    "FIN_ALLOC_%_PORTFOLIO_INVESTED_AMOUNT_TYP_CD",
    "FIN_ALLOC_%_PORTFOLIO_INVESTED_AMOUNT",
    "FIN_ALLOC_%_PORTFOLIO_INVESTED_AMOUNT_EXCLUDING_FEES_TYP_CD",
    "FIN_ALLOC_%_PORTFOLIO_INVESTED_AMOUNT_EXCLUDING_FEES",
    "FIN_ALLOC_%_QUANTITY_TYP_CD",
    "FIN_ALLOC_%_QUANTITY",
    "FIN_ALLOC_%_CURRENT_VALUE_TYP_CD",
    "FIN_ALLOC_%_CURRENT_VALUE",
    "FIN_ALLOC_%_PREVIOUS_VALUE_TYP_CD",
    "FIN_ALLOC_%_PREVIOUS_VALUE",
    "FIN_ALLOC_%_P/L_TYP_CD",
    "FIN_ALLOC_%_P/L",
    "FIN_ALLOC_%_DAY_P/L_TYP_CD",
    "FIN_ALLOC_%_DAY_P/L",
    NEXT_PROCESSING_DATE,
    START_DATE,
    END_DATE,
    RECORD_DELETED_FLAG
)
SELECT
    V.PORTFOLIO_TYPE,
    PD.PROC_DATE                         AS PROCESSING_DATE,
    PD.PREV_PROC_DATE                    AS PREV_PROCESSING_DATE,
    V.FIN_INVESTED_AMOUNT                AS AMOUNT_INVESTED_AS_ON_PROCESSING_DATE,
    V.FIN_INVESTED_AMOUNT_EXCLUDING_FEES AS AMOUNT_INVESTED_EXCLUDING_FEES_AS_ON_PROCESSING_DATE,
    V.FIN_QUANTITY                       AS QUANTITY,
    V.FIN_CURRENT_VALUE                  AS CURRENT_VALUE,
    V.FIN_PREVIOUS_VALUE                 AS PREVIOUS_VALUE,
    V."P/L",
    V."DAY_P/L",
    V.AVERAGE_PRICE,
    V.FIN_INVESTED_AMOUNT,
    V.FIN_INVESTED_AMOUNT_EXCLUDING_FEES,
    V.FIN_QUANTITY,
    V.FIN_CURRENT_AMOUNT,
    V.FIN_PREVIOUS_AMOUNT,
    V."FIN_P/L",
    V."FIN_DAY_P/L",
    V."FIN_ALLOC_%_PORTFOLIO_INVESTED_AMOUNT_TYP_CD",
    V."FIN_ALLOC_%_PORTFOLIO_INVESTED_AMOUNT",
    V."FIN_ALLOC_%_PORTFOLIO_INVESTED_AMOUNT_EXCLUDING_FEES_TYP_CD",
    V."FIN_ALLOC_%_PORTFOLIO_INVESTED_AMOUNT_EXCLUDING_FEES",
    V."FIN_ALLOC_%_QUANTITY_TYP_CD",
    V."FIN_ALLOC_%_QUANTITY",
    V."FIN_ALLOC_%_CURRENT_VALUE_TYP_CD",
    V."FIN_ALLOC_%_CURRENT_VALUE",
    V."FIN_ALLOC_%_PREVIOUS_VALUE_TYP_CD",
    V."FIN_ALLOC_%_PREVIOUS_VALUE",
    V."FIN_ALLOC_%_P/L_TYP_CD",
    V."FIN_ALLOC_%_P/L",
    V."FIN_ALLOC_%_DAY_P/L_TYP_CD",
    V."FIN_ALLOC_%_DAY_P/L",
    PD.NEXT_PROC_DATE                   AS NEXT_PROCESSING_DATE,
    PD.PROC_DATE                        AS START_DATE,
    '9998-12-31'                        AS END_DATE,
    0                                   AS RECORD_DELETED_FLAG
FROM
    FIN_CONSOLIDATED_ALLOCATION_PORTFOLIO_VIEW V
INNER JOIN
    PROCESSING_DATE PD
ON
    PD.PROC_TYP_CD = 'MF_PROC';
''')
    conn.commit()
    conn.close()

def get_consolidated_hist_allocation_portfolio_from_consolidated_hist_allocation_portfolio_table():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(f'SELECT PROCESSING_DATE, PORTFOLIO_TYPE, "FIN_ALLOC_%_PORTFOLIO_INVESTED_AMOUNT" FROM CONSOLIDATED_HIST_ALLOCATION_PORTFOLIO WHERE RECORD_DELETED_FLAG = 0 ORDER BY PROCESSING_DATE;')
    rows = cursor.fetchall()
    conn.close()
    if rows:
        data = [{'processing_date': row[0], 'portfolio_type': row[1], 'fin_alloc_perc_portfolio_invested_amount': row[2]} for row in rows]
        return jsonify(data)
    data = [{'processing_date': None, 'portfolio_type': None, 'fin_alloc_perc_portfolio_invested_amount': None}]
    return jsonify(data)

def get_all_from_consolidated_hist_allocation_table():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Consolidated Hist Allocation
    cursor.execute('''
    SELECT 
         PORTFOLIO_TYPE
        ,PORTFOLIO_NAME
        ,PORTFOLIO_HOUSE
        ,PORTFOLIO_SUB_TYPE
        ,PORTFOLIO_CATEGORY
        ,PROCESSING_DATE
        ,PREV_PROCESSING_DATE
        ,AMOUNT_INVESTED_AS_ON_PROCESSING_DATE
        ,AMOUNT_INVESTED_EXCLUDING_FEES_AS_ON_PROCESSING_DATE
        ,QUANTITY
        ,CURRENT_VALUE
        ,PREVIOUS_VALUE
        ,"P/L"
        ,"%_P/L"
        ,"DAY_P/L"
        ,"%_DAY_P/L"
        ,AVERAGE_PRICE
        ,FIN_INVESTED_AMOUNT
        ,FIN_INVESTED_AMOUNT_EXCLUDING_FEES
        ,FIN_QUANTITY
        ,FIN_CURRENT_AMOUNT
        ,FIN_PREVIOUS_AMOUNT
        ,"FIN_P/L"
        ,"FIN_DAY_P/L"
        ,"FIN_ALLOC_%_PORTFOLIO_INVESTED_AMOUNT_TYP_CD"
        ,"FIN_ALLOC_%_PORTFOLIO_INVESTED_AMOUNT"
        ,"FIN_ALLOC_%_PORTFOLIO_INVESTED_AMOUNT_EXCLUDING_FEES_TYP_CD"
        ,"FIN_ALLOC_%_PORTFOLIO_INVESTED_AMOUNT_EXCLUDING_FEES"
        ,"FIN_ALLOC_%_QUANTITY_TYP_CD"
        ,"FIN_ALLOC_%_QUANTITY"
        ,"FIN_ALLOC_%_CURRENT_VALUE_TYP_CD"
        ,"FIN_ALLOC_%_CURRENT_VALUE"
        ,"FIN_ALLOC_%_PREVIOUS_VALUE_TYP_CD"
        ,"FIN_ALLOC_%_PREVIOUS_VALUE"
        ,"FIN_ALLOC_%_P/L_TYP_CD"
        ,"FIN_ALLOC_%_P/L"
        ,"FIN_ALLOC_%_DAY_P/L_TYP_CD"
        ,"FIN_ALLOC_%_DAY_P/L"
        ,NEXT_PROCESSING_DATE
    FROM CONSOLIDATED_HIST_ALLOCATION
    WHERE RECORD_DELETED_FLAG = 0
    ORDER BY PROCESSING_DATE;
''')
    rows = cursor.fetchall()
    if rows:
        consolidated_allocation_data = [{
            'portfolio_type': row[0],
            'portfolio_name': row[1],
            'portfolio_house': row[2],
            'portfolio_sub_type': row[3],
            'portfolio_category': row[4],
            'processing_date': row[5],
            'prev_processing_date': row[6],
            'amount_invested': row[7],
            'amount_invested_excl_fees': row[8],
            'quantity': row[9],
            'current_value': row[10],
            'previous_value': row[11],
            'p_l': row[12],
            'perc_p_l': row[13],
            'day_p_l': row[14],
            'perc_day_p_l': row[15],
            'average_price': row[16],
            'fin_invested_amount': row[17],
            'fin_invested_amount_excl_fees': row[18],
            'fin_quantity': row[19],
            'fin_current_amount': row[20],
            'fin_previous_amount': row[21],
            'fin_p_l': row[22],
            'fin_day_p_l': row[23],
            'fin_alloc_perc_inv_amt_typ_cd': row[24],
            'fin_alloc_perc_inv_amt': row[25],
            'fin_alloc_perc_inv_amt_excl_fees_typ_cd': row[26],
            'fin_alloc_perc_inv_amt_excl_fees': row[27],
            'fin_alloc_perc_quantity_typ_cd': row[28],
            'fin_alloc_perc_quantity': row[29],
            'fin_alloc_perc_curr_val_typ_cd': row[30],
            'fin_alloc_perc_curr_val': row[31],
            'fin_alloc_perc_prev_val_typ_cd': row[32],
            'fin_alloc_perc_prev_val': row[33],
            'fin_alloc_perc_p_l_typ_cd': row[34],
            'fin_alloc_perc_p_l': row[35],
            'fin_alloc_perc_day_p_l_typ_cd': row[36],
            'fin_alloc_perc_day_p_l': row[37],
            'next_processing_date': row[38]
        } for row in rows]
    else:
        consolidated_allocation_data = [{
            'portfolio_type': None,
            'portfolio_name': None,
            'portfolio_house': None,
            'portfolio_sub_type': None,
            'portfolio_category': None,
            'processing_date': None,
            'prev_processing_date': None,
            'amount_invested': None,
            'amount_invested_excl_fees': None,
            'quantity': None,
            'current_value': None,
            'previous_value': None,
            'p_l': None,
            'perc_p_l': None,
            'day_p_l': None,
            'perc_day_p_l': None,
            'average_price': None,
            'fin_invested_amount': None,
            'fin_invested_amount_excl_fees': None,
            'fin_quantity': None,
            'fin_current_amount': None,
            'fin_previous_amount': None,
            'fin_p_l': None,
            'fin_day_p_l': None,
            'fin_alloc_perc_inv_amt_typ_cd': None,
            'fin_alloc_perc_inv_amt': None,
            'fin_alloc_perc_inv_amt_excl_fees_typ_cd': None,
            'fin_alloc_perc_inv_amt_excl_fees': None,
            'fin_alloc_perc_quantity_typ_cd': None,
            'fin_alloc_perc_quantity': None,
            'fin_alloc_perc_curr_val_typ_cd': None,
            'fin_alloc_perc_curr_val': None,
            'fin_alloc_perc_prev_val_typ_cd': None,
            'fin_alloc_perc_prev_val': None,
            'fin_alloc_perc_p_l_typ_cd': None,
            'fin_alloc_perc_p_l': None,
            'fin_alloc_perc_day_p_l_typ_cd': None,
            'fin_alloc_perc_day_p_l': None,
            'next_processing_date': None
        }]

    # Consolidated Hist Allocation Portfolio
    cursor.execute('''
        SELECT 
            PORTFOLIO_TYPE
            ,PROCESSING_DATE
            ,PREV_PROCESSING_DATE
            ,AMOUNT_INVESTED_AS_ON_PROCESSING_DATE
            ,AMOUNT_INVESTED_EXCLUDING_FEES_AS_ON_PROCESSING_DATE
            ,QUANTITY
            ,CURRENT_VALUE
            ,PREVIOUS_VALUE
            ,"P/L"
            ,"DAY_P/L"
            ,AVERAGE_PRICE
            ,FIN_INVESTED_AMOUNT
            ,FIN_INVESTED_AMOUNT_EXCLUDING_FEES
            ,FIN_QUANTITY
            ,FIN_CURRENT_AMOUNT
            ,FIN_PREVIOUS_AMOUNT
            ,"FIN_P/L"
            ,"FIN_DAY_P/L"
            ,"FIN_ALLOC_%_PORTFOLIO_INVESTED_AMOUNT_TYP_CD"
            ,"FIN_ALLOC_%_PORTFOLIO_INVESTED_AMOUNT"
            ,"FIN_ALLOC_%_PORTFOLIO_INVESTED_AMOUNT_EXCLUDING_FEES_TYP_CD"
            ,"FIN_ALLOC_%_PORTFOLIO_INVESTED_AMOUNT_EXCLUDING_FEES"
            ,"FIN_ALLOC_%_QUANTITY_TYP_CD"
            ,"FIN_ALLOC_%_QUANTITY"
            ,"FIN_ALLOC_%_CURRENT_VALUE_TYP_CD"
            ,"FIN_ALLOC_%_CURRENT_VALUE"
            ,"FIN_ALLOC_%_PREVIOUS_VALUE_TYP_CD"
            ,"FIN_ALLOC_%_PREVIOUS_VALUE"
            ,"FIN_ALLOC_%_P/L_TYP_CD"
            ,"FIN_ALLOC_%_P/L"
            ,"FIN_ALLOC_%_DAY_P/L_TYP_CD"
            ,"FIN_ALLOC_%_DAY_P/L"
            ,NEXT_PROCESSING_DATE
        FROM CONSOLIDATED_HIST_ALLOCATION_PORTFOLIO
        WHERE RECORD_DELETED_FLAG = 0
        ORDER BY PROCESSING_DATE;
    ''')
    rows = cursor.fetchall()
    if rows:
        consolidated_allocation_portfolio_data = [{
            'portfolio_type': row[0],
            'processing_date': row[1],
            'prev_processing_date': row[2],
            'amount_invested': row[3],
            'amount_invested_excl_fees': row[4],
            'quantity': row[5],
            'current_value': row[6],
            'previous_value': row[7],
            'p_l': row[8],
            'day_p_l': row[9],
            'average_price': row[10],
            'fin_invested_amount': row[11],
            'fin_invested_amount_excl_fees': row[12],
            'fin_quantity': row[13],
            'fin_current_amount': row[14],
            'fin_previous_amount': row[15],
            'fin_p_l': row[16],
            'fin_day_p_l': row[17],
            'fin_alloc_perc_inv_amt_typ_cd': row[18],
            'fin_alloc_perc_inv_amt': row[19],
            'fin_alloc_perc_inv_amt_excl_fees_typ_cd': row[20],
            'fin_alloc_perc_inv_amt_excl_fees': row[21],
            'fin_alloc_perc_quantity_typ_cd': row[22],
            'fin_alloc_perc_quantity': row[23],
            'fin_alloc_perc_curr_val_typ_cd': row[24],
            'fin_alloc_perc_curr_val': row[25],
            'fin_alloc_perc_prev_val_typ_cd': row[26],
            'fin_alloc_perc_prev_val': row[27],
            'fin_alloc_perc_p_l_typ_cd': row[28],
            'fin_alloc_perc_p_l': row[29],
            'fin_alloc_perc_day_p_l_typ_cd': row[30],
            'fin_alloc_perc_day_p_l': row[31],
            'next_processing_date': row[32]
        } for row in rows]
    else:
        consolidated_allocation_portfolio_data = [{
            'portfolio_type': None,
            'processing_date': None,
            'prev_processing_date': None,
            'amount_invested': None,
            'amount_invested_excl_fees': None,
            'quantity': None,
            'current_value': None,
            'previous_value': None,
            'p_l': None,
            'day_p_l': None,
            'average_price': None,
            'fin_invested_amount': None,
            'fin_invested_amount_excl_fees': None,
            'fin_quantity': None,
            'fin_current_amount': None,
            'fin_previous_amount': None,
            'fin_p_l': None,
            'fin_day_p_l': None,
            'fin_alloc_perc_inv_amt_typ_cd': None,
            'fin_alloc_perc_inv_amt': None,
            'fin_alloc_perc_inv_amt_excl_fees_typ_cd': None,
            'fin_alloc_perc_inv_amt_excl_fees': None,
            'fin_alloc_perc_quantity_typ_cd': None,
            'fin_alloc_perc_quantity': None,
            'fin_alloc_perc_curr_val_typ_cd': None,
            'fin_alloc_perc_curr_val': None,
            'fin_alloc_perc_prev_val_typ_cd': None,
            'fin_alloc_perc_prev_val': None,
            'fin_alloc_perc_p_l_typ_cd': None,
            'fin_alloc_perc_p_l': None,
            'fin_alloc_perc_day_p_l_typ_cd': None,
            'fin_alloc_perc_day_p_l': None,
            'next_processing_date': None
        }]
    
    # Agg Consolidated Hist Allocation
    cursor.execute('''
        SELECT 
            PORTFOLIO_TYPE
            ,PORTFOLIO_NAME
            ,PORTFOLIO_HOUSE
            ,PORTFOLIO_SUB_TYPE
            ,PORTFOLIO_CATEGORY
            ,PROCESSING_DATE
            ,PREV_PROCESSING_DATE
            ,AMOUNT_INVESTED_AS_ON_PROCESSING_DATE
            ,AMOUNT_INVESTED_EXCLUDING_FEES_AS_ON_PROCESSING_DATE
            ,QUANTITY
            ,CURRENT_VALUE
            ,PREVIOUS_VALUE
            ,"P/L"
            ,"%_P/L"
            ,"DAY_P/L"
            ,"%_DAY_P/L"
            ,AVERAGE_PRICE
            ,AGG_INVESTED_AMOUNT
            ,AGG_INVESTED_AMOUNT_EXCLUDING_FEES
            ,AGG_QUANTITY
            ,AGG_CURRENT_AMOUNT
            ,AGG_PREVIOUS_AMOUNT
            ,"AGG_P/L"
            ,"AGG_DAY_P/L"
            ,"AGG_ALLOC_%_PORTFOLIO_INVESTED_AMOUNT_TYP_CD"
            ,"AGG_ALLOC_%_PORTFOLIO_INVESTED_AMOUNT"
            ,"AGG_ALLOC_%_PORTFOLIO_INVESTED_AMOUNT_EXCLUDING_FEES_TYP_CD"
            ,"AGG_ALLOC_%_PORTFOLIO_INVESTED_AMOUNT_EXCLUDING_FEES"
            ,"AGG_ALLOC_%_QUANTITY_TYP_CD"
            ,"AGG_ALLOC_%_QUANTITY"
            ,"AGG_ALLOC_%_CURRENT_VALUE_TYP_CD"
            ,"AGG_ALLOC_%_CURRENT_VALUE"
            ,"AGG_ALLOC_%_PREVIOUS_VALUE_TYP_CD"
            ,"AGG_ALLOC_%_PREVIOUS_VALUE"
            ,"AGG_ALLOC_%_P/L_TYP_CD"
            ,"AGG_ALLOC_%_P/L"
            ,"AGG_ALLOC_%_DAY_P/L_TYP_CD"
            ,"AGG_ALLOC_%_DAY_P/L"
            ,NEXT_PROCESSING_DATE
        FROM AGG_CONSOLIDATED_HIST_ALLOCATION
        WHERE RECORD_DELETED_FLAG = 0
        ORDER BY PROCESSING_DATE;
    ''')
    rows = cursor.fetchall()
    if rows:
        agg_consolidated_allocation_data = [{
            'portfolio_type': row[0],
            'portfolio_name': row[1],
            'portfolio_house': row[2],
            'portfolio_sub_type': row[3],
            'portfolio_category': row[4],
            'processing_date': row[5],
            'prev_processing_date': row[6],
            'amount_invested': row[7],
            'amount_invested_excl_fees': row[8],
            'quantity': row[9],
            'current_value': row[10],
            'previous_value': row[11],
            'p_l': row[12],
            'perc_p_l': row[13],
            'day_p_l': row[14],
            'perc_day_p_l': row[15],
            'average_price': row[16],
            'agg_invested_amount': row[17],
            'agg_invested_amount_excl_fees': row[18],
            'agg_quantity': row[19],
            'agg_current_amount': row[20],
            'agg_previous_amount': row[21],
            'agg_p_l': row[22],
            'agg_day_p_l': row[23],
            'agg_alloc_perc_inv_amt_typ_cd': row[24],
            'agg_alloc_perc_inv_amt': row[25],
            'agg_alloc_perc_inv_amt_excl_fees_typ_cd': row[26],
            'agg_alloc_perc_inv_amt_excl_fees': row[27],
            'agg_alloc_perc_quantity_typ_cd': row[28],
            'agg_alloc_perc_quantity': row[29],
            'agg_alloc_perc_curr_val_typ_cd': row[30],
            'agg_alloc_perc_curr_val': row[31],
            'agg_alloc_perc_prev_val_typ_cd': row[32],
            'agg_alloc_perc_prev_val': row[33],
            'agg_alloc_perc_p_l_typ_cd': row[34],
            'agg_alloc_perc_p_l': row[35],
            'agg_alloc_perc_day_p_l_typ_cd': row[36],
            'agg_alloc_perc_day_p_l': row[37],
            'next_processing_date': row[38]
        } for row in rows]
    else:
        agg_consolidated_allocation_data = [{
            'portfolio_type': None,
            'portfolio_name': None,
            'portfolio_house': None,
            'portfolio_sub_type': None,
            'portfolio_category': None,
            'processing_date': None,
            'prev_processing_date': None,
            'amount_invested': None,
            'amount_invested_excl_fees': None,
            'quantity': None,
            'current_value': None,
            'previous_value': None,
            'p_l': None,
            'perc_p_l': None,
            'day_p_l': None,
            'perc_day_p_l': None,
            'average_price': None,
            'agg_invested_amount': None,
            'agg_invested_amount_excl_fees': None,
            'agg_quantity': None,
            'agg_current_amount': None,
            'agg_previous_amount': None,
            'agg_p_l': None,
            'agg_day_p_l': None,
            'agg_alloc_perc_inv_amt_typ_cd': None,
            'agg_alloc_perc_inv_amt': None,
            'agg_alloc_perc_inv_amt_excl_fees_typ_cd': None,
            'agg_alloc_perc_inv_amt_excl_fees': None,
            'agg_alloc_perc_quantity_typ_cd': None,
            'agg_alloc_perc_quantity': None,
            'agg_alloc_perc_curr_val_typ_cd': None,
            'agg_alloc_perc_curr_val': None,
            'agg_alloc_perc_prev_val_typ_cd': None,
            'agg_alloc_perc_prev_val': None,
            'agg_alloc_perc_p_l_typ_cd': None,
            'agg_alloc_perc_p_l': None,
            'agg_alloc_perc_day_p_l_typ_cd': None,
            'agg_alloc_perc_day_p_l': None,
            'next_processing_date': None
        }]

    # Latest Data Fetch

    # Consolidated Hist Allocation
    cursor.execute('''
    SELECT 
         PORTFOLIO_TYPE
        ,PORTFOLIO_NAME
        ,PORTFOLIO_HOUSE
        ,PORTFOLIO_SUB_TYPE
        ,PORTFOLIO_CATEGORY
        ,PROCESSING_DATE
        ,PREV_PROCESSING_DATE
        ,AMOUNT_INVESTED_AS_ON_PROCESSING_DATE
        ,AMOUNT_INVESTED_EXCLUDING_FEES_AS_ON_PROCESSING_DATE
        ,QUANTITY
        ,CURRENT_VALUE
        ,PREVIOUS_VALUE
        ,"P/L"
        ,"%_P/L"
        ,"DAY_P/L"
        ,"%_DAY_P/L"
        ,AVERAGE_PRICE
        ,FIN_INVESTED_AMOUNT
        ,FIN_INVESTED_AMOUNT_EXCLUDING_FEES
        ,FIN_QUANTITY
        ,FIN_CURRENT_AMOUNT
        ,FIN_PREVIOUS_AMOUNT
        ,"FIN_P/L"
        ,"FIN_DAY_P/L"
        ,"FIN_ALLOC_%_PORTFOLIO_INVESTED_AMOUNT_TYP_CD"
        ,"FIN_ALLOC_%_PORTFOLIO_INVESTED_AMOUNT"
        ,"FIN_ALLOC_%_PORTFOLIO_INVESTED_AMOUNT_EXCLUDING_FEES_TYP_CD"
        ,"FIN_ALLOC_%_PORTFOLIO_INVESTED_AMOUNT_EXCLUDING_FEES"
        ,"FIN_ALLOC_%_QUANTITY_TYP_CD"
        ,"FIN_ALLOC_%_QUANTITY"
        ,"FIN_ALLOC_%_CURRENT_VALUE_TYP_CD"
        ,"FIN_ALLOC_%_CURRENT_VALUE"
        ,"FIN_ALLOC_%_PREVIOUS_VALUE_TYP_CD"
        ,"FIN_ALLOC_%_PREVIOUS_VALUE"
        ,"FIN_ALLOC_%_P/L_TYP_CD"
        ,"FIN_ALLOC_%_P/L"
        ,"FIN_ALLOC_%_DAY_P/L_TYP_CD"
        ,"FIN_ALLOC_%_DAY_P/L"
        ,NEXT_PROCESSING_DATE
    FROM CONSOLIDATED_HIST_ALLOCATION
    WHERE RECORD_DELETED_FLAG = 0
    AND PROCESSING_DATE = (SELECT MAX(PROCESSING_DATE) FROM CONSOLIDATED_HIST_ALLOCATION)
    ORDER BY PROCESSING_DATE;
''')
    rows = cursor.fetchall()
    if rows:
        latest_consolidated_allocation_data = [{
            'portfolio_type': row[0],
            'portfolio_name': row[1],
            'portfolio_house': row[2],
            'portfolio_sub_type': row[3],
            'portfolio_category': row[4],
            'processing_date': row[5],
            'prev_processing_date': row[6],
            'amount_invested': row[7],
            'amount_invested_excl_fees': row[8],
            'quantity': row[9],
            'current_value': row[10],
            'previous_value': row[11],
            'p_l': row[12],
            'perc_p_l': row[13],
            'day_p_l': row[14],
            'perc_day_p_l': row[15],
            'average_price': row[16],
            'fin_invested_amount': row[17],
            'fin_invested_amount_excl_fees': row[18],
            'fin_quantity': row[19],
            'fin_current_amount': row[20],
            'fin_previous_amount': row[21],
            'fin_p_l': row[22],
            'fin_day_p_l': row[23],
            'fin_alloc_perc_inv_amt_typ_cd': row[24],
            'fin_alloc_perc_inv_amt': row[25],
            'fin_alloc_perc_inv_amt_excl_fees_typ_cd': row[26],
            'fin_alloc_perc_inv_amt_excl_fees': row[27],
            'fin_alloc_perc_quantity_typ_cd': row[28],
            'fin_alloc_perc_quantity': row[29],
            'fin_alloc_perc_curr_val_typ_cd': row[30],
            'fin_alloc_perc_curr_val': row[31],
            'fin_alloc_perc_prev_val_typ_cd': row[32],
            'fin_alloc_perc_prev_val': row[33],
            'fin_alloc_perc_p_l_typ_cd': row[34],
            'fin_alloc_perc_p_l': row[35],
            'fin_alloc_perc_day_p_l_typ_cd': row[36],
            'fin_alloc_perc_day_p_l': row[37],
            'next_processing_date': row[38]
        } for row in rows]
    else:
        latest_consolidated_allocation_data = [{
            'portfolio_type': None,
            'portfolio_name': None,
            'portfolio_house': None,
            'portfolio_sub_type': None,
            'portfolio_category': None,
            'processing_date': None,
            'prev_processing_date': None,
            'amount_invested': None,
            'amount_invested_excl_fees': None,
            'quantity': None,
            'current_value': None,
            'previous_value': None,
            'p_l': None,
            'perc_p_l': None,
            'day_p_l': None,
            'perc_day_p_l': None,
            'average_price': None,
            'fin_invested_amount': None,
            'fin_invested_amount_excl_fees': None,
            'fin_quantity': None,
            'fin_current_amount': None,
            'fin_previous_amount': None,
            'fin_p_l': None,
            'fin_day_p_l': None,
            'fin_alloc_perc_inv_amt_typ_cd': None,
            'fin_alloc_perc_inv_amt': None,
            'fin_alloc_perc_inv_amt_excl_fees_typ_cd': None,
            'fin_alloc_perc_inv_amt_excl_fees': None,
            'fin_alloc_perc_quantity_typ_cd': None,
            'fin_alloc_perc_quantity': None,
            'fin_alloc_perc_curr_val_typ_cd': None,
            'fin_alloc_perc_curr_val': None,
            'fin_alloc_perc_prev_val_typ_cd': None,
            'fin_alloc_perc_prev_val': None,
            'fin_alloc_perc_p_l_typ_cd': None,
            'fin_alloc_perc_p_l': None,
            'fin_alloc_perc_day_p_l_typ_cd': None,
            'fin_alloc_perc_day_p_l': None,
            'next_processing_date': None
        }]

    # Consolidated Hist Allocation Portfolio
    cursor.execute('''
        SELECT 
            PORTFOLIO_TYPE
            ,PROCESSING_DATE
            ,PREV_PROCESSING_DATE
            ,AMOUNT_INVESTED_AS_ON_PROCESSING_DATE
            ,AMOUNT_INVESTED_EXCLUDING_FEES_AS_ON_PROCESSING_DATE
            ,QUANTITY
            ,CURRENT_VALUE
            ,PREVIOUS_VALUE
            ,"P/L"
            ,"DAY_P/L"
            ,AVERAGE_PRICE
            ,FIN_INVESTED_AMOUNT
            ,FIN_INVESTED_AMOUNT_EXCLUDING_FEES
            ,FIN_QUANTITY
            ,FIN_CURRENT_AMOUNT
            ,FIN_PREVIOUS_AMOUNT
            ,"FIN_P/L"
            ,"FIN_DAY_P/L"
            ,"FIN_ALLOC_%_PORTFOLIO_INVESTED_AMOUNT_TYP_CD"
            ,"FIN_ALLOC_%_PORTFOLIO_INVESTED_AMOUNT"
            ,"FIN_ALLOC_%_PORTFOLIO_INVESTED_AMOUNT_EXCLUDING_FEES_TYP_CD"
            ,"FIN_ALLOC_%_PORTFOLIO_INVESTED_AMOUNT_EXCLUDING_FEES"
            ,"FIN_ALLOC_%_QUANTITY_TYP_CD"
            ,"FIN_ALLOC_%_QUANTITY"
            ,"FIN_ALLOC_%_CURRENT_VALUE_TYP_CD"
            ,"FIN_ALLOC_%_CURRENT_VALUE"
            ,"FIN_ALLOC_%_PREVIOUS_VALUE_TYP_CD"
            ,"FIN_ALLOC_%_PREVIOUS_VALUE"
            ,"FIN_ALLOC_%_P/L_TYP_CD"
            ,"FIN_ALLOC_%_P/L"
            ,"FIN_ALLOC_%_DAY_P/L_TYP_CD"
            ,"FIN_ALLOC_%_DAY_P/L"
            ,NEXT_PROCESSING_DATE
        FROM CONSOLIDATED_HIST_ALLOCATION_PORTFOLIO
        WHERE RECORD_DELETED_FLAG = 0
        AND PROCESSING_DATE = (SELECT MAX(PROCESSING_DATE) FROM CONSOLIDATED_HIST_ALLOCATION_PORTFOLIO)
        ORDER BY PROCESSING_DATE;
    ''')
    rows = cursor.fetchall()
    if rows:
        latest_consolidated_allocation_portfolio_data = [{
            'portfolio_type': row[0],
            'processing_date': row[1],
            'prev_processing_date': row[2],
            'amount_invested': row[3],
            'amount_invested_excl_fees': row[4],
            'quantity': row[5],
            'current_value': row[6],
            'previous_value': row[7],
            'p_l': row[8],
            'day_p_l': row[9],
            'average_price': row[10],
            'fin_invested_amount': row[11],
            'fin_invested_amount_excl_fees': row[12],
            'fin_quantity': row[13],
            'fin_current_amount': row[14],
            'fin_previous_amount': row[15],
            'fin_p_l': row[16],
            'fin_day_p_l': row[17],
            'fin_alloc_perc_inv_amt_typ_cd': row[18],
            'fin_alloc_perc_inv_amt': row[19],
            'fin_alloc_perc_inv_amt_excl_fees_typ_cd': row[20],
            'fin_alloc_perc_inv_amt_excl_fees': row[21],
            'fin_alloc_perc_quantity_typ_cd': row[22],
            'fin_alloc_perc_quantity': row[23],
            'fin_alloc_perc_curr_val_typ_cd': row[24],
            'fin_alloc_perc_curr_val': row[25],
            'fin_alloc_perc_prev_val_typ_cd': row[26],
            'fin_alloc_perc_prev_val': row[27],
            'fin_alloc_perc_p_l_typ_cd': row[28],
            'fin_alloc_perc_p_l': row[29],
            'fin_alloc_perc_day_p_l_typ_cd': row[30],
            'fin_alloc_perc_day_p_l': row[31],
            'next_processing_date': row[32]
        } for row in rows]
    else:
        latest_consolidated_allocation_portfolio_data = [{
            'portfolio_type': None,
            'processing_date': None,
            'prev_processing_date': None,
            'amount_invested': None,
            'amount_invested_excl_fees': None,
            'quantity': None,
            'current_value': None,
            'previous_value': None,
            'p_l': None,
            'day_p_l': None,
            'average_price': None,
            'fin_invested_amount': None,
            'fin_invested_amount_excl_fees': None,
            'fin_quantity': None,
            'fin_current_amount': None,
            'fin_previous_amount': None,
            'fin_p_l': None,
            'fin_day_p_l': None,
            'fin_alloc_perc_inv_amt_typ_cd': None,
            'fin_alloc_perc_inv_amt': None,
            'fin_alloc_perc_inv_amt_excl_fees_typ_cd': None,
            'fin_alloc_perc_inv_amt_excl_fees': None,
            'fin_alloc_perc_quantity_typ_cd': None,
            'fin_alloc_perc_quantity': None,
            'fin_alloc_perc_curr_val_typ_cd': None,
            'fin_alloc_perc_curr_val': None,
            'fin_alloc_perc_prev_val_typ_cd': None,
            'fin_alloc_perc_prev_val': None,
            'fin_alloc_perc_p_l_typ_cd': None,
            'fin_alloc_perc_p_l': None,
            'fin_alloc_perc_day_p_l_typ_cd': None,
            'fin_alloc_perc_day_p_l': None,
            'next_processing_date': None
        }]
    
    # Agg Consolidated Hist Allocation
    cursor.execute('''
        SELECT 
            PORTFOLIO_TYPE
            ,PORTFOLIO_NAME
            ,PORTFOLIO_HOUSE
            ,PORTFOLIO_SUB_TYPE
            ,PORTFOLIO_CATEGORY
            ,PROCESSING_DATE
            ,PREV_PROCESSING_DATE
            ,AMOUNT_INVESTED_AS_ON_PROCESSING_DATE
            ,AMOUNT_INVESTED_EXCLUDING_FEES_AS_ON_PROCESSING_DATE
            ,QUANTITY
            ,CURRENT_VALUE
            ,PREVIOUS_VALUE
            ,"P/L"
            ,"%_P/L"
            ,"DAY_P/L"
            ,"%_DAY_P/L"
            ,AVERAGE_PRICE
            ,AGG_INVESTED_AMOUNT
            ,AGG_INVESTED_AMOUNT_EXCLUDING_FEES
            ,AGG_QUANTITY
            ,AGG_CURRENT_AMOUNT
            ,AGG_PREVIOUS_AMOUNT
            ,"AGG_P/L"
            ,"AGG_DAY_P/L"
            ,"AGG_ALLOC_%_PORTFOLIO_INVESTED_AMOUNT_TYP_CD"
            ,"AGG_ALLOC_%_PORTFOLIO_INVESTED_AMOUNT"
            ,"AGG_ALLOC_%_PORTFOLIO_INVESTED_AMOUNT_EXCLUDING_FEES_TYP_CD"
            ,"AGG_ALLOC_%_PORTFOLIO_INVESTED_AMOUNT_EXCLUDING_FEES"
            ,"AGG_ALLOC_%_QUANTITY_TYP_CD"
            ,"AGG_ALLOC_%_QUANTITY"
            ,"AGG_ALLOC_%_CURRENT_VALUE_TYP_CD"
            ,"AGG_ALLOC_%_CURRENT_VALUE"
            ,"AGG_ALLOC_%_PREVIOUS_VALUE_TYP_CD"
            ,"AGG_ALLOC_%_PREVIOUS_VALUE"
            ,"AGG_ALLOC_%_P/L_TYP_CD"
            ,"AGG_ALLOC_%_P/L"
            ,"AGG_ALLOC_%_DAY_P/L_TYP_CD"
            ,"AGG_ALLOC_%_DAY_P/L"
            ,NEXT_PROCESSING_DATE
        FROM AGG_CONSOLIDATED_HIST_ALLOCATION
        WHERE RECORD_DELETED_FLAG = 0
        AND PROCESSING_DATE = (SELECT MAX(PROCESSING_DATE) FROM AGG_CONSOLIDATED_HIST_ALLOCATION)   
        ORDER BY PROCESSING_DATE;
    ''')
    rows = cursor.fetchall()
    if rows:
        latest_agg_consolidated_allocation_data = [{
            'portfolio_type': row[0],
            'portfolio_name': row[1],
            'portfolio_house': row[2],
            'portfolio_sub_type': row[3],
            'portfolio_category': row[4],
            'processing_date': row[5],
            'prev_processing_date': row[6],
            'amount_invested': row[7],
            'amount_invested_excl_fees': row[8],
            'quantity': row[9],
            'current_value': row[10],
            'previous_value': row[11],
            'p_l': row[12],
            'perc_p_l': row[13],
            'day_p_l': row[14],
            'perc_day_p_l': row[15],
            'average_price': row[16],
            'agg_invested_amount': row[17],
            'agg_invested_amount_excl_fees': row[18],
            'agg_quantity': row[19],
            'agg_current_amount': row[20],
            'agg_previous_amount': row[21],
            'agg_p_l': row[22],
            'agg_day_p_l': row[23],
            'agg_alloc_perc_inv_amt_typ_cd': row[24],
            'agg_alloc_perc_inv_amt': row[25],
            'agg_alloc_perc_inv_amt_excl_fees_typ_cd': row[26],
            'agg_alloc_perc_inv_amt_excl_fees': row[27],
            'agg_alloc_perc_quantity_typ_cd': row[28],
            'agg_alloc_perc_quantity': row[29],
            'agg_alloc_perc_curr_val_typ_cd': row[30],
            'agg_alloc_perc_curr_val': row[31],
            'agg_alloc_perc_prev_val_typ_cd': row[32],
            'agg_alloc_perc_prev_val': row[33],
            'agg_alloc_perc_p_l_typ_cd': row[34],
            'agg_alloc_perc_p_l': row[35],
            'agg_alloc_perc_day_p_l_typ_cd': row[36],
            'agg_alloc_perc_day_p_l': row[37],
            'next_processing_date': row[38]
        } for row in rows]
    else:
        latest_agg_consolidated_allocation_data = [{
            'portfolio_type': None,
            'portfolio_name': None,
            'portfolio_house': None,
            'portfolio_sub_type': None,
            'portfolio_category': None,
            'processing_date': None,
            'prev_processing_date': None,
            'amount_invested': None,
            'amount_invested_excl_fees': None,
            'quantity': None,
            'current_value': None,
            'previous_value': None,
            'p_l': None,
            'perc_p_l': None,
            'day_p_l': None,
            'perc_day_p_l': None,
            'average_price': None,
            'agg_invested_amount': None,
            'agg_invested_amount_excl_fees': None,
            'agg_quantity': None,
            'agg_current_amount': None,
            'agg_previous_amount': None,
            'agg_p_l': None,
            'agg_day_p_l': None,
            'agg_alloc_perc_inv_amt_typ_cd': None,
            'agg_alloc_perc_inv_amt': None,
            'agg_alloc_perc_inv_amt_excl_fees_typ_cd': None,
            'agg_alloc_perc_inv_amt_excl_fees': None,
            'agg_alloc_perc_quantity_typ_cd': None,
            'agg_alloc_perc_quantity': None,
            'agg_alloc_perc_curr_val_typ_cd': None,
            'agg_alloc_perc_curr_val': None,
            'agg_alloc_perc_prev_val_typ_cd': None,
            'agg_alloc_perc_prev_val': None,
            'agg_alloc_perc_p_l_typ_cd': None,
            'agg_alloc_perc_p_l': None,
            'agg_alloc_perc_day_p_l_typ_cd': None,
            'agg_alloc_perc_day_p_l': None,
            'next_processing_date': None
        }]

    data = {'consolidated_allocation_data':                   consolidated_allocation_data,
            'consolidated_allocation_portfolio_data' :        consolidated_allocation_portfolio_data,
            'agg_consolidated_allocation_data':               agg_consolidated_allocation_data,
            'latest_consolidated_allocation_data':            latest_consolidated_allocation_data,
            'latest_consolidated_allocation_portfolio_data' : latest_consolidated_allocation_portfolio_data,
            'latest_agg_consolidated_allocation_data':        latest_agg_consolidated_allocation_data}
    return jsonify(data)

def create_simulated_portfolio_table():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS SIMULATED_RETURNS (
            ID INTEGER PRIMARY KEY AUTOINCREMENT,
            SIM_FUND_NAME TEXT,
            SIM_BASE_TYPE TEXT,
            SIM_BASE_NAME TEXT,
            SIM_ALLOCATION_CATEGORY TEXT,
            SIM_PURCHASE_DATE DATE,
            SIM_NAV_DURING_PURCHASE REAL,
            SIM_HOLDING_DAYS REAL,
            SIM_CURRENT_NAV REAL,
            SIM_INVESTED_AMOUNT REAL,
            SIM_FUND_UNITS REAL,
            SIM_CURRENT_AMOUNT REAL,
            "SIM_P/L" REAL,
            "SIM_%_P/L" REAL,
            SIM_PREVIOUS_NAV REAL,
            SIM_PREVIOUS_AMOUNT REAL,
            "SIM_DAY_P/L" REAL,
            "SIM_%_DAY_P/L" REAL,
            PROCESSING_DATE DATE,
            PREVIOUS_PROCESSING_DATE DATE,
            NEXT_PROCESSING_DATE DATE,
            UPDATE_PROCESS_NAME TEXT,
            UPDATE_PROCESS_ID INTEGER,
            PROCESS_NAME TEXT,
            PROCESS_ID INTEGER,
            START_DATE DATE,
            END_DATE DATE,
            RECORD_DELETED_FLAG INTEGER
);''')

def create_agg_simulated_portfolio_table():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS AGG_SIMULATED_RETURNS (
            ID INTEGER PRIMARY KEY AUTOINCREMENT,
            AGG_SIM_FUND_NAME TEXT,
            AGG_SIM_BASE_TYPE TEXT,
            AGG_SIM_ALLOCATION_CATEGORY TEXT,
            AGG_SIM_INVESTED_AMOUNT REAL,
            AGG_SIM_FUND_UNITS REAL,
            AGG_SIM_CURRENT_AMOUNT REAL,
            AGG_SIM_PREVIOUS_AMOUNT REAL,
            "AGG_SIM_P/L" REAL,
            "AGG_SIM_DAY_P/L" REAL,
            "AGG_%_SIM_P/L" REAL,
            "AGG_%_SIM_DAY_P/L" REAL,
            AGG_SIM_AVG_PRICE REAL,
            PROCESSING_DATE DATE,
            PREVIOUS_PROCESSING_DATE DATE,
            NEXT_PROCESSING_DATE DATE,
            UPDATE_PROCESS_NAME TEXT,
            UPDATE_PROCESS_ID INTEGER,
            PROCESS_NAME TEXT,
            PROCESS_ID INTEGER,
            START_DATE DATE,
            END_DATE DATE,
            RECORD_DELETED_FLAG INTEGER
);''')

def create_fin_simulated_portfolio_table():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS FIN_SIMULATED_RETURNS (
            ID INTEGER PRIMARY KEY AUTOINCREMENT,
            FIN_SIM_FUND_NAME TEXT,
            FIN_SIM_ALLOCATION_CATEGORY TEXT,
            FIN_SIM_INVESTED_AMOUNT REAL,
            FIN_SIM_FUND_UNITS REAL,
            FIN_SIM_CURRENT_AMOUNT REAL,
            FIN_SIM_PREVIOUS_AMOUNT REAL,
            "FIN_SIM_P/L" REAL,
            "FIN_SIM_DAY_P/L" REAL,
            "FIN_%_SIM_P/L" REAL,
            "FIN_%_SIM_DAY_P/L" REAL,
            FIN_SIM_AVG_PRICE REAL,
            PROCESSING_DATE DATE,
            PREVIOUS_PROCESSING_DATE DATE,
            NEXT_PROCESSING_DATE DATE,
            UPDATE_PROCESS_NAME TEXT,
            UPDATE_PROCESS_ID INTEGER,
            PROCESS_NAME TEXT,
            PROCESS_ID INTEGER,
            START_DATE DATE,
            END_DATE DATE,
            RECORD_DELETED_FLAG INTEGER
);''')

def get_simulated_returns_from_fin_simulated_returns_table():
    simulated_returns_dict = fetch_queries_as_dictionaries('''
SELECT
    CONS.PROCESSING_DATE
    ,CONS."%_P/L"
    ,CONS."%_DAY_P/L"
    ,SIM."FIN_%_SIM_P/L"
    ,SIM."FIN_%_SIM_DAY_P/L"
FROM
    CONSOLIDATED_HIST_RETURNS CONS
INNER JOIN
    FIN_SIMULATED_RETURNS SIM
ON
    CONS.PROCESSING_DATE = SIM.PROCESSING_DATE
WHERE
    CONS.RECORD_DELETED_FLAG = 0
    AND SIM.RECORD_DELETED_FLAG = 0
ORDER BY CONS.PROCESSING_DATE;''')
    return jsonify(simulated_returns_dict)

def create_mutual_fund_returns_table():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS MUTUAL_FUND_RETURNS (
            FUND_NAME TEXT,
            FUND_AMC TEXT,
            FUND_TYPE TEXT,
            FUND_CATEGORY TEXT,
            ALLOCATION_CATEGORY TEXT,
            FUND_PURCHASE_DATE DATE,
            NAV_DURING_PURCHASE REAL,
            HOLDING_DAYS REAL,
            CURRENT_NAV REAL,
            INVESTED_AMOUNT REAL,
            AMC_AMOUNT REAL,
            STAMP_FEES_AMOUNT REAL,
            FUND_UNITS REAL,
            CURRENT_AMOUNT REAL,
            "P/L" REAL,
            "%_P/L" REAL,
            PREVIOUS_NAV REAL,
            PREVIOUS_AMOUNT REAL,
            "DAY_P/L" REAL,
            "%_DAY_P/L" REAL,
            PROCESSING_DATE DATE,
            PREVIOUS_PROCESSING_DATE DATE,
            NEXT_PROCESSING_DATE DATE,
            UPDATE_PROCESS_NAME TEXT,
            UPDATE_PROCESS_ID INTEGER,
            PROCESS_NAME TEXT,
            PROCESS_ID INTEGER,
            START_DATE DATE,
            END_DATE DATE,
            RECORD_DELETED_FLAG INTEGER
);''')

def create_agg_mutual_fund_returns_table():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS AGG_MUTUAL_FUND_RETURNS (
            FUND_NAME TEXT,
            FUND_AMC TEXT,
            FUND_TYPE TEXT,
            FUND_CATEGORY TEXT,
            ALLOCATION_CATEGORY TEXT,
            AGG_INVESTED_AMOUNT REAL,
            AGG_AMC_AMOUNT REAL,
            AGG_FUND_UNITS REAL,
            AGG_CURRENT_AMOUNT REAL,
            AGG_PREVIOUS_AMOUNT REAL,
            "AGG_P/L" REAL,
            "AGG_DAY_P/L" REAL,
            "AGG_%_P/L" REAL,
            "AGG_%_DAY_P/L" REAL,
            AGG_AVG_PRICE REAL,
            PROCESSING_DATE DATE,
            PREVIOUS_PROCESSING_DATE DATE,
            NEXT_PROCESSING_DATE DATE,
            UPDATE_PROCESS_NAME TEXT,
            UPDATE_PROCESS_ID INTEGER,
            PROCESS_NAME TEXT,
            PROCESS_ID INTEGER,
            START_DATE DATE,
            END_DATE DATE,
            RECORD_DELETED_FLAG INTEGER
);''')

def create_fin_mutual_fund_returns_table():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS FIN_MUTUAL_FUND_RETURNS (
            "FIN_P/L" REAL,
            FIN_AMC_AMOUNT REAL,
            FIN_CURRENT_AMOUNT REAL,
            FIN_PREVIOUS_AMOUNT REAL,
            "FIN_%_P/L" REAL,
            "FIN_DAY_P/L" REAL,
            "FIN_%_DAY_P/L" REAL,
            PROCESSING_DATE DATE,
            PREVIOUS_PROCESSING_DATE DATE,
            NEXT_PROCESSING_DATE DATE,
            UPDATE_PROCESS_NAME TEXT,
            UPDATE_PROCESS_ID INTEGER,
            PROCESS_NAME TEXT,
            PROCESS_ID INTEGER,
            START_DATE DATE,
            END_DATE DATE,
            RECORD_DELETED_FLAG INTEGER
);''')

def create_unrealised_stock_returns_table():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS UNREALISED_STOCK_RETURNS (
            STOCK_NAME TEXT,
            ALLOCATION_CATEGORY TEXT,
            TRADE_DATE DATE,
            STOCK_QUANTITY REAL,
            TRADE_PRICE REAL,
            CURRENT_PRICE REAL,
            INVESTED_AMOUNT REAL,
            TOTAL_FEES REAL,
            TOTAL_INVESTED_AMOUNT REAL,
            CURRENT_VALUE REAL,
            "P/L" REAL,
            "%_P/L" REAL,
            "NET_P/L" REAL,
            "%_NET_P/L" REAL,
            PREVIOUS_PRICE REAL,
            PREVIOUS_VALUE REAL,
            "DAY_P/L" REAL,
            "%_DAY_P/L" REAL,
            PROCESSING_DATE DATE,
            PREVIOUS_PROCESSING_DATE DATE,
            NEXT_PROCESSING_DATE DATE,
            UPDATE_PROCESS_NAME TEXT,
            UPDATE_PROCESS_ID INTEGER,
            PROCESS_NAME TEXT,
            PROCESS_ID INTEGER,
            START_DATE DATE,
            END_DATE DATE,
            RECORD_DELETED_FLAG INTEGER
);''')

def create_agg_unrealised_stock_returns_table():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS AGG_UNREALISED_STOCK_RETURNS (
            STOCK_NAME TEXT,
            ALLOCATION_CATEGORY TEXT,
            CURRENT_PRICE REAL,
            PREVIOUS_PRICE REAL,
            AGG_STOCK_QUANTITY REAL,
            AVG_TRADE_PRICE REAL,
            AGG_INVESTED_AMOUNT REAL,
            AGG_TOTAL_FEES REAL,
            AGG_TOTAL_INVESTED_AMOUNT REAL,
            AGG_CURRENT_VALUE REAL,
            "AGG_P/L" REAL,
            "AGG_%_P/L" REAL,
            "AGG_NET_P/L" REAL,
            "AGG_%_NET_P/L" REAL,
            AGG_PREVIOUS_VALUE REAL,
            "AGG_DAY_P/L" REAL,
            "AGG_%_DAY_P/L" REAL,
            PROCESSING_DATE DATE,
            PREVIOUS_PROCESSING_DATE DATE,
            NEXT_PROCESSING_DATE DATE,
            UPDATE_PROCESS_NAME TEXT,
            UPDATE_PROCESS_ID INTEGER,
            PROCESS_NAME TEXT,
            PROCESS_ID INTEGER,
            START_DATE DATE,
            END_DATE DATE,
            RECORD_DELETED_FLAG INTEGER
);''')

def create_fin_unrealised_stock_returns_table():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS FIN_UNREALISED_STOCK_RETURNS (
            FIN_INVESTED_AMOUNT REAL,
            FIN_TOTAL_FEES REAL,
            FIN_TOTAL_INVESTED_AMOUNT REAL,
            FIN_CURRENT_VALUE REAL,
            FIN_PREVIOUS_VALUE REAL,
            "FIN_P/L" REAL,
            "FIN_%_P/L" REAL,
            "FIN_NET_P/L" REAL,
            "FIN_NET_%_P/L" REAL,
            "FIN_DAY_P/L" REAL,
            "FIN_%_DAY_P/L" REAL,
            PROCESSING_DATE DATE,
            PREVIOUS_PROCESSING_DATE DATE,
            NEXT_PROCESSING_DATE DATE,
            UPDATE_PROCESS_NAME TEXT,
            UPDATE_PROCESS_ID INTEGER,
            PROCESS_NAME TEXT,
            PROCESS_ID INTEGER,
            START_DATE DATE,
            END_DATE DATE,
            RECORD_DELETED_FLAG INTEGER
);''')