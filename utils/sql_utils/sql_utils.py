import sqlite3, os
from flask import  jsonify
import datetime

db_path = os.path.join(os.getcwd(), "databases", "consolidated_portfolio.db")

if not os.path.exists(os.path.dirname(db_path)):
    os.makedirs(os.path.dirname(db_path))

def create_table(alt_symbol):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS ''' + alt_symbol + ''' (
            ID INTEGER PRIMARY KEY AUTOINCREMENT,
            VALUE_DATE DATE,
            PRICE REAL,
            START_DATE,
            END_DATE,
            RECORD_DELETED_FLAG
        )
    ''')

def insert_into_nav_table(alt_symbol, date, price, start_date, end_date, record_deleted_flag):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(f"SELECT VALUE_DATE, PRICE FROM {alt_symbol} WHERE VALUE_DATE = '{start_date}';")
    rows = cursor.fetchall()
    if rows:
        for row in rows:
            date_from_query = row[0]
            price_from_query = row[1]
            if date_from_query == date and price_from_query != price:
                cursor.execute(f"UPDATE {alt_symbol} SET END_DATE = '{start_date}', RECORD_DELETED_FLAG = 1 WHERE VALUE_DATE = '{start_date}';")
                cursor.execute("INSERT INTO " + alt_symbol + "(VALUE_DATE, PRICE, START_DATE, END_DATE, RECORD_DELETED_FLAG) VALUES (?, ?, ?, ?, ?)",
                                (date, price, start_date, end_date, record_deleted_flag ))
                conn.commit()
    else:
        cursor.execute("INSERT INTO " + alt_symbol + "(VALUE_DATE, PRICE, START_DATE, END_DATE, RECORD_DELETED_FLAG) VALUES (?, ?, ?, ?, ?)",
        (date, price, start_date, end_date, record_deleted_flag ))
        conn.commit()
    conn.close()

def truncate_table(alt_symbol):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('''
        DROP TABLE IF EXISTS ''' + alt_symbol);

def create_metadata_table():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS METADATA (
            ID INTEGER PRIMARY KEY AUTOINCREMENT,
            SYMBOL TEXT(100),
            NAME TEXT(200),
            AMC TEXT(50),
            MF_TYPE TEXT(100),
            FUND_CATEGORY TEXT(100),
            LAUNCHED_ON DATE,
            EXIT_LOAD NUMERIC(2,2),
            EXPENSE_RATIO NUMERIC(2,2),
            FUND_MANAGER TEXT(100),
            FUND_MANAGER_STARTED_ON DATE,
            START_DATE DATE,
            END_DATE DATE,
            RECORD_DELETED_FLAG INTEGER
        )''')

def insert_metadata_entry(symbol, alt_symbol, amc, mf_type, fund_category, launched_on, exit_load, expense_ratio, fund_manager, fund_manager_started_on):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("INSERT INTO METADATA (SYMBOL, NAME, AMC, MF_TYPE, FUND_CATEGORY, LAUNCHED_ON, EXIT_LOAD, EXPENSE_RATIO, FUND_MANAGER, FUND_MANAGER_STARTED_ON, START_DATE, END_DATE, RECORD_DELETED_FLAG) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                   (symbol, alt_symbol, amc, mf_type, fund_category, launched_on, exit_load, expense_ratio, fund_manager, fund_manager_started_on, '2025-05-11', '9998-12-31', 0  ))
    conn.commit()
    conn.close()

def get_name_from_metadata():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT DISTINCT NAME FROM METADATA ORDER BY NAME")
    rows = cursor.fetchall()
    conn.close()
    if rows:
        name_list = [{'name': row[0]} for row in rows]
        return jsonify(name_list)

def get_symbol_from_metadata(alt_symbol):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(f"SELECT DISTINCT SYMBOL FROM METADATA WHERE NAME = '{alt_symbol}'")
    rows = cursor.fetchall()
    conn.close()
    if rows:
        symbol_list = [{'symbol': row[0]} for row in rows]
        return jsonify(symbol_list)
    return

def get_nav_from_hist_table(table_name, purchase_date):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(f"SELECT DISTINCT PRICE FROM {table_name} WHERE VALUE_DATE = '{purchase_date}'")
    rows = cursor.fetchall()
    conn.close()
    if rows:
        nav = {'nav': rows[0]}
        return jsonify(nav)
    
def create_portfolio_order_table():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS PORTFOLIO_ORDER (
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
    
def insert_portfolio_order_entry(alt_symbol, purchase_date, invested_amount, stamp_fees_amount, amc_amount, nav_during_purchase, units):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("INSERT INTO PORTFOLIO_ORDER (NAME, PURCHASED_ON, INVESTED_AMOUNT, STAMP_FEES_AMOUNT, AMC_AMOUNT, NAV_DURING_PURCHASE, UNITS, START_DATE, END_DATE, RECORD_DELETED_FLAG) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                   (alt_symbol, purchase_date, invested_amount, stamp_fees_amount, amc_amount, nav_during_purchase, units, purchase_date, '9998-12-31', 0  ))
    conn.commit()
    conn.close()

def get_proc_date_from_processing_date_table():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(f"SELECT DISTINCT CURRENT_DATE, MF_PROC_DATE, PPFS_MF_PROC_DATE, STOCK_PROC_DATE FROM PROCESSING_DATE;")
    rows = cursor.fetchall()
    conn.close()
    if rows:
        dates = {'current_date': rows[0][0], 'mf_proc_date': rows[0][1], 'ppfs_mf_proc_date': rows[0][2], 'stock_proc_date' : rows[0][3]}
        return jsonify(dates)

def create_processing_date_table():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS PROCESSING_DATE (
            ID INTEGER PRIMARY KEY AUTOINCREMENT,
            CURRENT_DATE DATE,
            MF_PROC_DATE DATE,
            PPFS_MF_PROC_DATE DATE,
            STOCK_PROC_DATE DATE
        )''')
    cursor.execute(f"SELECT COUNT(*) FROM PROCESSING_DATE;")
    rows = cursor.fetchall()
    if rows:
        count = rows[0][0]
    if count == 0:
        cursor.execute("INSERT INTO PROCESSING_DATE (CURRENT_DATE, MF_PROC_DATE, PPFS_MF_PROC_DATE, STOCK_PROC_DATE) VALUES (?, ?, ?, ?)",
                   (datetime.date.today(), datetime.date.today(), datetime.date.today(), datetime.date.today()))
        conn.commit()
    conn.close()

def update_proc_date_in_processing_date_table(current_date, mf_proc_date, ppfs_mf_proc_date, stock_proc_date):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(f"UPDATE PROCESSING_DATE SET CURRENT_DATE = '{current_date}', MF_PROC_DATE = '{mf_proc_date}', PPFS_MF_PROC_DATE = '{ppfs_mf_proc_date}', STOCK_PROC_DATE = '{stock_proc_date}';")
    conn.commit()
    conn.close()

def get_tables_list():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(f"SELECT DISTINCT NAME FROM SQLITE_MASTER WHERE TYPE = 'table' AND NAME NOT IN ('sqlite_sequence', 'METADATA', 'PORTFOLIO_ORDER', 'PROCESSING_DATE');")
    rows = cursor.fetchall()
    conn.close()
    tables_list = []
    if rows:
        for row in rows:
            tables_list.append(row)
        return jsonify(tables_list)
    return
    

def get_max_date_from_table(table):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(f"SELECT MAX(VALUE_DATE) FROM {table};")
    rows = cursor.fetchall()
    conn.close()
    if rows:
        data = {'max_date' : rows[0][0]}
        return jsonify(data)
    return

def dup_check_on_nav_table(table):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(f"SELECT VALUE_DATE, COUNT(*) C FROM {table} WHERE RECORD_DELETED_FLAG = 0 GROUP BY 1 HAVING C > 1;")
    rows = cursor.fetchall()
    conn.close()
    if rows:
        data = {'value_date' : rows[0][0]}
        return jsonify(data)
    return