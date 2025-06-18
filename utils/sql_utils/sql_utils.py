import sqlite3, os
from flask import  jsonify
import datetime
from utils.sql_utils.views.MUTUAL_FUND_PORTFOLIO_VIEW import MUTUAL_FUND_PORTFOLIO_VIEW
from utils.sql_utils.views.AGG_MUTUAL_FUND_PORTFOLIO_VIEW import AGG_MUTUAL_FUND_PORTFOLIO_VIEW
from utils.sql_utils.views.FIN_MUTUAL_FUND_PORTFOLIO_VIEW import FIN_MUTUAL_FUND_PORTFOLIO_VIEW
from utils.sql_utils.views.STOCK_REALISED_PORTFOLIO_VIEW import STOCK_REALISED_PORTFOLIO_VIEW
from utils.sql_utils.views.AGG_STOCK_REALISED_PORTFOLIO_VIEW import AGG_STOCK_REALISED_PORTFOLIO_VIEW
from utils.sql_utils.views.FIN_STOCK_REALISED_PORTFOLIO_VIEW import FIN_STOCK_REALISED_PORTFOLIO_VIEW

db_path = os.path.join(os.getcwd(), "databases", "consolidated_portfolio.db")

if not os.path.exists(os.path.dirname(db_path)):
    os.makedirs(os.path.dirname(db_path))

def create_table(alt_symbol):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(f'''
        CREATE TABLE IF NOT EXISTS "{alt_symbol}" (
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
    cursor.execute(f"""SELECT VALUE_DATE, PRICE FROM "{alt_symbol}" WHERE VALUE_DATE = '{start_date}' AND RECORD_DELETED_FLAG = 0;""")
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
        cursor.execute(f'INSERT INTO "{alt_symbol}" (VALUE_DATE, PRICE, START_DATE, END_DATE, RECORD_DELETED_FLAG) VALUES (?, ?, ?, ?, ?)',
        (date, price, start_date, end_date, record_deleted_flag ))
        conn.commit()
    conn.close()

def truncate_table(alt_symbol):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(f'DROP TABLE IF EXISTS "{alt_symbol}');

def create_metadata_table():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS METADATA (
            ID INTEGER PRIMARY KEY AUTOINCREMENT,
            SYMBOL TEXT(100),
            NAME TEXT(200),
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
            START_DATE DATE,
            END_DATE DATE,
            RECORD_DELETED_FLAG INTEGER
        )''')

def insert_metadata_entry(symbol, alt_symbol, portfolio_type, amc, mf_type, fund_category, launched_on, exit_load, expense_ratio, fund_manager, fund_manager_started_on, isin):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    today = datetime.date.today()
    today = today.strftime("%Y-%m-%d")
    cursor.execute("INSERT INTO METADATA (SYMBOL, NAME, PORTFOLIO_TYPE, AMC, MF_TYPE, FUND_CATEGORY, LAUNCHED_ON, EXIT_LOAD, EXPENSE_RATIO, FUND_MANAGER, FUND_MANAGER_STARTED_ON, ISIN, PROCESS_FLAG, START_DATE, END_DATE, RECORD_DELETED_FLAG) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                   (symbol, alt_symbol, portfolio_type, amc, mf_type, fund_category, launched_on, exit_load, expense_ratio, fund_manager, fund_manager_started_on, isin, 1, today, '9998-12-31', 0  ))
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

def get_all_names_from_metadata():
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
        conn.commit()
    conn.close()

def update_proc_date_in_processing_date_table(proc_typ_cd, proc_date, next_proc_date, prev_proc_date):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(f"UPDATE PROCESSING_DATE SET PROC_DATE = '{proc_date}', NEXT_PROC_DATE = '{next_proc_date}', PREV_PROC_DATE = '{prev_proc_date}' WHERE PROC_TYP_CD = '{proc_typ_cd}';")
    conn.commit()
    conn.close()

def get_tables_list():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT DISTINCT NAME FROM METADATA WHERE PROCESS_FLAG = 1 ORDER BY NAME")
    rows = cursor.fetchall()
    conn.close()
    tables_list = []
    if rows:
        for row in rows:
            tables_list.append(row)
        return jsonify(tables_list)
    return

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
    
def get_max_date_from_table(table):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(f'SELECT MAX(VALUE_DATE) FROM "{table}";')
    rows = cursor.fetchall()
    conn.close()
    if rows:
        data = {'max_date' : rows[0][0]}
        return jsonify(data)
    return

def dup_check_on_nav_table(table):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(f'SELECT VALUE_DATE, COUNT(*) C FROM "{table}" WHERE RECORD_DELETED_FLAG = 0 GROUP BY 1 HAVING C > 1;')
    rows = cursor.fetchall()
    conn.close()
    if rows:
        data = {'value_date' : rows[0][0]}
        return jsonify(data)
    return

def create_mf_portfolio_view_in_db():
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

def create_stock_portfolio_view_in_db():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("DROP VIEW IF EXISTS STOCK_REALISED_PORTFOLIO_VIEW;")
    cursor.execute(STOCK_REALISED_PORTFOLIO_VIEW)
    cursor.execute("DROP VIEW IF EXISTS AGG_STOCK_REALISED_PORTFOLIO_VIEW;")
    cursor.execute(AGG_STOCK_REALISED_PORTFOLIO_VIEW)
    cursor.execute("DROP VIEW IF EXISTS FIN_STOCK_REALISED_PORTFOLIO_VIEW;")
    cursor.execute(FIN_STOCK_REALISED_PORTFOLIO_VIEW)
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
    
def insert_into_holiday_date_table(holiday_date, holiday_name, holiday_day):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("INSERT INTO HOLIDAY_DATES (HOLIDAY_DATE, HOLIDAY_NAME, HOLIDAY_DAY,  START_DATE, END_DATE, RECORD_DELETED_FLAG) VALUES (?, ?, ?, ?, ?, ?)",
                   (holiday_date, holiday_name, holiday_day, holiday_date, '9998-12-31', 0))
    conn.commit()
    conn.close()

def get_holiday_date_from_holiday_date_table(current_year = '1900'):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(f"SELECT HOLIDAY_DATE, HOLIDAY_NAME, HOLIDAY_DAY FROM HOLIDAY_DATES WHERE RECORD_DELETED_FLAG = 0 AND START_DATE >= '{current_year}-01-01';")
    rows = cursor.fetchall()
    conn.close()
    if rows:
        data = [{'holiday_date': row[0], 'holiday_name': row[1], 'holiday_day': row[2]} for row in rows]
        return jsonify(data)
    return

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

def get_working_date_from_holiday_date_table(current_year = '1900'):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(f"SELECT WORKING_DATE, WORKING_DAY_NAME, WORKING_DAY FROM WORKING_DATES WHERE RECORD_DELETED_FLAG = 0 AND START_DATE >= '{current_year}-01-01';")
    rows = cursor.fetchall()
    conn.close()
    if rows:
        data = [{'working_date': row[0], 'working_day_name': row[1], 'working_day': row[2]} for row in rows]
        return jsonify(data)
    data = [{'working_date': None, 'working_day_name': None, 'working_day': None}]
    return jsonify(data)


def truncate_hist_returns_table():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('DROP TABLE IF EXISTS MF_HIST_RETURNS')

def create_hist_returns_table():
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

def get_mf_hist_returns_from_mf_hist_returns_table():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(f'SELECT PROCESSING_DATE, "%TOTAL_P/L", "%DAY_P/L" FROM MF_HIST_RETURNS WHERE RECORD_DELETED_FLAG = 0 ORDER BY PROCESSING_DATE;')
    rows = cursor.fetchall()
    conn.close()
    if rows:
        data = [{'processing_date': row[0], 'perc_total_p_l': row[1], 'perc_day_p_l': row[2]} for row in rows]
        return jsonify(data)
    data = [{'processing_date': None, 'perc_total_p_l': None, 'perc_day_p_l': None} for row in rows]
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

def create_stock_order_table():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS STOCK_ORDER (
            ID INTEGER PRIMARY KEY AUTOINCREMENT,
            NAME TEXT(200),
            TRADE_ENTRY_DATE DATE,
            TRADE_ENTRY_TIME TIME,
            TRADE_EXIT_DATE DATE,
            TRADE_EXIT_TIME TIME,
            STOCK_QUANTITY INTEGER,
            TRADE_TYPE TEXT(20),
            LEVERAGE INTEGER,
            TRADE_POSITION TEXT(20),
            STOCK_BUY_PRICE NUMERIC(10,4),
            STOCK_SELL_PRICE NUMERIC(10,4),
            BROKERAGE NUMERIC(10,4),
            EXCHANGE_TRANSACTION_FEES NUMERIC(10,4),
            IGST NUMERIC(10,4),
            SECURITIES_TRANSACTION_TAX NUMERIC(10,4),
            SEBI_TURNOVER_FEES NUMERIC(10,4),
            HOLDING_DAYS INTEGER,
            "SELL-BUY" NUMERIC(10,4),
            "%ACTUAL_P_L_WITHOUT_LEVERAGE" NUMERIC(10,4),
            DEPLOYED_CAPITAL NUMERIC(10,4),
            NET_OBLIGATION NUMERIC(10,4),
            TOTAL_FEES NUMERIC(10,4),
            NET_RECEIVABLE NUMERIC(10,4),
            AUTO_SQUARE_OFF_CHARGES NUMERIC(10,4),
            DEPOSITORY_CHARGES NUMERIC(10,4),
            "%ACTUAL_P_L_WITH_LEVERAGE" NUMERIC(10,4),
            START_DATE DATE,
            END_DATE DATE,
            RECORD_DELETED_FLAG INTEGER
        )''')
    
def insert_stock_order_entry(alt_symbol, trade_entry_date, trade_entry_time, trade_exit_date, trade_exit_time, stock_quantity, trade_type, leverage, trade_position, stock_buy_price, stock_sell_price, brokerage, exchange_transaction_fees, igst, securities_transaction_tax, sebi_turnover_fees, holding_days, sell_minus_buy, actual_p_l_w_o_leverage, deployed_capital, net_obligation, total_fees, net_receivable, auto_square_off_charges, depository_charges, actual_p_l_w_leverage):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('INSERT INTO STOCK_ORDER (NAME, TRADE_ENTRY_DATE, TRADE_ENTRY_TIME, TRADE_EXIT_DATE, TRADE_EXIT_TIME, STOCK_QUANTITY, TRADE_TYPE, LEVERAGE, TRADE_POSITION, STOCK_BUY_PRICE, STOCK_SELL_PRICE, BROKERAGE, EXCHANGE_TRANSACTION_FEES, IGST, SECURITIES_TRANSACTION_TAX, SEBI_TURNOVER_FEES, HOLDING_DAYS, "SELL-BUY", "%ACTUAL_P_L_WITHOUT_LEVERAGE", DEPLOYED_CAPITAL, NET_OBLIGATION, TOTAL_FEES, NET_RECEIVABLE, AUTO_SQUARE_OFF_CHARGES, DEPOSITORY_CHARGES, "%ACTUAL_P_L_WITH_LEVERAGE", START_DATE, END_DATE, RECORD_DELETED_FLAG) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
                   (alt_symbol, trade_entry_date, trade_entry_time, trade_exit_date, trade_exit_time, stock_quantity, trade_type, leverage, trade_position, stock_buy_price, stock_sell_price, brokerage, exchange_transaction_fees, igst, securities_transaction_tax, sebi_turnover_fees, holding_days, sell_minus_buy, actual_p_l_w_o_leverage, deployed_capital, net_obligation, total_fees, net_receivable, auto_square_off_charges, depository_charges, actual_p_l_w_leverage, trade_entry_date, '9998-12-31', 0  ))
    conn.commit()
    conn.close()

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
            SECURTIES_TRANSACTION_TAX NUMERIC(10,4),
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
    cursor.execute(f"""SELECT TRADE_DATE, NET_OBLIGATION, BROKERAGE, EXCHANGE_TRANSACTION_CHARGES, IGST, SECURTIES_TRANSACTION_TAX, SEBI_TURN_OVER_FEES, AUTO_SQUARE_OFF_CHARGES, DEPOSITORY_CHARGES
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
                cursor.execute("INSERT INTO  FEE_COMPONENT (FEE_ID, TRADE_DATE, NET_OBLIGATION, BROKERAGE, EXCHANGE_TRANSACTION_CHARGES, IGST, SECURTIES_TRANSACTION_TAX, SEBI_TURN_OVER_FEES, AUTO_SQUARE_OFF_CHARGES, DEPOSITORY_CHARGES, START_DATE, END_DATE, RECORD_DELETED_FLAG) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                              (str(fee_id), trade_date, net_obligation, brokerage, exc_trans_charges, igst, sec_trans_tax, sebi_turn_fees, auto_square_off_charges, depository_charges, today, '9998-12-31', 0))
                conn.commit()
    else:
        cursor.execute("INSERT INTO FEE_COMPONENT (FEE_ID, TRADE_DATE, NET_OBLIGATION, BROKERAGE, EXCHANGE_TRANSACTION_CHARGES, IGST, SECURTIES_TRANSACTION_TAX, SEBI_TURN_OVER_FEES, AUTO_SQUARE_OFF_CHARGES, DEPOSITORY_CHARGES, START_DATE, END_DATE, RECORD_DELETED_FLAG) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                      (str(fee_id), trade_date, net_obligation, brokerage, exc_trans_charges, igst, sec_trans_tax, sebi_turn_fees, auto_square_off_charges, depository_charges, trade_date, '9998-12-31', 0))
        conn.commit()
    conn.close()