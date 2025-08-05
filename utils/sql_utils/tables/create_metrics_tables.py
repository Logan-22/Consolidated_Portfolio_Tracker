import sqlite3
from utils.folder_utils.paths import db_path

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
    conn.close()

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
    conn.close()

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
    conn.close()

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
    conn.close()

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
    conn.close()

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
    conn.close()

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
    conn.close()

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
    conn.close()

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
    conn.close()

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
    conn.close()

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
    conn.close()

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
    conn.close()

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
    conn.close()

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
    conn.close()

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
    conn.close()

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
    conn.close()

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
    conn.close()

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
    conn.close()

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
    conn.close()

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
    conn.close()

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
    conn.close()