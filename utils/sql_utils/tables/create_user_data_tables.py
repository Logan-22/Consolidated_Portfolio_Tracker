import sqlite3
from utils.folder_utils.paths import db_path

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
    conn.close()

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
    conn.close()

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
    conn.close()
