import sqlite3
from utils.folder_utils.paths import db_path

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

    conn.commit()
    conn.close()