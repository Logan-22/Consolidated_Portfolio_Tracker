import sqlite3
from utils.folder_utils.paths import db_path
from utils.sql_utils.process.fetch_queries import fetch_queries_as_dictionaries

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

def get_consolidated_returns():
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

def get_consolidated_allocation():
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

def get_all_consolidated_allocation():
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
    HC.PROCESSING_DATE    >= META.LAUNCHED_ON
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
    AND ((META.PORTFOLIO_TYPE = 'Mutual Fund' AND HC.PROCESSING_DATE < CURRENT_DATE)
    OR (META.PORTFOLIO_TYPE = 'Stock' AND HC.PROCESSING_DATE <= CURRENT_DATE))
GROUP BY 1,2,3,4
ORDER BY 2;
    """)
    return missing_prices_data
