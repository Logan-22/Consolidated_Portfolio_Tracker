from flask import current_app
import sqlite3
from utils.folder_utils.paths import db_path
from utils.sql_utils.process.fetch_queries import fetch_queries_as_dictionaries

def get_all_symbols_list_from_metadata_store(portfolio_type = None):
    env = current_app.config['ENVIRONMENT']
    portfolio_type_filter = f"AND PORTFOLIO_TYPE = '{portfolio_type}'" if portfolio_type else ""
    symbol_data = fetch_queries_as_dictionaries(f"""
SELECT DISTINCT
    EXCHANGE_SYMBOL
    ,YAHOO_SYMBOL
    ,PORTFOLIO_TYPE
FROM
    {env}T_META.METADATA_INSTRUMENTS
WHERE
    RECORD_DELETED_FLAG = 0
    {portfolio_type_filter};
    """, 'return_none', fetch = 'All')
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

def get_instrument_price(instrument_id, value_date):
    env = current_app.config['ENVIRONMENT']
    instrument_id_filter = f"AND INSTRUMENT_ID = {instrument_id}" if instrument_id else ""
    value_date_filter    = f"AND VALUE_DATE    = '{value_date}'"  if value_date    else ""
    instrument_price_data = fetch_queries_as_dictionaries(f"""
SELECT
    INSTRUMENT_ID
    ,VALUE_DATE
    ,PRICE
FROM
    {env}T_TIER0_METRICS.DAILY_INSTRUMENT_PRICES
WHERE
    RECORD_DELETED_FLAG = 0
    {instrument_id_filter}
    {value_date_filter}
GROUP BY 1,2,3
ORDER BY 1,2,3;
    """, 'return_none', fetch = 'One')
    return instrument_price_data

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

def get_prev_proc_date_from_holiday_calendar_table(processing_date):
    env = current_app.config['ENVIRONMENT']
    prev_processing_date_data = fetch_queries_as_dictionaries(f"""
SELECT DISTINCT
    PREVIOUS_PROCESSING_DATE
FROM
    {env}T_META.HOLIDAY_CALENDAR
WHERE
    PROCESSING_DATE = '{processing_date}';
    """, 'return_none', fetch = 'One')
    return prev_processing_date_data['PREVIOUS_PROCESSING_DATE']

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
    env = current_app.config['ENVIRONMENT']
    portfolio_type_filter = f"AND MI.PORTFOLIO_TYPE = '{portfolio_type}'" if portfolio_type else ""
    max_value_date_for_each_portfolio = fetch_queries_as_dictionaries(f"""
SELECT
    MI.EXCHANGE_SYMBOL
    ,MI.PORTFOLIO_TYPE
    ,MAX(PR.VALUE_DATE) AS MAX_PRICE_DATE
FROM
    {env}T_META.METADATA_INSTRUMENTS MI
LEFT OUTER JOIN
    {env}T_TIER0_METRICS.DAILY_INSTRUMENT_PRICES PR
ON
    MI.INSTRUMENT_ID           = PR.INSTRUMENT_ID
    AND PR.RECORD_DELETED_FLAG = 0
WHERE
    MI.RECORD_DELETED_FLAG = 0
    {portfolio_type_filter}
GROUP BY 1,2;
    """)
    return max_value_date_for_each_portfolio

def get_max_next_processing_date_from_table(databasename, table_name):
    max_next_proc_date = fetch_queries_as_dictionaries(f"""
SELECT
    MAX(NEXT_PROCESSING_DATE) AS NEXT_PROCESSING_DATE
FROM
    {databasename}.{table_name};
    """, 'return_none', fetch = 'One')
    return max_next_proc_date

def get_holiday_dates(year = None):
    env = current_app.config['ENVIRONMENT']
    current_year_filter = f"AND HOLIDAY_DATE >= '{year}-01-01' AND HOLIDAY_DATE <= '{year}-12-31'" if year else ""
    holiday_date_data = fetch_queries_as_dictionaries(f"""
SELECT
    DATE_FORMAT(HOLIDAY_DATE,'%Y-%m-%d') AS HOLIDAY_DATE
    ,HOLIDAY_NAME
    ,HOLIDAY_DAY
FROM
    {env}T_META.HOLIDAY_DATES
WHERE
    RECORD_DELETED_FLAG = 0
    {current_year_filter}
    ORDER BY HOLIDAY_DATE;
    """, 'return_none')
    return holiday_date_data

def get_working_dates(year = None):
    env = current_app.config['ENVIRONMENT']
    current_year_filter = f"AND WORKING_DATE >= '{year}-01-01' AND WORKING_DATE <= '{year}-12-31'" if year else ""
    working_date_data = fetch_queries_as_dictionaries(f"""
SELECT
    DATE_FORMAT(WORKING_DATE,'%Y-%m-%d') AS WORKING_DATE
    ,WORKING_DAY_NAME
    ,WORKING_DAY
FROM
    {env}T_META.WORKING_DATES
WHERE
    RECORD_DELETED_FLAG = 0
    {current_year_filter}
    ORDER BY WORKING_DATE;
    """, 'return_none')
    return working_date_data

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

def get_first_purchase_date_from_mf_txn_table(user_id = None):
    env = current_app.config['ENVIRONMENT']
    user_id_filter = f"AND TXN.USER_ID = {user_id}" if user_id else None
    first_mf_purchase_data = fetch_queries_as_dictionaries(f"""
SELECT
    MIN(TXN.TXN_DATE) AS MF_FIRST_PURCHASE_DATE
FROM
    {env}T_USR_TXN.MF_TRANSACTIONS TXN
WHERE
    TXN.RECORD_DELETED_FLAG = 0
    {user_id_filter};
    """, 'return_none', fetch = 'One')
    return first_mf_purchase_data

def get_date_setup_from_holiday_calendar(input_date):
    env = current_app.config['ENVIRONMENT']
    holiday_calendar_payload = fetch_queries_as_dictionaries(f"""
SELECT DISTINCT
    PROCESSING_DATE
    ,NEXT_PROCESSING_DATE
    ,PREVIOUS_PROCESSING_DATE
FROM
    {env}T_META.HOLIDAY_CALENDAR
WHERE
    PROCESSING_DATE <= '{input_date}'
    AND RECORD_DELETED_FLAG = 0
ORDER BY PROCESSING_DATE DESC
LIMIT 1;
    """, 'return_none', fetch = 'One')
    return holiday_calendar_payload

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

def get_first_trade_date_from_trades_table():
    first_swing_trade_data = fetch_queries_as_dictionaries("""
SELECT
    MIN(TRADE_DATE) AS FIRST_TRADE_DATE
FROM
    TRADES;
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

def get_component_info_from_db(component_type = None, schema_name = None, table_name = None):
    schema_info_dict = {}
    schema_filter = f"AND SCHEMA_NAME = '{schema_name}'" if schema_name else ""
    schema_list = fetch_queries_as_dictionaries(f"""
SELECT
    SCHEMA_NAME AS SCHEMA_NAME
FROM
    INFORMATION_SCHEMA.SCHEMATA
WHERE
    1 = 1
    {schema_filter}
    AND SCHEMA_NAME NOT IN ('information_schema','mysql','performance_schema','sys')
ORDER BY SCHEMA_NAME;
    """, "return_none")
    for schema in schema_list:
        schema_filter = f"AND TABLE_SCHEMA = '{schema['SCHEMA_NAME']}'"
        type_filter = f"AND TABLE_TYPE = '{component_type}'" if component_type else ""
        table_filter = f"AND TABLE_NAME = '{table_name}'" if table_name else ""
        component_info_dict = {}
        component_list = fetch_queries_as_dictionaries(f"""
SELECT
    TABLE_NAME AS COMPONENT_NAME
FROM
    INFORMATION_SCHEMA.TABLES
WHERE
    1 = 1
    {schema_filter}
    {type_filter}
    {table_filter}
    AND TABLE_NAME NOT LIKE 'mysql_%'
ORDER BY TABLE_NAME;
    """, "return_none")
        for component in component_list:
            component_filter = f"AND TABLE_NAME = '{component['COMPONENT_NAME']}'"
            component_column_data = fetch_queries_as_dictionaries(f"""
SELECT
    COLUMN_NAME AS COLUMN_NAME
FROM
    INFORMATION_SCHEMA.COLUMNS
WHERE
    1 = 1
    {schema_filter}
    {component_filter}
ORDER BY ORDINAL_POSITION;
    """)
            columns_list = []
            for column in component_column_data:
                columns_list.append(column['COLUMN_NAME'])
            component_info_dict[component['COMPONENT_NAME']] = columns_list
        schema_info_dict[schema['SCHEMA_NAME']] = component_info_dict
    return schema_info_dict

def get_missing_prices_from_price_table():
    env = current_app.config['ENVIRONMENT']
    price_start_date = current_app.config['PRICE_START_DATE']
    missing_prices_data = fetch_queries_as_dictionaries(f"""
SELECT
    META.INSTRUMENT_ID
    ,META.EXCHANGE_SYMBOL
    ,DATE_FORMAT(HC.PROCESSING_DATE, '%Y-%m-%d') AS VALUE_DATE
    ,PR.PRICE
    ,META.PORTFOLIO_TYPE
FROM
    {env}T_META.METADATA_INSTRUMENTS META
LEFT OUTER JOIN
    {env}T_META.HOLIDAY_CALENDAR HC
ON
    HC.PROCESSING_DATE         >= META.LAUNCHED_ON
    AND HC.PROCESSING_DATE     >= '{price_start_date}'
    AND HC.RECORD_DELETED_FLAG = 0
LEFT OUTER JOIN
    {env}T_TIER0_METRICS.DAILY_INSTRUMENT_PRICES PR
ON
    PR.INSTRUMENT_ID           = META.INSTRUMENT_ID
    AND PR.VALUE_DATE          = HC.PROCESSING_DATE
    AND PR.RECORD_DELETED_FLAG = 0
WHERE
    PR.PRICE IS NULL
    AND HC.PROCESSING_DATE NOT IN (SELECT DISTINCT WORKING_DATE FROM {env}T_META.WORKING_DATES)
    AND ((META.PORTFOLIO_TYPE = 'Mutual Fund' AND HC.PROCESSING_DATE < CURRENT_DATE)
    OR (META.PORTFOLIO_TYPE = 'Stock' AND HC.PROCESSING_DATE <= CURRENT_DATE))
GROUP BY 1,2,3,4
ORDER BY 3;
    """, 'return_none', fetch = 'All')
    return missing_prices_data

def get_from_sqlite_component(component_name):
    component_data = fetch_queries_as_dictionaries(f'SELECT * FROM "{component_name}";')
    return component_data

def get_or_create_instrument_id(exchange_symbol, process_type, process_flag = None):
    env = current_app.config['ENVIRONMENT']
    starting_key_value  = int(current_app.config['STARTING_KEY_VALUE'])
    exchange_symbol_filter = f"AND EXCHANGE_SYMBOL = '{exchange_symbol}'" if exchange_symbol else ""
    process_flag_filter    = f"AND PROCESS_FLAG    = {process_flag}"    if process_flag else ""
    instrument_ids = []
    instruments_data = fetch_queries_as_dictionaries(f"""
SELECT
    INSTRUMENT_ID
FROM
    {env}T_META.METADATA_INSTRUMENTS
WHERE
    RECORD_DELETED_FLAG = 0
    {exchange_symbol_filter}
    {process_flag_filter};
""", 'return_none', fetch = 'One' if exchange_symbol else 'All')
    if instruments_data and exchange_symbol:
        instrument_id = instruments_data['INSTRUMENT_ID']
        return instrument_id
    if instruments_data and not exchange_symbol:
        for instrument in instruments_data:
            instrument_ids.append(instrument['INSTRUMENT_ID'])
        return instrument_ids
    elif process_type == 'create':
        instrument_id_data = fetch_queries_as_dictionaries(f"""
SELECT
    MAX(INSTRUMENT_ID) AS INSTRUMENT_ID
FROM
    {env}T_META.METADATA_INSTRUMENTS
WHERE
    RECORD_DELETED_FLAG = 0;
""", 'return_none', fetch = 'One')
        if instrument_id_data and instrument_id_data.get('INSTRUMENT_ID'):
            instrument_id = instrument_id_data['INSTRUMENT_ID'] + 1
        else:
            instrument_id = starting_key_value
    else:
        instrument_id = None
    return instrument_id

def get_metadata_instruments(instrument_id, portfolio_type = None):
    env = current_app.config['ENVIRONMENT']
    price_start_date = current_app.config['PRICE_START_DATE']
    instrument_id_filter =  f"AND MI.INSTRUMENT_ID  = {instrument_id}"  if instrument_id else ""
    portfolio_type_filter = f"AND MI.PORTFOLIO_TYPE = '{portfolio_type}'" if portfolio_type else ""
    metadata_instruments_data = fetch_queries_as_dictionaries(f"""
SELECT
    MI.INSTRUMENT_ID
    ,MI.EXCHANGE_SYMBOL
    ,MI.YAHOO_SYMBOL
    ,MI.PORTFOLIO_TYPE
    ,COALESCE(MAX(PR.NEXT_PROCESSING_DATE), '{price_start_date}') AS START_DATE
FROM
    {env}T_META.METADATA_INSTRUMENTS MI
LEFT OUTER JOIN
    {env}T_TIER0_METRICS.DAILY_INSTRUMENT_PRICES PR
ON
    MI.INSTRUMENT_ID           = PR.INSTRUMENT_ID
    AND PR.RECORD_DELETED_FLAG = 0
WHERE
    MI.RECORD_DELETED_FLAG = 0
    {instrument_id_filter}
    {portfolio_type_filter}
GROUP BY 1,2,3,4;
    """, 'return_none', fetch = 'One')
    return metadata_instruments_data

def get_holding_data(instrument_id, user_id, processing_date):
    env = current_app.config['ENVIRONMENT']
    instrument_id_filter = f"AND DEP.INSTRUMENT_ID  = {instrument_id}"  if instrument_id else ""
    user_id_filter       = f"AND DEP.USER_ID = {user_id}" if user_id else ""
    processing_date_filter    = f"AND DEP.PROCESSING_DATE = '{processing_date}'" if processing_date else ""
    holding_data = fetch_queries_as_dictionaries(f"""
SELECT
    DEP.INSTRUMENT_ID
    ,DEP.USER_ID
    ,DEP.START_DATE
    ,DEP.TOTAL_QUANTITY
FROM
    {env}T_TIER0_METRICS.MF_DEPOSITORY_HOLDINGS DEP
WHERE
    DEP.RECORD_DELETED_FLAG = 0
    {instrument_id_filter}
    {user_id_filter}
    {processing_date_filter}
GROUP BY 1,2,3,4;
    """, 'return_none', fetch = 'One')
    return holding_data

def get_consolidated_quantity_from_mf_txn(instrument_id, user_id):
    env = current_app.config['ENVIRONMENT']
    instrument_id_filter = f"AND TXN.INSTRUMENT_ID  = {instrument_id}"  if instrument_id else ""
    user_id_filter       = f"AND TXN.USER_ID = {user_id}" if user_id else ""
    consolidated_quantity_data = fetch_queries_as_dictionaries(f"""
SELECT
    TXN.INSTRUMENT_ID
    ,TXN.USER_ID
    ,SUM(CASE WHEN TXN.TXN_TYPE = 'Buy'  THEN TXN.UNITS
              WHEN TXN.TXN_TYPE = 'Sell' THEN -1 * TXN.UNITS END) AS CONSOLIDATED_QUANTITY
FROM
    {env}T_USR_TXN.MF_TRANSACTIONS TXN
WHERE
    TXN.RECORD_DELETED_FLAG = 0
    {instrument_id_filter}
    {user_id_filter}
GROUP BY 1,2;
    """, 'return_none', fetch = 'One')
    return consolidated_quantity_data