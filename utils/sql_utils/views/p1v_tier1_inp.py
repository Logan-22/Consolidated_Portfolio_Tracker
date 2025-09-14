from os import getenv
from utils.connection_utils.connection_pool_config import connection_pool

env = getenv('ENVIRONMENT')

def create_tier1_inp_view_schema(tier1_inp_view_schema = f"{env}V_TIER1_INP"):
    conn = connection_pool.get_connection()
    cursor = conn.cursor()
    cursor.execute(f"""
CREATE DATABASE IF NOT EXISTS {tier1_inp_view_schema}
CHARACTER SET utf8mb4
COLLATE utf8mb4_unicode_ci;
    """)
    cursor.close()
    conn.close()

def create_tier1_mf_portfolio_view(tier1_inp_view_schema = f"{env}V_TIER1_INP"):
    conn = connection_pool.get_connection()
    cursor = conn.cursor()
    cursor.execute(f"""
CREATE OR REPLACE VIEW {tier1_inp_view_schema}.H1_MF_PORTFOLIO_VIEW AS
SELECT
    DEP.INSTRUMENT_ID                                                AS INSTRUMENT_ID
    ,DEP.USER_ID                                                     AS USER_ID
    ,META.AMC                                                        AS FUND_AMC
    ,META.MF_TYPE                                                    AS FUND_TYPE
    ,META.FUND_CATEGORY                                              AS FUND_CATEGORY
    ,META.ALLOCATION_CATEGORY                                        AS FUND_ALLOCATION_CATEGORY
    ,DEP.TOTAL_QUANTITY                                              AS FUND_UNITS
    ,DEP.AVERAGE_PRICE                                               AS AVERAGE_PRICE
    ,DEP.TOTAL_INVESTED_AMOUNT                                       AS TOTAL_INVESTED_AMOUNT
    ,DEP.TOTAL_AMC_AMOUNT                                            AS TOTAL_AMC_AMOUNT
    ,DEP.TOTAL_STAMP_FEES_AMOUNT                                     AS TOTAL_STAMP_FEES_AMOUNT
    ,(SELECT DISTINCT PROCESSING_DATE FROM {env}T_UTIL.PROCESSING_DATE
    WHERE PROC_TYP_CD = 'MF_PROC')                                   AS PROCESSING_DATE
    ,(SELECT DISTINCT PREVIOUS_PROCESSING_DATE FROM {env}T_UTIL.PROCESSING_DATE
    WHERE PROC_TYP_CD = 'MF_PROC')                                   AS PREVIOUS_PROCESSING_DATE
    ,(SELECT DISTINCT NEXT_PROCESSING_DATE FROM {env}T_UTIL.PROCESSING_DATE
    WHERE PROC_TYP_CD = 'MF_PROC')                                   AS NEXT_PROCESSING_DATE
    ,CURRENT_PRICE.PRICE                                             AS CURRENT_NAV
    ,ROUND(DEP.TOTAL_QUANTITY * CURRENT_PRICE.PRICE, 4)              AS CURRENT_VALUE
    ,ROUND((DEP.TOTAL_QUANTITY * CURRENT_PRICE.PRICE) - DEP.TOTAL_AMC_AMOUNT, 4)
                                                                     AS P_L
    ,ROUND(((DEP.TOTAL_QUANTITY * CURRENT_PRICE.PRICE) - DEP.TOTAL_AMC_AMOUNT)
    / DEP.TOTAL_AMC_AMOUNT, 4)                                       AS PERC_P_L                                                             
    ,PREV_PRICE.PRICE                                                AS PREVIOUS_NAV
    ,ROUND(PREV_DEP.TOTAL_QUANTITY * PREV_PRICE.PRICE, 4)            AS PREVIOUS_VALUE
    ,ROUND((DEP.TOTAL_QUANTITY * CURRENT_PRICE.PRICE) -
    (PREV_DEP.TOTAL_QUANTITY * PREV_PRICE.PRICE), 4)                 AS DAY_P_L
    ,ROUND(((DEP.TOTAL_QUANTITY * CURRENT_PRICE.PRICE) -
    (PREV_DEP.TOTAL_QUANTITY * PREV_PRICE.PRICE)) / 
    (PREV_DEP.TOTAL_QUANTITY * PREV_PRICE.PRICE), 4)                 AS PERC_DAY_P_L 
FROM
    {env}T_TIER0_METRICS.MF_DEPOSITORY_HOLDINGS DEP
LEFT OUTER JOIN
    {env}T_TIER0_METRICS.MF_DEPOSITORY_HOLDINGS PREV_DEP
ON
    DEP.INSTRUMENT_ID                = PREV_DEP.INSTRUMENT_ID
    AND DEP.USER_ID                  = PREV_DEP.USER_ID
    AND PREV_DEP.RECORD_DELETED_FLAG = 0
    AND PREV_DEP.START_DATE          = (SELECT DISTINCT PREVIOUS_PROCESSING_DATE FROM {env}T_UTIL.PROCESSING_DATE WHERE PROC_TYP_CD = 'MF_PROC')
LEFT OUTER JOIN
    {env}T_META.METADATA_INSTRUMENTS META
ON
    DEP.INSTRUMENT_ID            = META.INSTRUMENT_ID
    AND META.RECORD_DELETED_FLAG = 0
-- CURRENT NAV DATA
LEFT OUTER JOIN
    {env}T_TIER0_METRICS.DAILY_INSTRUMENT_PRICES CURRENT_PRICE
ON
    CURRENT_PRICE.VALUE_DATE              = (SELECT DISTINCT PROCESSING_DATE FROM {env}T_UTIL.PROCESSING_DATE WHERE PROC_TYP_CD = 'MF_PROC')
    AND CURRENT_PRICE.RECORD_DELETED_FLAG = 0
    AND CURRENT_PRICE.INSTRUMENT_ID       = DEP.INSTRUMENT_ID
--  PREVIOUS NAV DATA    
LEFT OUTER JOIN
    {env}T_TIER0_METRICS.DAILY_INSTRUMENT_PRICES PREV_PRICE
ON
    PREV_PRICE.VALUE_DATE              = (SELECT DISTINCT PREVIOUS_PROCESSING_DATE FROM {env}T_UTIL.PROCESSING_DATE WHERE PROC_TYP_CD = 'MF_PROC')
    AND PREV_PRICE.RECORD_DELETED_FLAG = 0
    AND PREV_PRICE.INSTRUMENT_ID       = DEP.INSTRUMENT_ID
WHERE
    DEP.START_DATE = (SELECT DISTINCT PROCESSING_DATE FROM {env}T_UTIL.PROCESSING_DATE WHERE PROC_TYP_CD = 'MF_PROC')
    AND DEP.RECORD_DELETED_FLAG = 0
    AND CURRENT_PRICE.PRICE IS NOT NULL
;
    """)
    cursor.close()
    conn.close()