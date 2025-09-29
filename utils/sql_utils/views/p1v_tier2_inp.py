from os import getenv
from utils.connection_utils.connection_pool_config import connection_pool

env = getenv('ENVIRONMENT')

def create_tier2_inp_view_schema(tier2_inp_view_schema = f"{env}V_TIER2_INP"):
    conn = connection_pool.get_connection()
    cursor = conn.cursor()
    cursor.execute(f"""
CREATE DATABASE IF NOT EXISTS {tier2_inp_view_schema}
CHARACTER SET utf8mb4
COLLATE utf8mb4_unicode_ci;
    """)
    cursor.close()
    conn.close()

def create_tier2_agg_mf_portfolio_view(tier2_inp_view_schema = f"{env}V_TIER2_INP"):
    conn = connection_pool.get_connection()
    cursor = conn.cursor()
    cursor.execute(f"""
CREATE OR REPLACE VIEW {tier2_inp_view_schema}.H2_AGG_MF_PORTFOLIO_VIEW AS
SELECT
    MF.INSTRUMENT_ID                                                 AS INSTRUMENT_ID
    ,MF.USER_ID                                                      AS USER_ID
    ,MF.FUND_AMC                                                     AS FUND_AMC
    ,MF.FUND_TYPE                                                    AS FUND_TYPE
    ,MF.FUND_CATEGORY                                                AS FUND_CATEGORY
    ,MF.FUND_ALLOCATION_CATEGORY                                     AS FUND_ALLOCATION_CATEGORY
    ,ROUND(SUM(MF.FUND_UNITS),4)                                     AS AGG_FUND_UNITS
    ,ROUND(SUM(MF.TOTAL_INVESTED_AMOUNT),4)                          AS AGG_TOTAL_INVESTED_AMOUNT
    ,ROUND(SUM(MF.TOTAL_AMC_AMOUNT),4)                               AS AGG_TOTAL_AMC_AMOUNT
    ,ROUND(SUM(MF.TOTAL_STAMP_FEES_AMOUNT),4)                        AS AGG_TOTAL_STAMP_FEES_AMOUNT
    ,ROUND(SUM(MF.TOTAL_AMC_AMOUNT) / SUM(MF.FUND_UNITS) ,4)         AS AGG_AVERAGE_PRICE
    ,MF.CURRENT_NAV                                                  AS CURRENT_NAV
    ,ROUND(SUM(MF.CURRENT_VALUE), 4)                                 AS AGG_CURRENT_VALUE
    ,ROUND(SUM(MF.P_L), 4)                                           AS AGG_P_L
    ,ROUND(SUM(MF.P_L) / SUM(MF.TOTAL_AMC_AMOUNT) * 100, 4)          AS AGG_PERC_P_L                                                            
    ,MF.PREVIOUS_NAV                                                 AS PREVIOUS_NAV
    ,ROUND(SUM(MF.PREVIOUS_VALUE), 4)                                AS AGG_PREVIOUS_VALUE
    ,ROUND(SUM(MF.DAY_P_L), 4)                                       AS AGG_DAY_P_L
    ,ROUND(SUM(MF.DAY_P_L) / SUM(MF.TOTAL_AMC_AMOUNT) * 100, 4)      AS AGG_PERC_DAY_P_L
    ,(SELECT DISTINCT PROCESSING_DATE FROM {env}T_UTIL.PROCESSING_DATE
    WHERE PROC_TYP_CD = 'MF_PROC')                                   AS PROCESSING_DATE
    ,(SELECT DISTINCT PREVIOUS_PROCESSING_DATE FROM {env}T_UTIL.PROCESSING_DATE
    WHERE PROC_TYP_CD = 'MF_PROC')                                   AS PREVIOUS_PROCESSING_DATE
    ,(SELECT DISTINCT NEXT_PROCESSING_DATE FROM {env}T_UTIL.PROCESSING_DATE
    WHERE PROC_TYP_CD = 'MF_PROC')                                   AS NEXT_PROCESSING_DATE 
FROM
    {env}T_TIER1_METRICS.MF_PORTFOLIO MF
WHERE
    MF.PROCESSING_DATE = (SELECT DISTINCT PROCESSING_DATE FROM {env}T_UTIL.PROCESSING_DATE WHERE PROC_TYP_CD = 'MF_PROC')
    AND MF.RECORD_DELETED_FLAG = 0
GROUP BY 1,2,3,4,5,6,12,16,20,21,22
    """)
    cursor.close()
    conn.close()