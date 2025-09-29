from os import getenv
from utils.connection_utils.connection_pool_config import connection_pool

env = getenv('ENVIRONMENT')

def create_tier3_inp_view_schema(tier3_inp_view_schema = f"{env}V_TIER3_INP"):
    conn = connection_pool.get_connection()
    cursor = conn.cursor()
    cursor.execute(f"""
CREATE DATABASE IF NOT EXISTS {tier3_inp_view_schema}
CHARACTER SET utf8mb4
COLLATE utf8mb4_unicode_ci;
    """)
    cursor.close()
    conn.close()

def create_tier3_fin_mf_portfolio_view(tier3_inp_view_schema = f"{env}V_TIER3_INP"):
    conn = connection_pool.get_connection()
    cursor = conn.cursor()
    cursor.execute(f"""
CREATE OR REPLACE VIEW {tier3_inp_view_schema}.H3_FIN_MF_PORTFOLIO_VIEW AS
SELECT
    AMF.USER_ID                                                      AS USER_ID
    ,ROUND(SUM(AMF.AGG_TOTAL_INVESTED_AMOUNT),4)                     AS FIN_TOTAL_INVESTED_AMOUNT
    ,ROUND(SUM(AMF.AGG_TOTAL_AMC_AMOUNT),4)                          AS FIN_TOTAL_AMC_AMOUNT
    ,ROUND(SUM(AMF.AGG_TOTAL_STAMP_FEES_AMOUNT),4)                   AS FIN_TOTAL_STAMP_FEES_AMOUNT
    ,ROUND(SUM(AMF.AGG_CURRENT_VALUE), 4)                            AS FIN_CURRENT_VALUE
    ,ROUND(SUM(AMF.AGG_P_L), 4)                                      AS FIN_P_L
    ,ROUND(SUM(AMF.AGG_P_L) / 
     SUM(AMF.AGG_TOTAL_AMC_AMOUNT) * 100, 4)                         AS FIN_PERC_P_L                                                            
    ,ROUND(SUM(AMF.AGG_PREVIOUS_VALUE), 4)                           AS FIN_PREVIOUS_VALUE
    ,ROUND(SUM(AMF.AGG_DAY_P_L), 4)                                  AS FIN_DAY_P_L
    ,ROUND(SUM(AMF.AGG_DAY_P_L) /
     SUM(AMF.AGG_TOTAL_AMC_AMOUNT) * 100, 4)                         AS FIN_PERC_DAY_P_L
    ,(SELECT DISTINCT PROCESSING_DATE FROM {env}T_UTIL.PROCESSING_DATE
    WHERE PROC_TYP_CD = 'MF_PROC')                                   AS PROCESSING_DATE
    ,(SELECT DISTINCT PREVIOUS_PROCESSING_DATE FROM {env}T_UTIL.PROCESSING_DATE
    WHERE PROC_TYP_CD = 'MF_PROC')                                   AS PREVIOUS_PROCESSING_DATE
    ,(SELECT DISTINCT NEXT_PROCESSING_DATE FROM {env}T_UTIL.PROCESSING_DATE
    WHERE PROC_TYP_CD = 'MF_PROC')                                   AS NEXT_PROCESSING_DATE 
FROM
    {env}T_TIER2_METRICS.AGG_MF_PORTFOLIO AMF
WHERE
    AMF.PROCESSING_DATE = (SELECT DISTINCT PROCESSING_DATE FROM {env}T_UTIL.PROCESSING_DATE WHERE PROC_TYP_CD = 'MF_PROC')
    AND AMF.RECORD_DELETED_FLAG = 0
GROUP BY 1,11,12,13
    """)
    cursor.close()
    conn.close()