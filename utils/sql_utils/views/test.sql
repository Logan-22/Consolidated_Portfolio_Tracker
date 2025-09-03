SELECT
    SUB.INSTRUMENT_ID                                                AS INSTRUMENT_ID
    ,SUB.USER_ID                                                     AS USER_ID
    ,SUB.EXCHANGE_SYMBOL                                             AS EXCHANGE_SYMBOL
    ,SUB.TOTAL_QUANTITY                                              AS TOTAL_QUANTITY
    ,SUB.TOTAL_INVESTED_AMOUNT                                       AS TOTAL_INVESTED_AMOUNT
    ,ROUND(SUB.TOTAL_INVESTED_AMOUNT / SUB.TOTAL_QUANTITY, 4)        AS AVERAGE_PRICE
    ,SUB.LAST_TXN_ID                                                 AS LAST_TXN_ID
    ,CASE WHEN SUB.TOTAL_QUANTITY <= 0 THEN 'Closed'
          ELSE 'Active' END                                          AS DEP_STATUS
    ,SUB.PROCESSING_DATE                                             AS PROCESSING_DATE
    ,SUB.PREVIOUS_PROCESSING_DATE                                    AS PREVIOUS_PROCESSING_DATE
    ,SUB.NEXT_PROCESSING_DATE                                        AS NEXT_PROCESSING_DATE
FROM
(
SELECT
    TXN.INSTRUMENT_ID                                                AS INSTRUMENT_ID
    ,TXN.USER_ID                                                     AS USER_ID
    ,TXN.EXCHANGE_SYMBOL                                             AS EXCHANGE_SYMBOL
    ,SUM(CASE WHEN TXN.TXN_TYPE = 'Buy'  THEN TXN.UNITS
              WHEN TXN.TXN_TYPE = 'Sell' THEN -1 * TXN.UNITS END)    AS TOTAL_QUANTITY
    ,SUM(TXN.TXN_AMOUNT)                                             AS TOTAL_INVESTED_AMOUNT
    ,MAX(TXN.TXN_ID)                                                 AS LAST_TXN_ID
    ,(SELECT DISTINCT PROCESSING_DATE 
    FROM {env}T_UTIL.PROCESSING_DATE WHERE PROC_TYP_CD = 'MF_PROC')  AS PROCESSING_DATE
    ,(SELECT DISTINCT PREVIOUS_PROCESSING_DATE 
    FROM {env}T_UTIL.PROCESSING_DATE WHERE PROC_TYP_CD = 'MF_PROC')  AS PREVIOUS_PROCESSING_DATE
    ,(SELECT DISTINCT NEXT_PROCESSING_DATE 
    FROM {env}T_UTIL.PROCESSING_DATE WHERE PROC_TYP_CD = 'MF_PROC')  AS NEXT_PROCESSING_DATE
FROM
    {env}T_USR_TXN.MF_TRANSACTIONS TXN
WHERE
    TXN.RECORD_DELETED_FLAG = 0
    AND TXN.TXN_DATE       <= (SELECT DISTINCT PROCESSING_DATE FROM {env}T_UTIL.PROCESSING_DATE WHERE PROC_TYP_CD = 'MF_PROC') -- SINCE THIS IS A USER ENTRY AND START_DATE WILL BE THE DATE IT WAS ENTERED
    AND TXN.END_DATE       >= (SELECT DISTINCT PROCESSING_DATE FROM {env}T_UTIL.PROCESSING_DATE WHERE PROC_TYP_CD = 'MF_PROC')
GROUP BY 1,2,3
) SUB
;
