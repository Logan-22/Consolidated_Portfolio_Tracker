CONSOLIDATED_PORTFOLIO_VIEW = '''
CREATE VIEW CONSOLIDATED_PORTFOLIO_VIEW AS
SELECT
    CAST('Mutual Funds' AS VARCHAR(100))                                              AS PORTFOLIO_TYPE
    ,MF.PROCESSING_DATE                                                               AS PROCESSING_DATE
    ,MF.PREV_PROCESSING_DATE                                                          AS PREV_PROCESSING_DATE
    ,MF.NEXT_PROCESSING_DATE                                                          AS NEXT_PROCESSING_DATE
    ,MF.AMOUNT_INVESTED_AS_ON_PROCESSING_DATE                                         AS INVESTED_AMOUNT
    ,MF.AMOUNT_AS_ON_PROCESSING_DATE                                                  AS CURRENT_VALUE
    ,MF.AMOUNT_AS_ON_PREV_PROCESSING_DATE                                             AS PREVIOUS_VALUE
    ,MF."TOTAL_P/L"                                                                   AS "TOTAL_P/L"
    ,MF."DAY_P/L"                                                                     AS "DAY_P/L"
    ,MF."%TOTAL_P/L"                                                                  AS "%_TOTAL_P/L"
    ,MF."%DAY_P/L"                                                                    AS "%_DAY_P/L"
FROM
    MF_HIST_RETURNS MF
WHERE
    (SELECT DISTINCT PROC_DATE FROM PROCESSING_DATE WHERE PROC_TYP_CD = 'MF_PROC') BETWEEN MF.START_DATE AND MF.END_DATE

UNION ALL

SELECT
    'Intraday Stocks'                                                                 AS PORTFOLIO_TYPE
    ,REALISED_INTRADAY.TRADE_DATE                                                     AS PROCESSING_DATE
    ,NULL                                                                             AS PREV_PROCESSING_DATE
    ,NULL                                                                             AS NEXT_PROCESSING_DATE
    ,REALISED_INTRADAY.ACTUAL_DEPLOYED_CAPITAL                                        AS INVESTED_AMOUNT
    ,ROUND(REALISED_INTRADAY.ACTUAL_DEPLOYED_CAPITAL + 
     REALISED_INTRADAY."NET_P/L_MINUS_CHARGES",4)                                     AS CURRENT_VALUE
    ,0                                                                                AS PREVIOUS_VALUE
    ,REALISED_INTRADAY."NET_P/L_MINUS_CHARGES"                                        AS "TOTAL_P/L"
    ,REALISED_INTRADAY."NET_P/L_MINUS_CHARGES"                                        AS "DAY_P/L"
    ,REALISED_INTRADAY."NET_%_P/L_WITH_LEVERAGE_INCLUDING_CHARGES"                    AS "%_TOTAL_P/L"
    ,REALISED_INTRADAY."NET_%_P/L_WITH_LEVERAGE_INCLUDING_CHARGES"                    AS "%_DAY_P/L"
FROM
    REALISED_INTRADAY_STOCK_HIST_RETURNS REALISED_INTRADAY
WHERE
    (SELECT DISTINCT PROC_DATE FROM PROCESSING_DATE WHERE PROC_TYP_CD = 'STOCK_PROC') BETWEEN REALISED_INTRADAY.START_DATE AND REALISED_INTRADAY.END_DATE

UNION ALL

SELECT
    'Realised Swing Stocks'                                                           AS PORTFOLIO_TYPE
    ,REALISED_SWING.TRADE_CLOSE_DATE                                                  AS PROCESSING_DATE
    ,NULL                                                                             AS PREV_PROCESSING_DATE
    ,NULL                                                                             AS NEXT_PROCESSING_DATE
    ,ROUND(REALISED_SWING.OPENING_BUY_PRICE + REALISED_SWING.OPEN_TOTAL_FEES,4)       AS INVESTED_AMOUNT
    ,ROUND(REALISED_SWING.OPENING_BUY_PRICE + REALISED_SWING.OPEN_TOTAL_FEES
     + REALISED_SWING."NET_P/L",4)                                                    AS CURRENT_VALUE
    ,0                                                                                AS PREVIOUS_VALUE
    ,REALISED_SWING."NET_P/L"                                                         AS "TOTAL_P/L"
    ,REALISED_SWING."NET_P/L"                                                         AS "DAY_P/L"
    ,REALISED_SWING."NET_%_P/L"                                                       AS "%_TOTAL_P/L"
    ,REALISED_SWING."NET_%_P/L"                                                       AS "%_DAY_P/L"
FROM
    REALISED_SWING_STOCK_HIST_RETURNS REALISED_SWING
WHERE
    (SELECT DISTINCT PROC_DATE FROM PROCESSING_DATE WHERE PROC_TYP_CD = 'STOCK_PROC') BETWEEN REALISED_SWING.START_DATE AND REALISED_SWING.END_DATE

UNION ALL

SELECT
    'Unrealised Swing Stocks'                                                         AS PORTFOLIO_TYPE
    ,UNREALISED_STOCK.PROCESSING_DATE                                                 AS PROCESSING_DATE
    ,UNREALISED_STOCK.PREV_PROCESSING_DATE                                            AS PREV_PROCESSING_DATE
    ,UNREALISED_STOCK.NEXT_PROCESSING_DATE                                            AS NEXT_PROCESSING_DATE
    ,UNREALISED_STOCK.TOTAL_AMOUNT_INVESTED_AS_ON_PROCESSING_DATE                     AS INVESTED_AMOUNT
    ,UNREALISED_STOCK.CURRENT_VALUE                                                   AS CURRENT_VALUE
    ,UNREALISED_STOCK.PREVIOUS_VALUE                                                  AS PREVIOUS_VALUE
    ,UNREALISED_STOCK."NET_P/L"                                                       AS "TOTAL_P/L"
    ,UNREALISED_STOCK."DAY_P/L"                                                       AS "DAY_P/L"
    ,UNREALISED_STOCK."%_NET_P/L"                                                     AS "%_TOTAL_P/L"
    ,UNREALISED_STOCK."%_DAY_P/L"                                                     AS "%_DAY_P/L"
FROM
    UNREALISED_SWING_STOCK_HIST_RETURNS UNREALISED_STOCK
INNER JOIN
    REALISED_SWING_STOCK_HIST_RETURNS REALISED_ENTRY -- TO PREVENT DUPLICATE RETURNS ENTRY FROM BOTH REALISED AND UNREALISED FOR THE SAME DATE
ON
    UNREALISED_STOCK.PROCESSING_DATE != REALISED_ENTRY.TRADE_CLOSE_DATE
WHERE
    (SELECT DISTINCT PROC_DATE FROM PROCESSING_DATE WHERE PROC_TYP_CD = 'STOCK_PROC') BETWEEN UNREALISED_STOCK.START_DATE AND UNREALISED_STOCK.END_DATE
;
'''