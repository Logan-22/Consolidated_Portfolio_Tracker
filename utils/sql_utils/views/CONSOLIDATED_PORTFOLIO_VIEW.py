CONSOLIDATED_PORTFOLIO_VIEW = '''
CREATE VIEW CONSOLIDATED_PORTFOLIO_VIEW AS
SELECT
    CAST('Mutual Funds' AS VARCHAR(100))                                              AS PORTFOLIO_TYPE
    ,MF.PROCESSING_DATE                                                               AS PROCESSING_DATE
    ,MF.PREVIOUS_PROCESSING_DATE                                                      AS PREVIOUS_PROCESSING_DATE
    ,MF.NEXT_PROCESSING_DATE                                                          AS NEXT_PROCESSING_DATE
    ,MF.FIN_AMC_AMOUNT                                                                AS INVESTED_AMOUNT
    ,MF.FIN_CURRENT_AMOUNT                                                            AS CURRENT_VALUE
    ,MF.FIN_PREVIOUS_AMOUNT                                                           AS PREVIOUS_VALUE
    ,MF."FIN_P/L"                                                                     AS "TOTAL_P/L"
    ,MF."FIN_DAY_P/L"                                                                 AS "DAY_P/L"
    ,MF."FIN_%_P/L"                                                                   AS "%_TOTAL_P/L"
    ,MF."FIN_%_DAY_P/L"                                                               AS "%_DAY_P/L"
FROM
    FIN_MUTUAL_FUND_RETURNS MF
WHERE
    MF.PROCESSING_DATE         = (SELECT DISTINCT PROC_DATE FROM PROCESSING_DATE WHERE PROC_TYP_CD = 'MF_PROC')
    AND MF.RECORD_DELETED_FLAG = 0

UNION ALL

SELECT
    'Intraday Stocks'                                                                 AS PORTFOLIO_TYPE
    ,INTRA.PROCESSING_DATE                                                            AS PROCESSING_DATE
    ,INTRA.PREVIOUS_PROCESSING_DATE                                                   AS PREVIOUS_PROCESSING_DATE
    ,INTRA.NEXT_PROCESSING_DATE                                                       AS NEXT_PROCESSING_DATE
    ,INTRA.AGG_ACTUAL_DEPLOYED_CAPITAL                                                AS INVESTED_AMOUNT
    ,ROUND(INTRA.AGG_ACTUAL_DEPLOYED_CAPITAL + 
     INTRA."NET_P/L_MINUS_CHARGES",4)                                                 AS CURRENT_VALUE
    ,0                                                                                AS PREVIOUS_VALUE
    ,INTRA."NET_P/L_MINUS_CHARGES"                                                    AS "TOTAL_P/L"
    ,INTRA."NET_P/L_MINUS_CHARGES"                                                    AS "DAY_P/L"
    ,INTRA."NET_%_P/L_WITH_LEVERAGE_INCL_CHARGES"                                     AS "%_TOTAL_P/L"
    ,INTRA."NET_%_P/L_WITH_LEVERAGE_INCL_CHARGES"                                     AS "%_DAY_P/L"
FROM
    FIN_REALISED_INTRADAY_STOCK_RETURNS INTRA
WHERE
    INTRA.PROCESSING_DATE         = (SELECT DISTINCT PROC_DATE FROM PROCESSING_DATE WHERE PROC_TYP_CD = 'STOCK_PROC')
    AND INTRA.RECORD_DELETED_FLAG = 0

UNION ALL

SELECT
    'Realised Swing Stocks'                                                           AS PORTFOLIO_TYPE
    ,SWING.PROCESSING_DATE                                                            AS PROCESSING_DATE
    ,SWING.PREVIOUS_PROCESSING_DATE                                                   AS PREVIOUS_PROCESSING_DATE
    ,SWING.NEXT_PROCESSING_DATE                                                       AS NEXT_PROCESSING_DATE
    ,ROUND(SWING.AGG_OPENING_BUY_PRICE + SWING.OPEN_TOTAL_FEES,4)                     AS INVESTED_AMOUNT
    ,ROUND(SWING.AGG_OPENING_BUY_PRICE + SWING.OPEN_TOTAL_FEES
     + SWING."NET_P/L",4)                                                             AS CURRENT_VALUE
    ,0                                                                                AS PREVIOUS_VALUE
    ,SWING."NET_P/L"                                                                  AS "TOTAL_P/L"
    ,SWING."NET_P/L"                                                                  AS "DAY_P/L"
    ,SWING."NET_%_P/L"                                                                AS "%_TOTAL_P/L"
    ,SWING."NET_%_P/L"                                                                AS "%_DAY_P/L"
FROM
    FIN_REALISED_SWING_STOCK_RETURNS SWING
WHERE
    SWING.PROCESSING_DATE         = (SELECT DISTINCT PROC_DATE FROM PROCESSING_DATE WHERE PROC_TYP_CD = 'STOCK_PROC')
    AND SWING.RECORD_DELETED_FLAG = 0

UNION ALL

SELECT
    'Unrealised Swing Stocks'                                                         AS PORTFOLIO_TYPE
    ,UNREALISED_STOCK.PROCESSING_DATE                                                 AS PROCESSING_DATE
    ,UNREALISED_STOCK.PREVIOUS_PROCESSING_DATE                                        AS PREVIOUS_PROCESSING_DATE
    ,UNREALISED_STOCK.NEXT_PROCESSING_DATE                                            AS NEXT_PROCESSING_DATE
    ,UNREALISED_STOCK.FIN_TOTAL_INVESTED_AMOUNT                                       AS INVESTED_AMOUNT
    ,UNREALISED_STOCK.FIN_CURRENT_VALUE                                               AS CURRENT_VALUE
    ,UNREALISED_STOCK.FIN_PREVIOUS_VALUE                                              AS PREVIOUS_VALUE
    ,UNREALISED_STOCK."FIN_NET_P/L"                                                   AS "TOTAL_P/L"
    ,UNREALISED_STOCK."FIN_DAY_P/L"                                                   AS "DAY_P/L"
    ,UNREALISED_STOCK."FIN_NET_%_P/L"                                                 AS "%_TOTAL_P/L"
    ,UNREALISED_STOCK."FIN_%_DAY_P/L"                                                 AS "%_DAY_P/L"
FROM
    FIN_UNREALISED_STOCK_RETURNS UNREALISED_STOCK
INNER JOIN
    FIN_REALISED_SWING_STOCK_RETURNS REALISED_ENTRY -- TO PREVENT DUPLICATE RETURNS ENTRY FROM BOTH REALISED AND UNREALISED FOR THE SAME DATE
ON
    UNREALISED_STOCK.PROCESSING_DATE != REALISED_ENTRY.TRADE_CLOSE_DATE
WHERE
    UNREALISED_STOCK.PROCESSING_DATE = (SELECT DISTINCT PROC_DATE FROM PROCESSING_DATE WHERE PROC_TYP_CD = 'STOCK_PROC')
;
'''