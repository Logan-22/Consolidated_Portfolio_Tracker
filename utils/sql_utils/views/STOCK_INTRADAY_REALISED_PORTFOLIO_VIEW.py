STOCK_INTRADAY_REALISED_PORTFOLIO_VIEW = '''
CREATE VIEW STOCK_INTRADAY_REALISED_PORTFOLIO_VIEW AS
SELECT
    FINAL_SUB.TRADE_DATE                                       AS TRADE_DATE
    ,FINAL_SUB.STOCK_NAME                                      AS STOCK_NAME
    ,FINAL_SUB.TRADE_SET_ID                                    AS TRADE_SET_ID
    ,FINAL_SUB.TRADE_SET                                       AS TRADE_SET
    ,FINAL_SUB.LEVERAGE                                        AS LEVERAGE
    ,FINAL_SUB.TRADE_TYPE                                      AS TRADE_TYPE
    ,FINAL_SUB.TRADE_POSITION                                  AS TRADE_POSITION
    ,FINAL_SUB.FEE_ID                                          AS FEE_ID
    ,FINAL_SUB.STOCK_QUANTITY                                  AS STOCK_QUANTITY
    ,FINAL_SUB.PERCEIVED_BUY_PRICE                             AS PERCEIVED_BUY_PRICE
    ,FINAL_SUB.PERCEIVED_SELL_PRICE                            AS PERCEIVED_SELL_PRICE
    ,FINAL_SUB.ACTUAL_BUY_PRICE                                AS ACTUAL_BUY_PRICE
    ,FINAL_SUB.ACTUAL_SELL_PRICE                               AS ACTUAL_SELL_PRICE
    ,FINAL_SUB."P/L"                                           AS "P/L"
    ,FINAL_SUB.PERCEIVED_DEPLOYED_CAPITAL                      AS PERCEIVED_DEPLOYED_CAPITAL
    ,FINAL_SUB.ACTUAL_DEPLOYED_CAPITAL                         AS ACTUAL_DEPLOYED_CAPITAL
    ,ROUND(
     FINAL_SUB."P/L"/FINAL_SUB.PERCEIVED_DEPLOYED_CAPITAL * 100
     ,2)                                                       AS "%_P/L_WITHOUT_LEVERAGE"
    ,ROUND(
     FINAL_SUB."P/L"/FINAL_SUB.ACTUAL_DEPLOYED_CAPITAL * 100
     ,2)                                                       AS "%_P/L_WITH_LEVERAGE"
    ,FINAL_SUB.PROCESSING_DATE                                 AS PROCESSING_DATE
    ,FINAL_SUB.PREVIOUS_PROCESSING_DATE                        AS PREVIOUS_PROCESSING_DATE
    ,FINAL_SUB.NEXT_PROCESSING_DATE                            AS NEXT_PROCESSING_DATE
FROM
(
SELECT
    OUTER_SUB.TRADE_DATE                                       AS TRADE_DATE
    ,OUTER_SUB.STOCK_NAME                                      AS STOCK_NAME
    ,OUTER_SUB.TRADE_SET_ID                                    AS TRADE_SET_ID
    ,OUTER_SUB.TRADE_SET                                       AS TRADE_SET
    ,OUTER_SUB.LEVERAGE                                        AS LEVERAGE
    ,OUTER_SUB.TRADE_TYPE                                      AS TRADE_TYPE
    ,OUTER_SUB.TRADE_POSITION                                  AS TRADE_POSITION
    ,OUTER_SUB.FEE_ID                                          AS FEE_ID
    ,OUTER_SUB.STOCK_QUANTITY                                  AS STOCK_QUANTITY
    ,ROUND(OUTER_SUB.PERCEIVED_BUY_PRICE,4)                    AS PERCEIVED_BUY_PRICE
    ,ROUND(OUTER_SUB.PERCEIVED_SELL_PRICE,4)                   AS PERCEIVED_SELL_PRICE
    ,ROUND(OUTER_SUB.ACTUAL_BUY_PRICE,4)                       AS ACTUAL_BUY_PRICE
    ,ROUND(OUTER_SUB.ACTUAL_SELL_PRICE,4)                      AS ACTUAL_SELL_PRICE
    ,ROUND(OUTER_SUB."P/L", 4)                                 AS "P/L"
    ,ROUND(CASE 
     WHEN OUTER_SUB.TRADE_POSITION = 'Long'
     THEN OUTER_SUB.PERCEIVED_BUY_PRICE
     WHEN OUTER_SUB.TRADE_POSITION = 'Short'
     THEN OUTER_SUB.PERCEIVED_SELL_PRICE
     END,4)                                                    AS PERCEIVED_DEPLOYED_CAPITAL
    ,ROUND(CASE 
     WHEN OUTER_SUB.TRADE_POSITION = 'Long'
     THEN OUTER_SUB.ACTUAL_BUY_PRICE
     WHEN OUTER_SUB.TRADE_POSITION = 'Short'
     THEN OUTER_SUB.ACTUAL_SELL_PRICE
     END,4)                                                    AS ACTUAL_DEPLOYED_CAPITAL
    ,OUTER_SUB.PROCESSING_DATE                                 AS PROCESSING_DATE
    ,OUTER_SUB.PREVIOUS_PROCESSING_DATE                        AS PREVIOUS_PROCESSING_DATE
    ,OUTER_SUB.NEXT_PROCESSING_DATE                            AS NEXT_PROCESSING_DATE
FROM
(
SELECT
    INNER_SUB.TRADE_DATE                                       AS TRADE_DATE
    ,INNER_SUB.STOCK_NAME                                      AS STOCK_NAME
    ,INNER_SUB.TRADE_SET_ID                                    AS TRADE_SET_ID
    ,INNER_SUB.TRADE_SET                                       AS TRADE_SET
    ,INNER_SUB.LEVERAGE                                        AS LEVERAGE
    ,INNER_SUB.TRADE_TYPE                                      AS TRADE_TYPE
    ,INNER_SUB.TRADE_POSITION                                  AS TRADE_POSITION
    ,INNER_SUB.FEE_ID                                          AS FEE_ID
    ,MAX(INNER_SUB.AGG_STOCK_QUANTITY)                         AS STOCK_QUANTITY --MAX SINCE STOCK QUANTITY WITHIN THE SAME SET WILL BE SAME
    ,MAX(CASE WHEN INNER_SUB.BUY_OR_SELL = 'B'
          THEN INNER_SUB.AGG_BUY_OR_SELL_PRICE END)            AS PERCEIVED_BUY_PRICE -- MAX TO COMBINE BOTH BUY AND SELL LEGS
    ,MAX(CASE WHEN INNER_SUB.BUY_OR_SELL = 'S'
          THEN INNER_SUB.AGG_BUY_OR_SELL_PRICE END)            AS PERCEIVED_SELL_PRICE
    ,MAX(CASE WHEN INNER_SUB.BUY_OR_SELL = 'B'
          THEN INNER_SUB.AGG_ACTUAL_BUY_OR_SELL_PRICE END)     AS ACTUAL_BUY_PRICE
    ,MAX(CASE WHEN INNER_SUB.BUY_OR_SELL = 'S'
          THEN INNER_SUB.AGG_ACTUAL_BUY_OR_SELL_PRICE END)     AS ACTUAL_SELL_PRICE
    ,MAX(CASE WHEN INNER_SUB.BUY_OR_SELL = 'S'
          THEN INNER_SUB.AGG_BUY_OR_SELL_PRICE END) -
     MAX(CASE WHEN INNER_SUB.BUY_OR_SELL = 'B'
          THEN INNER_SUB.AGG_BUY_OR_SELL_PRICE END)            AS "P/L"
    ,INNER_SUB.PROCESSING_DATE                                 AS PROCESSING_DATE
    ,INNER_SUB.PREVIOUS_PROCESSING_DATE                        AS PREVIOUS_PROCESSING_DATE
    ,INNER_SUB.NEXT_PROCESSING_DATE                            AS NEXT_PROCESSING_DATE
FROM
(SELECT
    SUB.TRADE_DATE                                             AS TRADE_DATE
    ,SUB.STOCK_NAME                                            AS STOCK_NAME
    ,SUB.TRADE_SET_ID                                          AS TRADE_SET_ID
    ,SUB.TRADE_SET                                             AS TRADE_SET
    ,SUB.BUY_OR_SELL                                           AS BUY_OR_SELL
    ,SUB.LEVERAGE                                              AS LEVERAGE
    ,SUB.TRADE_TYPE                                            AS TRADE_TYPE
    ,SUB.TRADE_POSITION                                        AS TRADE_POSITION
    ,SUB.FEE_ID                                                AS FEE_ID
    ,SUM(SUB.STOCK_QUANTITY)                                   AS AGG_STOCK_QUANTITY
    ,SUM(SUB.BUY_OR_SELL_PRICE)                                AS AGG_BUY_OR_SELL_PRICE
    ,SUM(SUB.ACTUAL_BUY_OR_SELL_PRICE)                         AS AGG_ACTUAL_BUY_OR_SELL_PRICE
    ,SUB.PROCESSING_DATE                                       AS PROCESSING_DATE
    ,SUB.PREVIOUS_PROCESSING_DATE                              AS PREVIOUS_PROCESSING_DATE
    ,SUB.NEXT_PROCESSING_DATE                                  AS NEXT_PROCESSING_DATE
FROM
(
SELECT
    TRD.TRADE_DATE                                             AS TRADE_DATE
    ,TRD.STOCK_NAME                                            AS STOCK_NAME
    ,TRD.TRADE_SET_ID                                          AS TRADE_SET_ID
    ,TRD.TRADE_SET                                             AS TRADE_SET
    ,TRD.BUY_OR_SELL                                           AS BUY_OR_SELL
    ,TRD.LEVERAGE                                              AS LEVERAGE
    ,TRD.TRADE_TYPE                                            AS TRADE_TYPE
    ,TRD.TRADE_POSITION                                        AS TRADE_POSITION
    ,TRD.FEE_ID                                                AS FEE_ID
    ,TRD.NET_TRADE_PRICE_PER_UNIT                              AS BUY_OR_SELL_PRICE_PER_UNIT
    ,TRD.STOCK_QUANTITY                                        AS STOCK_QUANTITY
    ,ROUND(TRD.STOCK_QUANTITY *
     TRD.NET_TRADE_PRICE_PER_UNIT, 4)                          AS BUY_OR_SELL_PRICE
    ,ROUND(TRD.STOCK_QUANTITY *
     TRD.NET_TRADE_PRICE_PER_UNIT / TRD.LEVERAGE, 4)           AS ACTUAL_BUY_OR_SELL_PRICE
    ,(SELECT PROC_DATE FROM PROCESSING_DATE 
     WHERE PROC_TYP_CD = 'STOCK_PROC')                         AS PROCESSING_DATE
    ,(SELECT PREV_PROC_DATE FROM PROCESSING_DATE 
     WHERE PROC_TYP_CD = 'STOCK_PROC')                         AS PREVIOUS_PROCESSING_DATE
    ,(SELECT NEXT_PROC_DATE FROM PROCESSING_DATE 
     WHERE PROC_TYP_CD = 'STOCK_PROC')                         AS NEXT_PROCESSING_DATE
FROM
    TRADES TRD
WHERE
    TRD.TRADE_EXIT_DATE         IS NOT NULL -- REALISED P/L
    AND TRD.RECORD_DELETED_FLAG = 0
    AND TRD.TRADE_DATE          = (SELECT PROC_DATE FROM PROCESSING_DATE WHERE PROC_TYP_CD = 'STOCK_PROC')
) SUB
GROUP BY 1,2,3,4,5,6,7,8,9,13,14,15
) INNER_SUB
GROUP BY 1,2,3,4,5,6,7,8,15,16,17
) OUTER_SUB
) FINAL_SUB
;
'''