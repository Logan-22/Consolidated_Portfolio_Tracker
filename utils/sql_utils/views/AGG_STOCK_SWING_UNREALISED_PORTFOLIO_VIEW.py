AGG_STOCK_SWING_UNREALISED_PORTFOLIO_VIEW = '''
CREATE VIEW AGG_STOCK_SWING_UNREALISED_PORTFOLIO_VIEW AS
SELECT
    SSUPV.STOCK_NAME                                                                      AS STOCK_NAME
    ,SSUPV.PROCESSING_DATE                                                                AS PROCESSING_DATE
    ,SSUPV.CURRENT_PRICE                                                                  AS CURRENT_PRICE
    ,SSUPV.PREVIOUS_PROCESSING_DATE                                                       AS PREVIOUS_PROCESSING_DATE
    ,SSUPV.PREVIOUS_PRICE                                                                 AS PREVIOUS_PRICE
    ,SUM(SSUPV.STOCK_QUANTITY)                                                            AS AGG_STOCK_QUANTITY
    ,SUM(SSUPV.INVESTED_AMOUNT)/ SUM(SSUPV.STOCK_QUANTITY)                                AS AVG_TRADE_PRICE
    ,SUM(SSUPV.INVESTED_AMOUNT)                                                           AS AGG_INVESTED_AMOUNT
    ,SUM(SSUPV.TOTAL_FEES)                                                                AS AGG_TOTAL_FEES
    ,SUM(SSUPV.TOTAL_INVESTED_AMOUNT)                                                     AS AGG_TOTAL_INVESTED_AMOUNT
    ,SUM(SSUPV.CURRENT_VALUE)                                                             AS AGG_CURRENT_VALUE
    ,SUM(SSUPV."P/L")                                                                     AS "AGG_P/L"
    ,ROUND(SUM(SSUPV."P/L")/SUM(SSUPV.TOTAL_INVESTED_AMOUNT) * 100,2)                     AS "AGG_%_P/L"
    ,SUM(SSUPV."NET_P/L")                                                                 AS "AGG_NET_P/L"
    ,ROUND(SUM(SSUPV."NET_P/L")/SUM(SSUPV.TOTAL_INVESTED_AMOUNT) * 100,2)                 AS "AGG_%_NET_P/L"
    ,SUM(SSUPV.PREVIOUS_VALUE)                                                            AS AGG_PREVIOUS_VALUE
    ,SUM(SSUPV."DAY_P/L")                                                                 AS "AGG_DAY_P/L"
    ,ROUND(SUM(SSUPV."DAY_P/L")/SUM(SSUPV.PREVIOUS_VALUE) * 100,2)                        AS "AGG_%_DAY_P/L"
FROM
    STOCK_SWING_UNREALISED_PORTFOLIO_VIEW SSUPV
GROUP BY 1,2,3,4,5
ORDER BY 1,2,3,4,5
;
'''