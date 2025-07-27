AGG_STOCK_INTRADAY_REALISED_PORTFOLIO_VIEW = '''
CREATE VIEW AGG_STOCK_INTRADAY_REALISED_PORTFOLIO_VIEW AS
SELECT
    SRPV.TRADE_DATE                                            AS TRADE_DATE
    ,SRPV.STOCK_NAME                                           AS STOCK_NAME
    ,SRPV.LEVERAGE                                             AS LEVERAGE
    ,SRPV.TRADE_TYPE                                           AS TRADE_TYPE
    ,SRPV.FEE_ID                                               AS FEE_ID
    ,SUM(SRPV.PERCEIVED_DEPLOYED_CAPITAL)                      AS AGG_PERCEIVED_DEPLOYED_CAPITAL
    ,SUM(SRPV.ACTUAL_DEPLOYED_CAPITAL)                         AS AGG_ACTUAL_DEPLOYED_CAPITAL
    ,SUM(SRPV."P/L")                                           AS "AGG_P/L"
    ,ROUND(SUM(SRPV."P/L")/SUM(SRPV.PERCEIVED_DEPLOYED_CAPITAL)
     * 100,2)                                                  AS "%_P/L_WITHOUT_LEVERAGE"
    ,ROUND(SUM(SRPV."P/L")/SUM(SRPV.ACTUAL_DEPLOYED_CAPITAL)
     * 100,2)                                                  AS "%_P/L_WITH_LEVERAGE"
    ,SRPV.PROCESSING_DATE                                      AS PROCESSING_DATE
    ,SRPV.PREVIOUS_PROCESSING_DATE                             AS PREVIOUS_PROCESSING_DATE
    ,SRPV.NEXT_PROCESSING_DATE                                 AS NEXT_PROCESSING_DATE
FROM
    STOCK_INTRADAY_REALISED_PORTFOLIO_VIEW SRPV
GROUP BY 1,2,3,4,5,11,12,13
;
'''