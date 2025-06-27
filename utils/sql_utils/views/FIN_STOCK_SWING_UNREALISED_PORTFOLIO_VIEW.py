FIN_STOCK_SWING_UNREALISED_PORTFOLIO_VIEW = '''
CREATE VIEW FIN_STOCK_SWING_UNREALISED_PORTFOLIO_VIEW AS 
SELECT
    ASSUPV.PROCESSING_DATE                                                                AS PROCESSING_DATE
    ,ASSUPV.PREVIOUS_PROCESSING_DATE                                                      AS PREVIOUS_PROCESSING_DATE
    ,ROUND(SUM(ASSUPV.AGG_INVESTED_AMOUNT),4)                                             AS FIN_INVESTED_AMOUNT
    ,ROUND(SUM(ASSUPV.AGG_TOTAL_FEES),4)                                                  AS FIN_TOTAL_FEES
    ,ROUND(SUM(ASSUPV.AGG_TOTAL_INVESTED_AMOUNT),4)                                       AS FIN_TOTAL_INVESTED_AMOUNT
    ,ROUND(SUM(ASSUPV.AGG_CURRENT_VALUE),4)                                               AS FIN_CURRENT_VALUE
    ,ROUND(SUM(ASSUPV.AGG_PREVIOUS_VALUE),4)                                              AS FIN_PREVIOUS_VALUE
    ,ROUND(SUM(ASSUPV."AGG_P/L"),4)                                                       AS "FIN_P/L"
    ,ROUND(SUM(ASSUPV."AGG_P/L")/SUM(ASSUPV.AGG_TOTAL_INVESTED_AMOUNT) * 100,2)           AS "FIN_%_P/L"
    ,ROUND(SUM(ASSUPV."AGG_NET_P/L"),4)                                                   AS "FIN_NET_P/L"
    ,ROUND(SUM(ASSUPV."AGG_NET_P/L")/SUM(ASSUPV.AGG_TOTAL_INVESTED_AMOUNT) * 100,2)       AS "FIN_NET_%_P/L"
    ,ROUND(SUM(ASSUPV."AGG_DAY_P/L"),4)                                                   AS "FIN_DAY_P/L"
    ,ROUND(SUM(ASSUPV."AGG_DAY_P/L")/SUM(ASSUPV.AGG_PREVIOUS_VALUE) * 100,2)              AS "FIN_%_DAY_P/L"
FROM
    AGG_STOCK_SWING_UNREALISED_PORTFOLIO_VIEW ASSUPV
GROUP BY 1,2
;
'''