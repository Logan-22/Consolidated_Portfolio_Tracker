FIN_CONSOLIDATED_PORTFOLIO_VIEW = '''
CREATE VIEW FIN_CONSOLIDATED_PORTFOLIO_VIEW AS
SELECT
     MAX(ACPV.PROCESSING_DATE)                                                        AS PROCESSING_DATE
    ,MAX(ACPV.PREV_PROCESSING_DATE)                                                   AS PREV_PROCESSING_DATE
    ,MAX(ACPV.NEXT_PROCESSING_DATE)                                                   AS NEXT_PROCESSING_DATE
    ,ROUND(SUM(ACPV.AGG_INVESTED_AMOUNT),4)                                           AS FIN_INVESTED_AMOUNT
    ,ROUND(SUM(ACPV.AGG_CURRENT_VALUE),4)                                             AS FIN_CURRENT_VALUE
    ,ROUND(SUM(ACPV.AGG_PREVIOUS_VALUE),4)                                            AS FIN_PREVIOUS_VALUE
    ,ROUND(SUM(ACPV."AGG_TOTAL_P/L"),4)                                               AS "FIN_TOTAL_P/L"
    ,ROUND(SUM(ACPV."AGG_DAY_P/L"),4)                                                 AS "FIN_DAY_P/L"
    ,ROUND(SUM(ACPV."AGG_TOTAL_P/L")/SUM(ACPV.AGG_INVESTED_AMOUNT) * 100, 2)          AS "%_FIN_TOTAL_P/L"
    ,ROUND(SUM(ACPV."AGG_DAY_P/L")/SUM(ACPV.AGG_PREVIOUS_VALUE) * 100, 2)             AS "%_FIN_DAY_P/L"
FROM
    AGG_CONSOLIDATED_PORTFOLIO_VIEW ACPV
WHERE
    ACPV.PORTFOLIO_TYPE NOT IN ('Intraday Stocks', 'Realised Swing Stocks')
;
'''