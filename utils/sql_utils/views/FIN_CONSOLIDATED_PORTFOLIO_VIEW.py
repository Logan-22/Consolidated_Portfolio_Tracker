FIN_CONSOLIDATED_PORTFOLIO_VIEW = '''
CREATE VIEW FIN_CONSOLIDATED_PORTFOLIO_VIEW AS
SELECT
    (SELECT DISTINCT PROC_DATE FROM PROCESSING_DATE WHERE PROC_TYP_CD 
     IN ('MF_PROC','STOCK_PROC'))                                                     AS PROCESSING_DATE
    ,(SELECT DISTINCT PREV_PROC_DATE FROM PROCESSING_DATE WHERE PROC_TYP_CD 
     IN ('MF_PROC','STOCK_PROC'))                                                     AS PREVIOUS_PROCESSING_DATE
    ,(SELECT DISTINCT NEXT_PROC_DATE FROM PROCESSING_DATE WHERE PROC_TYP_CD 
     IN ('MF_PROC','STOCK_PROC'))                                                     AS NEXT_PROCESSING_DATE
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