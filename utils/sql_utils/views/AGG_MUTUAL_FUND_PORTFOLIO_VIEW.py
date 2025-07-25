AGG_MUTUAL_FUND_PORTFOLIO_VIEW = '''
CREATE VIEW AGG_MUTUAL_FUND_PORTFOLIO_VIEW AS
SELECT
    MFPV.FUND_NAME                           AS FUND_NAME
    ,MFPV.FUND_AMC                           AS FUND_AMC
    ,MFPV.FUND_TYPE                          AS FUND_TYPE
    ,MFPV.FUND_CATEGORY                      AS FUND_CATEGORY
    ,MFPV.ALLOCATION_CATEGORY                AS ALLOCATION_CATEGORY
    ,ROUND(SUM(MFPV.INVESTED_AMOUNT),4)      AS AGG_INVESTED_AMOUNT
    ,ROUND(SUM(MFPV.AMC_AMOUNT),4)           AS AGG_AMC_AMOUNT
    ,ROUND(SUM(MFPV.FUND_UNITS),4)           AS AGG_FUND_UNITS
    ,ROUND(SUM(MFPV.CURRENT_AMOUNT),4)       AS AGG_CURRENT_AMOUNT
    ,ROUND(SUM(MFPV.PREVIOUS_AMOUNT),4)      AS AGG_PREVIOUS_AMOUNT
    ,ROUND(SUM(MFPV."P/L"),4)                AS "AGG_P/L"
    ,ROUND(SUM(MFPV."DAY_P/L"),4)            AS "AGG_DAY_P/L"
    ,ROUND(ROUND(SUM(MFPV."P/L"),4) 
    / ROUND(SUM(MFPV.AMC_AMOUNT),4),4) * 100 AS "AGG_%_P/L"
    ,ROUND(ROUND(SUM(MFPV."DAY_P/L"),4) 
    / ROUND(SUM(MFPV.AMC_AMOUNT),4),4) * 100 AS "AGG_%_DAY_P/L"
    ,ROUND(ROUND(SUM(MFPV.AMC_AMOUNT),4)
    / ROUND(SUM(MFPV.FUND_UNITS),4),4)       AS AGG_AVG_PRICE
    ,MFPV.PROCESSING_DATE                    AS PROCESSING_DATE
    ,MFPV.PREVIOUS_PROCESSING_DATE           AS PREVIOUS_PROCESSING_DATE
    ,MFPV.NEXT_PROCESSING_DATE               AS NEXT_PROCESSING_DATE
FROM
    MUTUAL_FUND_PORTFOLIO_VIEW MFPV
GROUP BY 1,2,3,4,5,16,17,18
;
'''