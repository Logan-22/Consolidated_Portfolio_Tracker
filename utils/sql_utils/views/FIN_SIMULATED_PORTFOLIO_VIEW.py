FIN_SIMULATED_PORTFOLIO_VIEW = '''
CREATE VIEW FIN_SIMULATED_PORTFOLIO_VIEW AS
SELECT
    ASPV.AGG_SIM_FUND_NAME                                                                AS FIN_SIM_FUND_NAME
    ,ASPV.AGG_SIM_ALLOCATION_CATEGORY                                                     AS FIN_SIM_ALLOCATION_CATEGORY
    ,ASPV.AGG_SIM_PROCESSING_DATE                                                         AS FIN_SIM_PROCESSING_DATE
    ,ROUND(SUM(ASPV.AGG_SIM_INVESTED_AMOUNT),4)                                           AS FIN_SIM_INVESTED_AMOUNT
    ,ROUND(SUM(ASPV.AGG_SIM_FUND_UNITS),4)                                                AS FIN_SIM_FUND_UNITS
    ,ROUND(SUM(ASPV.AGG_SIM_CURRENT_AMOUNT),4)                                            AS FIN_SIM_CURRENT_AMOUNT
    ,ROUND(SUM(ASPV.AGG_SIM_PREVIOUS_AMOUNT),4)                                           AS FIN_SIM_PREVIOUS_AMOUNT
    ,ROUND(SUM(ASPV."AGG_SIM_P/L"),4)                                                     AS "FIN_SIM_P/L"
    ,ROUND(SUM(ASPV."AGG_SIM_DAY_P/L"),4)                                                 AS "FIN_SIM_DAY_P/L"
    ,ROUND(SUM(ASPV."AGG_SIM_P/L") / SUM(ASPV.AGG_SIM_INVESTED_AMOUNT) * 100, 2)          AS "FIN_%_SIM_P/L"
    ,ROUND(SUM(ASPV."AGG_SIM_DAY_P/L") / SUM(ASPV.AGG_SIM_INVESTED_AMOUNT) * 100, 2)      AS "FIN_%_SIM_DAY_P/L"
    ,ROUND(SUM(ASPV.AGG_SIM_INVESTED_AMOUNT)/ SUM(ASPV.AGG_SIM_FUND_UNITS), 4)            AS FIN_SIM_AVG_PRICE
FROM
    AGG_SIMULATED_PORTFOLIO_VIEW ASPV
;
'''