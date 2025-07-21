AGG_SIMULATED_PORTFOLIO_VIEW = '''
CREATE VIEW AGG_SIMULATED_PORTFOLIO_VIEW AS
SELECT
    SMFPV.SIM_FUND_NAME                                                                   AS AGG_SIM_FUND_NAME
    ,SMFPV.SIM_BASE_TYPE                                                                  AS AGG_SIM_BASE_TYPE
    ,SMFPV.SIM_ALLOCATION_CATEGORY                                                        AS AGG_SIM_ALLOCATION_CATEGORY
    ,SMFPV.SIM_PROCESSING_DATE                                                            AS AGG_SIM_PROCESSING_DATE
    ,ROUND(SUM(SMFPV.SIM_INVESTED_AMOUNT),4)                                              AS AGG_SIM_INVESTED_AMOUNT
    ,ROUND(SUM(SMFPV.SIM_FUND_UNITS),4)                                                   AS AGG_SIM_FUND_UNITS
    ,ROUND(SUM(SMFPV.SIM_CURRENT_AMOUNT),4)                                               AS AGG_SIM_CURRENT_AMOUNT
    ,ROUND(SUM(SMFPV.SIM_PREVIOUS_AMOUNT),4)                                              AS AGG_SIM_PREVIOUS_AMOUNT
    ,ROUND(SUM(SMFPV."SIM_P/L"),4)                                                        AS "AGG_SIM_P/L"
    ,ROUND(SUM(SMFPV."SIM_DAY_P/L"),4)                                                    AS "AGG_SIM_DAY_P/L"
    ,ROUND(SUM(SMFPV."SIM_P/L") / SUM(SMFPV.SIM_INVESTED_AMOUNT) * 100, 2)                AS "AGG_%_SIM_P/L"
    ,ROUND(SUM(SMFPV."SIM_DAY_P/L") / SUM(SMFPV.SIM_INVESTED_AMOUNT) * 100, 2)            AS "AGG_%_SIM_DAY_P/L"
    ,ROUND(SUM(SMFPV.SIM_INVESTED_AMOUNT)/ SUM(SMFPV.SIM_FUND_UNITS), 4)                  AS AGG_SIM_AVG_PRICE
FROM
    SIMULATED_PORTFOLIO_VIEW SMFPV
GROUP BY 1,2,3,4
ORDER BY 1,2,3,4
;
'''