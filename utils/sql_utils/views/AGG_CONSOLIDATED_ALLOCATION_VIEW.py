AGG_CONSOLIDATED_ALLOCATION_VIEW = '''
CREATE VIEW AGG_CONSOLIDATED_ALLOCATION_VIEW AS
SELECT
    AMFPV.ALLOCATION_CATEGORY                                                         AS PORTFOLIO_TYPE
    ,AMFPV.FUND_NAME                                                                  AS PORTFOLIO_NAME
    ,AMFPV.FUND_AMC                                                                   AS PORTFOLIO_HOUSE
    ,AMFPV.FUND_TYPE                                                                  AS PORTFOLIO_SUB_TYPE
    ,AMFPV.FUND_CATEGORY                                                              AS PORTFOLIO_CATEGORY
    ,AMFPV.AGG_INVESTED_AMOUNT                                                        AS INVESTED_AMOUNT
    ,AMFPV.AGG_AMC_AMOUNT                                                             AS INVESTED_AMOUNT_EXCLUDING_FEES
    ,AMFPV.AGG_FUND_UNITS                                                             AS QUANTITY
    ,AMFPV.AGG_CURRENT_AMOUNT                                                         AS CURRENT_VALUE
    ,AMFPV.AGG_PREVIOUS_AMOUNT                                                        AS PREVIOUS_VALUE
    ,AMFPV."AGG_P/L"                                                                  AS "P/L"
    ,AMFPV."AGG_%P/L"                                                                 AS "%_P_L"
    ,AMFPV."AGG_DAY_P/L"                                                              AS "DAY_P/L"
    ,AMFPV."AGG_%DAY_P/L"                                                             AS "%_DAY_P/L"
    ,AMFPV.AGG_AVG_PRICE                                                              AS AVERAGE_PRICE
    ,AMFPV_AGG.AGG_INVESTED_AMOUNT                                                    AS AGG_INVESTED_AMOUNT
    ,AMFPV_AGG.AGG_INVESTED_AMOUNT_EXCLUDING_FEES                                     AS AGG_INVESTED_AMOUNT_EXCLUDING_FEES
    ,AMFPV_AGG.AGG_QUANTITY                                                           AS AGG_QUANTITY
    ,AMFPV_AGG.AGG_CURRENT_AMOUNT                                                     AS AGG_CURRENT_AMOUNT
    ,AMFPV_AGG.AGG_PREVIOUS_AMOUNT                                                    AS AGG_PREVIOUS_AMOUNT
    ,AMFPV_AGG."AGG_P/L"                                                              AS "AGG_P/L"
    ,AMFPV_AGG."AGG_DAY_P/L"                                                          AS "AGG_DAY_P/L"
    ,CASE WHEN AMFPV_AGG.AGG_INVESTED_AMOUNT < 100 THEN 'ABSOLUTE'
                                                 ELSE 'NET' END                       AS "AGG_ALLOC_%_PORTFOLIO_INVESTED_AMOUNT_TYP_CD"
    ,CASE WHEN AMFPV_AGG.AGG_INVESTED_AMOUNT < 100 
     THEN ROUND(ABS(AMFPV.AGG_INVESTED_AMOUNT)
           /ABS(AMFPV_AGG.ABS_AGG_INVESTED_AMOUNT) * 100, 2)
     ELSE ROUND(AMFPV.AGG_INVESTED_AMOUNT
           /AMFPV_AGG.AGG_INVESTED_AMOUNT * 100, 2) END                               AS "AGG_ALLOC_%_PORTFOLIO_INVESTED_AMOUNT"
    ,CASE WHEN AMFPV_AGG.AGG_INVESTED_AMOUNT_EXCLUDING_FEES < 100 THEN 'ABSOLUTE'
                                                                ELSE 'NET' END        AS "AGG_ALLOC_%_PORTFOLIO_INVESTED_AMOUNT_EXCLUDING_FEES_TYP_CD"
    ,CASE WHEN AMFPV_AGG.AGG_INVESTED_AMOUNT_EXCLUDING_FEES < 100
     THEN ROUND(ABS(AMFPV.AGG_AMC_AMOUNT)
           /ABS(AMFPV_AGG.ABS_AGG_INVESTED_AMOUNT_EXCLUDING_FEES) * 100, 2)
     ELSE ROUND(AMFPV.AGG_AMC_AMOUNT
           /AMFPV_AGG.AGG_INVESTED_AMOUNT_EXCLUDING_FEES * 100, 2) END                AS "AGG_ALLOC_%_PORTFOLIO_INVESTED_AMOUNT_EXCLUDING_FEES"
    ,CASE WHEN AMFPV_AGG.AGG_QUANTITY < 100 THEN 'ABSOLUTE'
                                          ELSE 'NET' END                              AS "AGG_ALLOC_%_QUANTITY_TYP_CD"
    ,CASE WHEN AMFPV_AGG.AGG_QUANTITY < 100
     THEN ROUND(ABS(AMFPV.AGG_FUND_UNITS)
           /ABS(AMFPV_AGG.ABS_AGG_QUANTITY) * 100, 2)
     ELSE ROUND(AMFPV.AGG_FUND_UNITS
           /AMFPV_AGG.AGG_QUANTITY * 100, 2) END                                      AS "AGG_ALLOC_%_QUANTITY"
    ,CASE WHEN AMFPV_AGG.AGG_CURRENT_AMOUNT < 100 THEN 'ABSOLUTE'
                                                ELSE 'NET' END                        AS "AGG_ALLOC_%_CURRENT_VALUE_TYP_CD"
    ,CASE WHEN AMFPV_AGG.AGG_CURRENT_AMOUNT < 100
     THEN ROUND(ABS(AMFPV.AGG_CURRENT_AMOUNT)
           /ABS(AMFPV_AGG.ABS_AGG_CURRENT_AMOUNT) * 100, 2)
     ELSE ROUND(AMFPV.AGG_CURRENT_AMOUNT
           /AMFPV_AGG.AGG_CURRENT_AMOUNT * 100, 2) END                                AS "AGG_ALLOC_%_CURRENT_VALUE"
    ,CASE WHEN AMFPV_AGG.AGG_PREVIOUS_AMOUNT < 100 THEN 'ABSOLUTE'
                                                 ELSE 'NET' END                       AS "AGG_ALLOC_%_PREVIOUS_VALUE_TYP_CD"
    ,CASE WHEN AMFPV_AGG.AGG_PREVIOUS_AMOUNT < 100
     THEN ROUND(ABS(AMFPV.AGG_PREVIOUS_AMOUNT)
           /ABS(AMFPV_AGG.ABS_AGG_PREVIOUS_AMOUNT) * 100, 2)
     ELSE ROUND(AMFPV.AGG_PREVIOUS_AMOUNT
           /AMFPV_AGG.AGG_PREVIOUS_AMOUNT * 100, 2) END                               AS "AGG_ALLOC_%_PREVIOUS_VALUE"
    ,CASE WHEN AMFPV_AGG."AGG_P/L" < 100 THEN 'ABSOLUTE'
                                       ELSE 'NET' END                                 AS "AGG_ALLOC_%_P/L_TYP_CD"
    ,CASE WHEN AMFPV_AGG."AGG_P/L" < 100
     THEN ROUND(ABS(AMFPV."AGG_P/L")
           /ABS(AMFPV_AGG."ABS_AGG_P/L") * 100, 2)
     ELSE ROUND(AMFPV."AGG_P/L"
           /AMFPV_AGG."AGG_P/L" * 100, 2) END                                         AS "AGG_ALLOC_%_P/L"
    ,CASE WHEN AMFPV_AGG."AGG_DAY_P/L" < 100 THEN 'ABSOLUTE'
                                           ELSE 'NET' END                             AS "AGG_ALLOC_%_DAY_P/L_TYP_CD"
    ,CASE WHEN AMFPV_AGG."AGG_DAY_P/L" < 100
     THEN ROUND(ABS(AMFPV."AGG_DAY_P/L")
           /ABS(AMFPV_AGG."ABS_AGG_DAY_P/L") * 100, 2)
     ELSE ROUND(AMFPV."AGG_DAY_P/L"
           /AMFPV_AGG."AGG_DAY_P/L" * 100, 2) END                                     AS "AGG_ALLOC_%_DAY_P/L"
FROM
    AGG_MUTUAL_FUND_PORTFOLIO_VIEW AMFPV
INNER JOIN
(
SELECT
    ROUND(SUM(SUB_AGG.AGG_INVESTED_AMOUNT),4)                                         AS AGG_INVESTED_AMOUNT
    ,ROUND(SUM(SUB_AGG.AGG_AMC_AMOUNT),4)                                             AS AGG_INVESTED_AMOUNT_EXCLUDING_FEES
    ,ROUND(SUM(SUB_AGG.AGG_FUND_UNITS),4)                                             AS AGG_QUANTITY
    ,ROUND(SUM(SUB_AGG.AGG_CURRENT_AMOUNT),4)                                         AS AGG_CURRENT_AMOUNT
    ,ROUND(SUM(SUB_AGG.AGG_PREVIOUS_AMOUNT),4)                                        AS AGG_PREVIOUS_AMOUNT
    ,ROUND(SUM(SUB_AGG."AGG_P/L"),4)                                                  AS "AGG_P/L"
    ,ROUND(SUM(SUB_AGG."AGG_DAY_P/L"),4)                                              AS "AGG_DAY_P/L"
    ,ROUND(SUM(ABS(SUB_AGG.AGG_INVESTED_AMOUNT)),4)                                   AS ABS_AGG_INVESTED_AMOUNT
    ,ROUND(SUM(ABS(SUB_AGG.AGG_AMC_AMOUNT)),4)                                        AS ABS_AGG_INVESTED_AMOUNT_EXCLUDING_FEES
    ,ROUND(SUM(ABS(SUB_AGG.AGG_FUND_UNITS)),4)                                        AS ABS_AGG_QUANTITY
    ,ROUND(SUM(ABS(SUB_AGG.AGG_CURRENT_AMOUNT)),4)                                    AS ABS_AGG_CURRENT_AMOUNT
    ,ROUND(SUM(ABS(SUB_AGG.AGG_PREVIOUS_AMOUNT)),4)                                   AS ABS_AGG_PREVIOUS_AMOUNT
    ,ROUND(SUM(ABS(SUB_AGG."AGG_P/L")),4)                                             AS "ABS_AGG_P/L"
    ,ROUND(SUM(ABS(SUB_AGG."AGG_DAY_P/L")),4)                                         AS "ABS_AGG_DAY_P/L"
FROM
    AGG_MUTUAL_FUND_PORTFOLIO_VIEW SUB_AGG
) AMFPV_AGG
ON 1 = 1

UNION ALL

SELECT
    ASSUPV.ALLOCATION_CATEGORY                                                        AS PORTFOLIO_TYPE
    ,ASSUPV.STOCK_NAME                                                                AS PORTFOLIO_NAME
    ,ASSUPV.STOCK_NAME                                                                AS PORTFOLIO_HOUSE
    ,'Swing Equity'                                                                   AS PORTFOLIO_SUB_TYPE
    ,'Individual Stocks'                                                              AS PORTFOLIO_CATEGORY
    ,ASSUPV.AGG_TOTAL_INVESTED_AMOUNT                                                 AS INVESTED_AMOUNT
    ,ASSUPV.AGG_INVESTED_AMOUNT                                                       AS INVESTED_AMOUNT_EXCLUDING_FEES
    ,ASSUPV.AGG_STOCK_QUANTITY                                                        AS QUANTITY
    ,ASSUPV.AGG_CURRENT_VALUE                                                         AS CURRENT_VALUE
    ,ASSUPV.AGG_PREVIOUS_VALUE                                                        AS PREVIOUS_VALUE
    ,ASSUPV."AGG_P/L"                                                                 AS "P/L"
    ,ASSUPV."AGG_%_P/L"                                                               AS "%_P_L"
    ,ASSUPV."AGG_DAY_P/L"                                                             AS "DAY_P/L"
    ,ASSUPV."AGG_%_DAY_P/L"                                                           AS "%_DAY_P/L"
    ,ASSUPV.AVG_TRADE_PRICE                                                           AS AVERAGE_PRICE
    ,ASSUPV_AGG.AGG_INVESTED_AMOUNT                                                   AS AGG_INVESTED_AMOUNT
    ,ASSUPV_AGG.AGG_INVESTED_AMOUNT_EXCLUDING_FEES                                    AS AGG_INVESTED_AMOUNT_EXCLUDING_FEES
    ,ASSUPV_AGG.AGG_QUANTITY                                                          AS AGG_QUANTITY
    ,ASSUPV_AGG.AGG_CURRENT_AMOUNT                                                    AS AGG_CURRENT_AMOUNT
    ,ASSUPV_AGG.AGG_PREVIOUS_AMOUNT                                                   AS AGG_PREVIOUS_AMOUNT
    ,ASSUPV_AGG."AGG_P/L"                                                             AS "AGG_P/L"
    ,ASSUPV_AGG."AGG_DAY_P/L"                                                         AS "AGG_DAY_P/L"
    ,CASE WHEN ASSUPV_AGG.AGG_INVESTED_AMOUNT < 100 THEN 'ABSOLUTE'
                                                  ELSE 'NET' END                      AS "AGG_ALLOC_%_PORTFOLIO_INVESTED_AMOUNT_TYP_CD"
    ,CASE WHEN ASSUPV_AGG.AGG_INVESTED_AMOUNT < 100 
     THEN ROUND(ABS(ASSUPV.AGG_TOTAL_INVESTED_AMOUNT)
           /ABS(ASSUPV_AGG.ABS_AGG_INVESTED_AMOUNT) * 100, 2)
     ELSE ROUND(ASSUPV.AGG_TOTAL_INVESTED_AMOUNT
           /ASSUPV_AGG.AGG_INVESTED_AMOUNT * 100, 2) END                              AS "AGG_ALLOC_%_PORTFOLIO_INVESTED_AMOUNT"
    ,CASE WHEN ASSUPV_AGG.AGG_INVESTED_AMOUNT_EXCLUDING_FEES < 100 THEN 'ABSOLUTE'
                                                                 ELSE 'NET' END       AS "AGG_ALLOC_%_PORTFOLIO_INVESTED_AMOUNT_EXCLUDING_FEES_TYP_CD"
    ,CASE WHEN ASSUPV_AGG.AGG_INVESTED_AMOUNT_EXCLUDING_FEES < 100
     THEN ROUND(ABS(ASSUPV.AGG_INVESTED_AMOUNT)
           /ABS(ASSUPV_AGG.ABS_AGG_INVESTED_AMOUNT_EXCLUDING_FEES) * 100, 2)
     ELSE ROUND(ASSUPV.AGG_INVESTED_AMOUNT
           /ASSUPV_AGG.AGG_INVESTED_AMOUNT_EXCLUDING_FEES * 100, 2) END               AS "AGG_ALLOC_%_PORTFOLIO_INVESTED_AMOUNT_EXCLUDING_FEES"
    ,CASE WHEN ASSUPV_AGG.AGG_QUANTITY < 100 THEN 'ABSOLUTE'
                                           ELSE 'NET' END                             AS "AGG_ALLOC_%_QUANTITY_TYP_CD"
    ,CASE WHEN ASSUPV_AGG.AGG_QUANTITY < 100
     THEN ROUND(ABS(ASSUPV.AGG_STOCK_QUANTITY)
           /ABS(ASSUPV_AGG.ABS_AGG_QUANTITY) * 100, 2)
     ELSE ROUND(ASSUPV.AGG_STOCK_QUANTITY
           /ASSUPV_AGG.AGG_QUANTITY * 100, 2) END                                     AS "AGG_ALLOC_%_QUANTITY"
    ,CASE WHEN ASSUPV_AGG.AGG_CURRENT_AMOUNT < 100 THEN 'ABSOLUTE'
                                                 ELSE 'NET' END                       AS "AGG_ALLOC_%_CURRENT_VALUE_TYP_CD"
    ,CASE WHEN ASSUPV_AGG.AGG_CURRENT_AMOUNT < 100
     THEN ROUND(ABS(ASSUPV.AGG_CURRENT_VALUE)
           /ABS(ASSUPV_AGG.ABS_AGG_CURRENT_AMOUNT) * 100, 2)
     ELSE ROUND(ASSUPV.AGG_CURRENT_VALUE
           /ASSUPV_AGG.AGG_CURRENT_AMOUNT * 100, 2) END                               AS "AGG_ALLOC_%_CURRENT_VALUE"
    ,CASE WHEN ASSUPV_AGG.AGG_PREVIOUS_AMOUNT < 100 THEN 'ABSOLUTE'
                                                  ELSE 'NET' END                      AS "AGG_ALLOC_%_PREVIOUS_VALUE_TYP_CD"
    ,CASE WHEN ASSUPV_AGG.AGG_PREVIOUS_AMOUNT < 100
     THEN ROUND(ABS(ASSUPV.AGG_PREVIOUS_VALUE)
           /ABS(ASSUPV_AGG.ABS_AGG_PREVIOUS_AMOUNT) * 100, 2)
     ELSE ROUND(ASSUPV.AGG_PREVIOUS_VALUE
           /ASSUPV_AGG.AGG_PREVIOUS_AMOUNT * 100, 2) END                              AS "AGG_ALLOC_%_PREVIOUS_VALUE"
    ,CASE WHEN ASSUPV_AGG."AGG_P/L" < 100 THEN 'ABSOLUTE'
                                        ELSE 'NET' END                                AS "AGG_ALLOC_%_P/L_TYP_CD"
    ,CASE WHEN ASSUPV_AGG."AGG_P/L" < 100
     THEN ROUND(ABS(ASSUPV."AGG_P/L")
           /ABS(ASSUPV_AGG."ABS_AGG_P/L") * 100, 2)
     ELSE ROUND(ASSUPV."AGG_P/L"
           /ASSUPV_AGG."AGG_P/L" * 100, 2) END                                        AS "AGG_ALLOC_%_P/L"
    ,CASE WHEN ASSUPV_AGG."AGG_DAY_P/L" < 100 THEN 'ABSOLUTE'
                                            ELSE 'NET' END                            AS "AGG_ALLOC_%_DAY_P/L_TYP_CD"
    ,CASE WHEN ASSUPV_AGG."AGG_DAY_P/L" < 100
     THEN ROUND(ABS(ASSUPV."AGG_DAY_P/L")
           /ABS(ASSUPV_AGG."ABS_AGG_DAY_P/L") * 100, 2)
     ELSE ROUND(ASSUPV."AGG_DAY_P/L"
           /ASSUPV_AGG."AGG_DAY_P/L" * 100, 2) END                                    AS "AGG_ALLOC_%_DAY_P/L"
FROM
    AGG_STOCK_SWING_UNREALISED_PORTFOLIO_VIEW ASSUPV
INNER JOIN
(
SELECT
    ROUND(SUM(SUB_AGG.AGG_INVESTED_AMOUNT),4)                                         AS AGG_INVESTED_AMOUNT
    ,ROUND(SUM(SUB_AGG.AGG_TOTAL_INVESTED_AMOUNT),4)                                  AS AGG_INVESTED_AMOUNT_EXCLUDING_FEES
    ,ROUND(SUM(SUB_AGG.AGG_STOCK_QUANTITY),4)                                         AS AGG_QUANTITY
    ,ROUND(SUM(SUB_AGG.AGG_CURRENT_VALUE),4)                                          AS AGG_CURRENT_AMOUNT
    ,ROUND(SUM(SUB_AGG.AGG_PREVIOUS_VALUE),4)                                         AS AGG_PREVIOUS_AMOUNT
    ,ROUND(SUM(SUB_AGG."AGG_P/L"),4)                                                  AS "AGG_P/L"
    ,ROUND(SUM(SUB_AGG."AGG_DAY_P/L"),4)                                              AS "AGG_DAY_P/L"
    ,ROUND(SUM(ABS(SUB_AGG.AGG_INVESTED_AMOUNT)),4)                                   AS ABS_AGG_INVESTED_AMOUNT
    ,ROUND(SUM(ABS(SUB_AGG.AGG_TOTAL_INVESTED_AMOUNT)),4)                             AS ABS_AGG_INVESTED_AMOUNT_EXCLUDING_FEES
    ,ROUND(SUM(ABS(SUB_AGG.AGG_STOCK_QUANTITY)),4)                                    AS ABS_AGG_QUANTITY
    ,ROUND(SUM(ABS(SUB_AGG.AGG_CURRENT_VALUE)),4)                                     AS ABS_AGG_CURRENT_AMOUNT
    ,ROUND(SUM(ABS(SUB_AGG.AGG_PREVIOUS_VALUE)),4)                                    AS ABS_AGG_PREVIOUS_AMOUNT
    ,ROUND(SUM(ABS(SUB_AGG."AGG_P/L")),4)                                             AS "ABS_AGG_P/L"
    ,ROUND(SUM(ABS(SUB_AGG."AGG_DAY_P/L")),4)                                         AS "ABS_AGG_DAY_P/L"
FROM
    AGG_STOCK_SWING_UNREALISED_PORTFOLIO_VIEW SUB_AGG
) ASSUPV_AGG
ON 1 = 1
;
'''