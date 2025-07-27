AGG_CONSOLIDATED_ALLOCATION_VIEW = '''
CREATE VIEW AGG_CONSOLIDATED_ALLOCATION_VIEW AS
SELECT
    ACAV.PORTFOLIO_TYPE                                                               AS PORTFOLIO_TYPE
    ,ACAV.PORTFOLIO_NAME                                                              AS PORTFOLIO_NAME
    ,ACAV.PORTFOLIO_HOUSE                                                             AS PORTFOLIO_HOUSE
    ,ACAV.PORTFOLIO_SUB_TYPE                                                          AS PORTFOLIO_SUB_TYPE
    ,ACAV.PORTFOLIO_CATEGORY                                                          AS PORTFOLIO_CATEGORY
    ,ACAV.INVESTED_AMOUNT                                                             AS INVESTED_AMOUNT
    ,ACAV.INVESTED_AMOUNT_EXCLUDING_FEES                                              AS INVESTED_AMOUNT_EXCLUDING_FEES
    ,ACAV.QUANTITY                                                                    AS QUANTITY
    ,ACAV.CURRENT_VALUE                                                               AS CURRENT_VALUE
    ,ACAV.PREVIOUS_VALUE                                                              AS PREVIOUS_VALUE
    ,ACAV."P/L"                                                                       AS "P/L"
    ,ACAV."%_P_L"                                                                     AS "%_P_L"
    ,ACAV."DAY_P/L"                                                                   AS "DAY_P/L"
    ,ACAV."%_DAY_P/L"                                                                 AS "%_DAY_P/L"
    ,ACAV.AVERAGE_PRICE                                                               AS AVERAGE_PRICE
    ,ACAV_AGG.FIN_INVESTED_AMOUNT                                                     AS FIN_INVESTED_AMOUNT
    ,ACAV_AGG.FIN_INVESTED_AMOUNT_EXCLUDING_FEES                                      AS FIN_INVESTED_AMOUNT_EXCLUDING_FEES
    ,ACAV_AGG.FIN_QUANTITY                                                            AS FIN_QUANTITY
    ,ACAV_AGG.FIN_CURRENT_AMOUNT                                                      AS FIN_CURRENT_AMOUNT
    ,ACAV_AGG.FIN_PREVIOUS_AMOUNT                                                     AS FIN_PREVIOUS_AMOUNT
    ,ACAV_AGG."FIN_P/L"                                                               AS "FIN_P/L"
    ,ACAV_AGG."FIN_DAY_P/L"                                                           AS "FIN_DAY_P/L"
    ,CASE WHEN ACAV_AGG.FIN_INVESTED_AMOUNT < 100 THEN 'ABSOLUTE'
                                                  ELSE 'NET' END                      AS "FIN_ALLOC_%_PORTFOLIO_INVESTED_AMOUNT_TYP_CD"
    ,CASE WHEN ACAV_AGG.FIN_INVESTED_AMOUNT < 100 
     THEN ROUND(ABS(ACAV.INVESTED_AMOUNT)
           /ABS(ACAV_AGG.ABS_FIN_INVESTED_AMOUNT) * 100, 2)
     ELSE ROUND(ACAV.INVESTED_AMOUNT
           /ACAV_AGG.FIN_INVESTED_AMOUNT * 100, 2) END                                AS "FIN_ALLOC_%_PORTFOLIO_INVESTED_AMOUNT"
    ,CASE WHEN ACAV_AGG.FIN_INVESTED_AMOUNT_EXCLUDING_FEES < 100 THEN 'ABSOLUTE'
                                                                 ELSE 'NET' END       AS "FIN_ALLOC_%_PORTFOLIO_INVESTED_AMOUNT_EXCLUDING_FEES_TYP_CD"
    ,CASE WHEN ACAV_AGG.FIN_INVESTED_AMOUNT_EXCLUDING_FEES < 100
     THEN ROUND(ABS(ACAV.INVESTED_AMOUNT_EXCLUDING_FEES)
           /ABS(ACAV_AGG.ABS_FIN_INVESTED_AMOUNT_EXCLUDING_FEES) * 100, 2)
     ELSE ROUND(ACAV.INVESTED_AMOUNT_EXCLUDING_FEES
           /ACAV_AGG.FIN_INVESTED_AMOUNT_EXCLUDING_FEES * 100, 2) END                 AS "FIN_ALLOC_%_PORTFOLIO_INVESTED_AMOUNT_EXCLUDING_FEES"
    ,CASE WHEN ACAV_AGG.FIN_QUANTITY < 100 THEN 'ABSOLUTE'
                                           ELSE 'NET' END                             AS "FIN_ALLOC_%_QUANTITY_TYP_CD"
    ,CASE WHEN ACAV_AGG.FIN_QUANTITY < 100
     THEN ROUND(ABS(ACAV.QUANTITY)
           /ABS(ACAV_AGG.ABS_FIN_QUANTITY) * 100, 2)
     ELSE ROUND(ACAV.QUANTITY
           /ACAV_AGG.FIN_QUANTITY * 100, 2) END                                       AS "FIN_ALLOC_%_QUANTITY"
    ,CASE WHEN ACAV_AGG.FIN_CURRENT_AMOUNT < 100 THEN 'ABSOLUTE'
                                                 ELSE 'NET' END                       AS "FIN_ALLOC_%_CURRENT_VALUE_TYP_CD"
    ,CASE WHEN ACAV_AGG.FIN_CURRENT_AMOUNT < 100
     THEN ROUND(ABS(ACAV.CURRENT_VALUE)
           /ABS(ACAV_AGG.ABS_FIN_CURRENT_AMOUNT) * 100, 2)
     ELSE ROUND(ACAV.CURRENT_VALUE
           /ACAV_AGG.FIN_CURRENT_AMOUNT * 100, 2) END                                 AS "FIN_ALLOC_%_CURRENT_VALUE"
    ,CASE WHEN ACAV_AGG.FIN_PREVIOUS_AMOUNT < 100 THEN 'ABSOLUTE'
                                                  ELSE 'NET' END                      AS "FIN_ALLOC_%_PREVIOUS_VALUE_TYP_CD"
    ,CASE WHEN ACAV_AGG.FIN_PREVIOUS_AMOUNT < 100
     THEN ROUND(ABS(ACAV.PREVIOUS_VALUE)
           /ABS(ACAV_AGG.ABS_FIN_PREVIOUS_AMOUNT) * 100, 2)
     ELSE ROUND(ACAV.PREVIOUS_VALUE
           /ACAV_AGG.FIN_PREVIOUS_AMOUNT * 100, 2) END                                AS "FIN_ALLOC_%_PREVIOUS_VALUE"
    ,CASE WHEN ACAV_AGG."FIN_P/L" < 100 THEN 'ABSOLUTE'
                                        ELSE 'NET' END                                AS "FIN_ALLOC_%_P/L_TYP_CD"
    ,CASE WHEN ACAV_AGG."FIN_P/L" < 100
     THEN ROUND(ABS(ACAV."P/L")
           /ABS(ACAV_AGG."ABS_FIN_P/L") * 100, 2)
     ELSE ROUND(ACAV."P/L"
           /ACAV_AGG."FIN_P/L" * 100, 2) END                                          AS "FIN_ALLOC_%_P/L"
    ,CASE WHEN ACAV_AGG."FIN_DAY_P/L" < 100 THEN 'ABSOLUTE'
                                            ELSE 'NET' END                            AS "FIN_ALLOC_%_DAY_P/L_TYP_CD"
    ,CASE WHEN ACAV_AGG."FIN_DAY_P/L" < 100
     THEN ROUND(ABS(ACAV."DAY_P/L")
           /ABS(ACAV_AGG."ABS_FIN_DAY_P/L") * 100, 2)
     ELSE ROUND(ACAV."DAY_P/L"
           /ACAV_AGG."FIN_DAY_P/L" * 100, 2) END                                      AS "FIN_ALLOC_%_DAY_P/L"
    ,ACAV.PROCESSING_DATE                                                             AS PROCESSING_DATE
    ,ACAV.PREVIOUS_PROCESSING_DATE                                                    AS PREVIOUS_PROCESSING_DATE
    ,ACAV.NEXT_PROCESSING_DATE                                                        AS NEXT_PROCESSING_DATE
FROM
    CONSOLIDATED_ALLOCATION_VIEW ACAV
INNER JOIN
(
SELECT
    ROUND(SUM(SUB_AGG.INVESTED_AMOUNT),4)                                             AS FIN_INVESTED_AMOUNT
    ,ROUND(SUM(SUB_AGG.INVESTED_AMOUNT_EXCLUDING_FEES),4)                             AS FIN_INVESTED_AMOUNT_EXCLUDING_FEES
    ,ROUND(SUM(SUB_AGG.QUANTITY),4)                                                   AS FIN_QUANTITY
    ,ROUND(SUM(SUB_AGG.CURRENT_VALUE),4)                                              AS FIN_CURRENT_AMOUNT
    ,ROUND(SUM(SUB_AGG.PREVIOUS_VALUE),4)                                             AS FIN_PREVIOUS_AMOUNT
    ,ROUND(SUM(SUB_AGG."P/L"),4)                                                      AS "FIN_P/L"
    ,ROUND(SUM(SUB_AGG."DAY_P/L"),4)                                                  AS "FIN_DAY_P/L"
    ,ROUND(SUM(ABS(SUB_AGG.INVESTED_AMOUNT)),4)                                       AS ABS_FIN_INVESTED_AMOUNT
    ,ROUND(SUM(ABS(SUB_AGG.INVESTED_AMOUNT_EXCLUDING_FEES)),4)                        AS ABS_FIN_INVESTED_AMOUNT_EXCLUDING_FEES
    ,ROUND(SUM(ABS(SUB_AGG.QUANTITY)),4)                                              AS ABS_FIN_QUANTITY
    ,ROUND(SUM(ABS(SUB_AGG.CURRENT_VALUE)),4)                                         AS ABS_FIN_CURRENT_AMOUNT
    ,ROUND(SUM(ABS(SUB_AGG.PREVIOUS_VALUE)),4)                                        AS ABS_FIN_PREVIOUS_AMOUNT
    ,ROUND(SUM(ABS(SUB_AGG."P/L")),4)                                                 AS "ABS_FIN_P/L"
    ,ROUND(SUM(ABS(SUB_AGG."DAY_P/L")),4)                                             AS "ABS_FIN_DAY_P/L"
FROM
    CONSOLIDATED_ALLOCATION_VIEW SUB_AGG
) ACAV_AGG
ON 1 = 1
;
'''