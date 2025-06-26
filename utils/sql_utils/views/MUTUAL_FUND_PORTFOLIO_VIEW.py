MUTUAL_FUND_PORTFOLIO_VIEW = '''
CREATE VIEW MUTUAL_FUND_PORTFOLIO_VIEW AS
SELECT
SUB.FUND_NAME
,SUB.FUND_AMC
,SUB.FUND_TYPE
,SUB.FUND_CATEGORY
,SUB.FUND_PURCHASE_DATE
,SUB.NAV_DURING_PURCHASE
,SUB.PROCESSING_DATE
,SUB.HOLDING_DAYS
,SUB.CURRENT_NAV
,SUB.INVESTED_AMOUNT
,SUB.AMC_AMOUNT
,SUB.STAMP_FEES_AMOUNT
,SUB.FUND_UNITS
,ROUND(SUB.FUND_UNITS * SUB.CURRENT_NAV, 4)                                                                  AS CURRENT_AMOUNT
,ROUND(ROUND(SUB.FUND_UNITS * SUB.CURRENT_NAV, 4) - SUB.AMC_AMOUNT, 4)                                       AS "P/L"
,ROUND((ROUND(ROUND(SUB.FUND_UNITS * SUB.CURRENT_NAV, 4) - SUB.AMC_AMOUNT, 4)
/ SUB.AMC_AMOUNT )* 100 , 2)                                                                                 AS "%P/L"
,SUB.PREVIOUS_PROCESSING_DATE                                                                                AS PREVIOUS_PROCESSING_DATE
,SUB.PREVIOUS_NAV                                                                                            AS PREVIOUS_NAV
,ROUND(SUB.FUND_UNITS * SUB.PREVIOUS_NAV, 4)                                                                 AS PREVIOUS_AMOUNT
,ROUND(ROUND(SUB.FUND_UNITS * SUB.CURRENT_NAV, 4) - 
ROUND(SUB.FUND_UNITS * SUB.PREVIOUS_NAV, 4),4)                                                               AS "DAY_P/L"
,ROUND((ROUND(SUB.FUND_UNITS * SUB.CURRENT_NAV, 4) - 
ROUND(SUB.FUND_UNITS * SUB.PREVIOUS_NAV, 4)) / ROUND(SUB.FUND_UNITS * SUB.PREVIOUS_NAV, 4),4) * 100          AS "%DAY_P/L" 
FROM
(SELECT
    PO.NAME                                                                               AS FUND_NAME
    ,META.AMC                                                                             AS FUND_AMC
    ,META.MF_TYPE                                                                         AS FUND_TYPE
    ,META.FUND_CATEGORY                                                                   AS FUND_CATEGORY
    ,PO.PURCHASED_ON                                                                      AS FUND_PURCHASE_DATE
    ,PO.NAV_DURING_PURCHASE                                                               AS NAV_DURING_PURCHASE
    ,(SELECT DISTINCT PROC_DATE FROM PROCESSING_DATE
    WHERE PROC_TYP_CD = 'MF_PROC')                                                        AS PROCESSING_DATE
    ,(SELECT JULIANDAY(DISTINCT PROC_DATE)
     FROM PROCESSING_DATE WHERE PROC_TYP_CD = 'MF_PROC') 
      - JULIANDAY(PO.PURCHASED_ON)                                                        AS HOLDING_DAYS
    ,CURRENT_PRICE.PRICE                                                                  AS CURRENT_NAV
    ,PO.INVESTED_AMOUNT                                                                   AS INVESTED_AMOUNT
    ,PO.AMC_AMOUNT                                                                        AS AMC_AMOUNT
    ,PO.STAMP_FEES_AMOUNT                                                                 AS STAMP_FEES_AMOUNT
    ,PO.UNITS                                                                             AS FUND_UNITS
    ,(SELECT DISTINCT PREV_PROC_DATE FROM PROCESSING_DATE
    WHERE PROC_TYP_CD = 'MF_PROC')                                                        AS PREVIOUS_PROCESSING_DATE
    ,PREV_PRICE.PRICE                                                                     AS PREVIOUS_NAV
FROM
    MF_ORDER PO
LEFT OUTER JOIN
    METADATA_STORE META
ON
    PO.NAME = META.EXCHANGE_SYMBOL
-- CURRENT NAV DATA
LEFT OUTER JOIN
    PRICE_TABLE CURRENT_PRICE
ON
    CURRENT_PRICE.START_DATE              = (SELECT DISTINCT PROC_DATE FROM PROCESSING_DATE WHERE PROC_TYP_CD = 'MF_PROC')
    AND CURRENT_PRICE.RECORD_DELETED_FLAG = 0
    AND CURRENT_PRICE.PRICE_TYP_CD        = 'CLOSE_PRICE'
    AND CURRENT_PRICE.ALT_SYMBOL          = META.ALT_SYMBOL
----  PREVIOUS NAV DATA    
LEFT OUTER JOIN
    PRICE_TABLE PREV_PRICE
ON
    PREV_PRICE.START_DATE                 = (SELECT DISTINCT PREV_PROC_DATE FROM PROCESSING_DATE WHERE PROC_TYP_CD = 'MF_PROC')
    AND PREV_PRICE.RECORD_DELETED_FLAG    = 0
    AND PREV_PRICE.PRICE_TYP_CD           = 'CLOSE_PRICE'
    AND PREV_PRICE.ALT_SYMBOL             = META.ALT_SYMBOL
WHERE
    (SELECT DISTINCT PROC_DATE FROM PROCESSING_DATE WHERE PROC_TYP_CD = 'MF_PROC') BETWEEN PO.START_DATE AND PO.END_DATE
    AND PO.RECORD_DELETED_FLAG = 0
) SUB
;
'''