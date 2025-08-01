SIMULATED_PORTFOLIO_VIEW = '''
CREATE VIEW SIMULATED_PORTFOLIO_VIEW AS
SELECT
    SUB.FUND_NAME                                                                         AS SIM_FUND_NAME
    ,SUB.BASE_TYPE                                                                        AS SIM_BASE_TYPE
    ,SUB.BASE_NAME                                                                        AS SIM_BASE_NAME
    ,SUB.ALLOCATION_CATEGORY                                                              AS SIM_ALLOCATION_CATEGORY
    ,SUB.FUND_PURCHASE_DATE                                                               AS SIM_PURCHASE_DATE
    ,SUB.NAV_DURING_PURCHASE                                                              AS SIM_NAV_DURING_PURCHASE
    ,SUB.HOLDING_DAYS                                                                     AS SIM_HOLDING_DAYS
    ,SUB.CURRENT_NAV                                                                      AS SIM_CURRENT_NAV
    ,SUB.INVESTED_AMOUNT                                                                  AS SIM_INVESTED_AMOUNT
    ,SUB.FUND_UNITS                                                                       AS SIM_FUND_UNITS
    ,ROUND(SUB.FUND_UNITS * SUB.CURRENT_NAV, 4)                                           AS SIM_CURRENT_AMOUNT
    ,ROUND((SUB.FUND_UNITS * SUB.CURRENT_NAV) - SUB.INVESTED_AMOUNT, 4)                   AS "SIM_P/L"
    ,ROUND(((SUB.FUND_UNITS * SUB.CURRENT_NAV) - SUB.INVESTED_AMOUNT) 
    / SUB.INVESTED_AMOUNT * 100 , 2)                                                      AS "SIM_%_P/L"
    ,SUB.PREVIOUS_NAV                                                                     AS SIM_PREVIOUS_NAV
    ,ROUND(SUB.FUND_UNITS * SUB.PREVIOUS_NAV, 4)                                          AS SIM_PREVIOUS_AMOUNT
    ,ROUND((SUB.FUND_UNITS * SUB.CURRENT_NAV) - 
    (SUB.FUND_UNITS * SUB.PREVIOUS_NAV),4)                                                AS "SIM_DAY_P/L"
    ,ROUND(((SUB.FUND_UNITS * SUB.CURRENT_NAV) - (SUB.FUND_UNITS * SUB.PREVIOUS_NAV)) 
    / (SUB.FUND_UNITS * SUB.PREVIOUS_NAV) * 100, 2)                                       AS "SIM_%_DAY_P/L"
    ,SUB.PROCESSING_DATE                                                                  AS PROCESSING_DATE
    ,SUB.PREVIOUS_PROCESSING_DATE                                                         AS PREVIOUS_PROCESSING_DATE
    ,SUB.NEXT_PROCESSING_DATE                                                             AS NEXT_PROCESSING_DATE
FROM
(
SELECT
    PT.PROC_DESCRIPTION                                                                   AS FUND_NAME
    ,PO.BASE_TYPE                                                                         AS BASE_TYPE
    ,PO.BASE_NAME                                                                         AS BASE_NAME
    ,META.ALLOCATION_CATEGORY                                                             AS ALLOCATION_CATEGORY
    ,PO.PURCHASED_ON                                                                      AS FUND_PURCHASE_DATE
    ,PURCHASE_PRICE.PRICE                                                                 AS NAV_DURING_PURCHASE
    ,(SELECT DISTINCT PROC_DATE FROM PROCESSING_DATE
    WHERE PROC_TYP_CD = 'SIM_MF_PROC')                                                    AS PROCESSING_DATE
    ,(SELECT JULIANDAY(DISTINCT PROC_DATE)
     FROM PROCESSING_DATE WHERE PROC_TYP_CD = 'SIM_MF_PROC') 
      - JULIANDAY(PO.PURCHASED_ON)                                                        AS HOLDING_DAYS
    ,CURRENT_PRICE.PRICE                                                                  AS CURRENT_NAV
    ,PO.INVESTED_AMOUNT                                                                   AS INVESTED_AMOUNT
    ,ROUND(PO.INVESTED_AMOUNT / PURCHASE_PRICE.PRICE, 4)                                  AS FUND_UNITS
    ,(SELECT DISTINCT PREV_PROC_DATE FROM PROCESSING_DATE
    WHERE PROC_TYP_CD = 'SIM_MF_PROC')                                                    AS PREVIOUS_PROCESSING_DATE
    ,PREV_PRICE.PRICE                                                                     AS PREVIOUS_NAV
    ,(SELECT DISTINCT NEXT_PROC_DATE FROM PROCESSING_DATE
    WHERE PROC_TYP_CD = 'SIM_MF_PROC')                                                    AS NEXT_PROCESSING_DATE
FROM
(
SELECT
    MF.PURCHASED_ON                                                                       AS PURCHASED_ON
    ,MF.INVESTED_AMOUNT                                                                   AS INVESTED_AMOUNT
    ,CAST('UNREALISED MUTUAL FUNDS' AS VARCHAR(20))                                       AS BASE_TYPE
    ,MF.NAME                                                                              AS BASE_NAME
FROM
    MF_ORDER MF
WHERE
    (SELECT DISTINCT PROC_DATE FROM PROCESSING_DATE WHERE PROC_TYP_CD = 'SIM_MF_PROC') BETWEEN MF.START_DATE AND MF.END_DATE
    AND MF.RECORD_DELETED_FLAG = 0

UNION ALL

SELECT
    TRD.TRADE_DATE                                                                        AS PURCHASED_ON
    ,ROUND((TRD.STOCK_QUANTITY * TRD.NET_TRADE_PRICE_PER_UNIT) + 
     FEE.BROKERAGE + FEE.EXCHANGE_TRANSACTION_CHARGES + FEE.IGST + 
     FEE.SECURITIES_TRANSACTION_TAX + FEE.SEBI_TURN_OVER_FEES +
     FEE.AUTO_SQUARE_OFF_CHARGES + FEE.DEPOSITORY_CHARGES,4)                              AS INVESTED_AMOUNT
    ,CAST('UNREALISED STOCKS' AS VARCHAR(20))                                             AS BASE_TYPE
    ,TRD.STOCK_NAME                                                                       AS BASE_NAME
FROM
    TRADES TRD
LEFT OUTER JOIN
    FEE_COMPONENT FEE
ON
    TRD.FEE_ID                  = FEE.FEE_ID
    AND FEE.RECORD_DELETED_FLAG = 0
LEFT OUTER JOIN
    FIN_REALISED_SWING_STOCK_RETURNS FSSRPV_OPEN -- TO DETERMINE WHEN THE TRADE WAS CLOSED
ON
    FSSRPV_OPEN.OPENING_FEE_ID          = TRD.FEE_ID
    AND FSSRPV_OPEN.TRADES_CLOSE_STATUS = 'TRADES_COMPLETELY_CLOSED'
    AND FSSRPV_OPEN.RECORD_DELETED_FLAG = 0
    AND (SELECT DISTINCT PROC_DATE FROM PROCESSING_DATE WHERE PROC_TYP_CD = 'STOCK_PROC') BETWEEN FSSRPV_OPEN.START_DATE AND FSSRPV_OPEN.END_DATE
LEFT OUTER JOIN
    FIN_REALISED_SWING_STOCK_RETURNS FSSRPV_CLOSE
ON
    FSSRPV_CLOSE.CLOSING_FEE_ID          = TRD.FEE_ID
    AND FSSRPV_CLOSE.TRADES_CLOSE_STATUS = 'TRADES_COMPLETELY_CLOSED'
    AND FSSRPV_CLOSE.RECORD_DELETED_FLAG = 0
    AND (SELECT DISTINCT PROC_DATE FROM PROCESSING_DATE WHERE PROC_TYP_CD = 'STOCK_PROC') BETWEEN FSSRPV_CLOSE.START_DATE AND FSSRPV_CLOSE.END_DATE
WHERE
    (SELECT DISTINCT PROC_DATE FROM PROCESSING_DATE WHERE PROC_TYP_CD = 'SIM_STOCK_PROC') BETWEEN TRD.START_DATE AND (CASE WHEN FSSRPV_OPEN.TRADE_CLOSE_DATE IS NULL -- TRADE IS NOT CLOSED YET
                                                                                                                       THEN TRD.END_DATE
                                                                                                                       ELSE FSSRPV_OPEN.TRADE_CLOSE_DATE END)
    AND TRD.RECORD_DELETED_FLAG = 0
    AND TRD.TRADE_EXIT_DATE IS NULL -- ONLY OPEN TRADES
    AND FSSRPV_CLOSE.CLOSING_FEE_ID IS NULL -- IGNORE CLOSING TRADES
) PO                 
INNER JOIN
    PROCESSING_TYPE PT
ON
    PROC_TYP_CD = 'SIMULATED_RETURNS'
LEFT OUTER JOIN
    METADATA_STORE META
ON
    PT.PROC_TYPE = META.ALT_SYMBOL
-- NAV DURING PURCHASE
LEFT OUTER JOIN
    PRICE_TABLE PURCHASE_PRICE
ON
    PURCHASE_PRICE.START_DATE              = PO.PURCHASED_ON
    AND PURCHASE_PRICE.RECORD_DELETED_FLAG = 0
    AND PURCHASE_PRICE.PRICE_TYP_CD        = 'CLOSE_PRICE'
    AND PURCHASE_PRICE.ALT_SYMBOL          = META.ALT_SYMBOL
-- CURRENT NAV DATA
LEFT OUTER JOIN
    PRICE_TABLE CURRENT_PRICE
ON
    CURRENT_PRICE.START_DATE              = (SELECT DISTINCT PROC_DATE FROM PROCESSING_DATE WHERE PROC_TYP_CD = 'SIM_MF_PROC')
    AND CURRENT_PRICE.RECORD_DELETED_FLAG = 0
    AND CURRENT_PRICE.PRICE_TYP_CD        = 'CLOSE_PRICE'
    AND CURRENT_PRICE.ALT_SYMBOL          = META.ALT_SYMBOL
----  PREVIOUS NAV DATA    
LEFT OUTER JOIN
    PRICE_TABLE PREV_PRICE
ON
    PREV_PRICE.START_DATE                 = (SELECT DISTINCT PREV_PROC_DATE FROM PROCESSING_DATE WHERE PROC_TYP_CD = 'SIM_MF_PROC')
    AND PREV_PRICE.RECORD_DELETED_FLAG    = 0
    AND PREV_PRICE.PRICE_TYP_CD           = 'CLOSE_PRICE'
    AND PREV_PRICE.ALT_SYMBOL             = META.ALT_SYMBOL
) SUB
;
'''