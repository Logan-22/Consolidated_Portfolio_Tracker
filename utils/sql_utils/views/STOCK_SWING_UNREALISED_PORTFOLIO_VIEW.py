STOCK_SWING_UNREALISED_PORTFOLIO_VIEW = '''
CREATE VIEW STOCK_SWING_UNREALISED_PORTFOLIO_VIEW AS
SELECT
    SUB.STOCK_NAME                                                                        AS STOCK_NAME
    ,SUB.ALLOCATION_CATEGORY                                                              AS ALLOCATION_CATEGORY
    ,SUB.TRADE_DATE                                                                       AS TRADE_DATE
    ,SUB.STOCK_QUANTITY                                                                   AS STOCK_QUANTITY
    ,SUB.TRADE_PRICE                                                                      AS TRADE_PRICE
    ,SUB.CURRENT_PRICE                                                                    AS CURRENT_PRICE
    ,SUB.INVESTED_AMOUNT                                                                  AS INVESTED_AMOUNT
    ,SUB.TOTAL_FEES                                                                       AS TOTAL_FEES
    ,SUB.TOTAL_INVESTED_AMOUNT                                                            AS TOTAL_INVESTED_AMOUNT
    ,SUB.CURRENT_VALUE                                                                    AS CURRENT_VALUE
    ,ROUND(SUB.CURRENT_VALUE - SUB.INVESTED_AMOUNT,4)                                     AS "P/L"
    ,ROUND((SUB.CURRENT_VALUE - SUB.INVESTED_AMOUNT)/SUB.INVESTED_AMOUNT * 100,2)         AS "%_P/L"
    ,ROUND(SUB.CURRENT_VALUE - SUB.TOTAL_INVESTED_AMOUNT,4)                               AS "NET_P/L"
    ,ROUND((SUB.CURRENT_VALUE - SUB.TOTAL_INVESTED_AMOUNT)
    /SUB.TOTAL_INVESTED_AMOUNT * 100,2)                                                   AS "%_NET_P/L"
    ,SUB.PREVIOUS_PRICE                                                                   AS PREVIOUS_PRICE
    ,SUB.PREVIOUS_VALUE                                                                   AS PREVIOUS_VALUE
    ,ROUND(SUB.CURRENT_VALUE - SUB.PREVIOUS_VALUE,4)                                      AS "DAY_P/L"
    ,ROUND((SUB.CURRENT_VALUE - SUB.PREVIOUS_VALUE)/SUB.PREVIOUS_VALUE * 100,2)           AS "%_DAY_P/L"
    ,SUB.PROCESSING_DATE                                                                  AS PROCESSING_DATE
    ,SUB.PREVIOUS_PROCESSING_DATE                                                         AS PREVIOUS_PROCESSING_DATE
    ,SUB.NEXT_PROCESSING_DATE                                                             AS NEXT_PROCESSING_DATE
FROM
(
SELECT
    TRD.STOCK_NAME                                                                        AS STOCK_NAME
    ,META.ALLOCATION_CATEGORY                                                             AS ALLOCATION_CATEGORY
    ,TRD.TRADE_DATE                                                                       AS TRADE_DATE
    ,TRD.STOCK_QUANTITY                                                                   AS STOCK_QUANTITY
    ,TRD.NET_TRADE_PRICE_PER_UNIT                                                         AS TRADE_PRICE
    ,(SELECT DISTINCT PROC_DATE FROM PROCESSING_DATE
    WHERE PROC_TYP_CD = 'STOCK_PROC')                                                     AS PROCESSING_DATE
    ,CURRENT_PRICE.PRICE                                                                  AS CURRENT_PRICE
    ,ROUND(TRD.STOCK_QUANTITY * TRD.NET_TRADE_PRICE_PER_UNIT,4)                           AS INVESTED_AMOUNT
    ,ROUND(FEE.BROKERAGE + FEE.EXCHANGE_TRANSACTION_CHARGES + FEE.IGST + 
     FEE.SECURITIES_TRANSACTION_TAX + FEE.SEBI_TURN_OVER_FEES +
     FEE.AUTO_SQUARE_OFF_CHARGES + FEE.DEPOSITORY_CHARGES,4)                              AS TOTAL_FEES
    ,ROUND((TRD.STOCK_QUANTITY * TRD.NET_TRADE_PRICE_PER_UNIT) + 
     FEE.BROKERAGE + FEE.EXCHANGE_TRANSACTION_CHARGES + FEE.IGST + 
     FEE.SECURITIES_TRANSACTION_TAX + FEE.SEBI_TURN_OVER_FEES +
     FEE.AUTO_SQUARE_OFF_CHARGES + FEE.DEPOSITORY_CHARGES,4)                              AS TOTAL_INVESTED_AMOUNT
    ,ROUND(TRD.STOCK_QUANTITY * CURRENT_PRICE.PRICE,4)                                    AS CURRENT_VALUE
    ,(SELECT DISTINCT PREV_PROC_DATE FROM PROCESSING_DATE
    WHERE PROC_TYP_CD = 'STOCK_PROC')                                                     AS PREVIOUS_PROCESSING_DATE
    ,PREV_PRICE.PRICE                                                                     AS PREVIOUS_PRICE
    ,ROUND(TRD.STOCK_QUANTITY * PREV_PRICE.PRICE,4)                                       AS PREVIOUS_VALUE
    ,CASE WHEN FSSRPV_OPEN.TRADE_CLOSE_DATE IS NULL -- TRADE IS NOT CLOSED YET
          THEN TRD.END_DATE
          ELSE FSSRPV_OPEN.TRADE_CLOSE_DATE END                                           AS TRADE_CLOSE_DATE
    ,FSSRPV_CLOSE.CLOSING_FEE_ID                                                          AS CLOSED_FEE_ID
    ,(SELECT DISTINCT NEXT_PROC_DATE FROM PROCESSING_DATE
    WHERE PROC_TYP_CD = 'STOCK_PROC')                                                     AS NEXT_PROCESSING_DATE
FROM
    TRADES TRD
LEFT OUTER JOIN
    FEE_COMPONENT FEE
ON
    TRD.FEE_ID                  = FEE.FEE_ID
    AND FEE.RECORD_DELETED_FLAG = 0
LEFT OUTER JOIN
    METADATA_STORE META
ON
    TRD.STOCK_NAME = META.EXCHANGE_SYMBOL
-- CURRENT NAV DATA
LEFT OUTER JOIN
    PRICE_TABLE CURRENT_PRICE
ON
    CURRENT_PRICE.START_DATE              = (SELECT DISTINCT PROC_DATE FROM PROCESSING_DATE WHERE PROC_TYP_CD = 'STOCK_PROC')
    AND CURRENT_PRICE.RECORD_DELETED_FLAG = 0
    AND CURRENT_PRICE.PRICE_TYP_CD        = 'CLOSE_PRICE'
    AND CURRENT_PRICE.ALT_SYMBOL          = META.ALT_SYMBOL
----  PREVIOUS NAV DATA    
LEFT OUTER JOIN
    PRICE_TABLE PREV_PRICE
ON
    PREV_PRICE.START_DATE                 = (SELECT DISTINCT PREV_PROC_DATE FROM PROCESSING_DATE WHERE PROC_TYP_CD = 'STOCK_PROC')
    AND PREV_PRICE.RECORD_DELETED_FLAG    = 0
    AND PREV_PRICE.PRICE_TYP_CD           = 'CLOSE_PRICE'
    AND PREV_PRICE.ALT_SYMBOL             = META.ALT_SYMBOL
LEFT OUTER JOIN
    FIN_STOCK_SWING_REALISED_PORTFOLIO_VIEW FSSRPV_OPEN -- TO DETERMINE WHEN THE TRADE WAS CLOSED
ON
    FSSRPV_OPEN.OPENING_FEE_ID          = TRD.FEE_ID
    AND FSSRPV_OPEN.TRADES_CLOSE_STATUS = 'TRADES_COMPLETELY_CLOSED'
LEFT OUTER JOIN
    FIN_STOCK_SWING_REALISED_PORTFOLIO_VIEW FSSRPV_CLOSE
ON
    FSSRPV_CLOSE.CLOSING_FEE_ID          = TRD.FEE_ID
    AND FSSRPV_CLOSE.TRADES_CLOSE_STATUS = 'TRADES_COMPLETELY_CLOSED'
WHERE
    (SELECT DISTINCT PROC_DATE FROM PROCESSING_DATE WHERE PROC_TYP_CD = 'STOCK_PROC') BETWEEN TRD.START_DATE AND (CASE WHEN FSSRPV_OPEN.TRADE_CLOSE_DATE IS NULL -- TRADE IS NOT CLOSED YET
                                                                                                                       THEN TRD.END_DATE
                                                                                                                       ELSE FSSRPV_OPEN.TRADE_CLOSE_DATE END)
    AND TRD.RECORD_DELETED_FLAG = 0
    AND TRD.TRADE_EXIT_DATE IS NULL -- ONLY OPEN TRADES
    AND CLOSED_FEE_ID IS NULL -- IGNORE CLOSING TRADES
)
SUB
;
'''