FIN_STOCK_REALISED_PORTFOLIO_VIEW = '''
CREATE VIEW FIN_STOCK_REALISED_PORTFOLIO_VIEW AS
SELECT
    SUB.TRADE_DATE                                             AS TRADE_DATE
    ,SUB.TRADE_TYPE                                            AS TRADE_TYPE
    ,SUB.FEE_ID                                                AS FEE_ID
    ,SUB.AGG_PERCEIVED_DEPLOYED_CAPITAL                        AS AGG_PERCEIVED_DEPLOYED_CAPITAL
    ,SUB.AGG_ACTUAL_DEPLOYED_CAPITAL                           AS AGG_ACTUAL_DEPLOYED_CAPITAL
    ,SUB."AGG_P/L"                                             AS "AGG_P/L"
    ,SUB."%_P/L_WITHOUT_LEVERAGE"                              AS "%_P/L_WITHOUT_LEVERAGE"
    ,SUB."%_P/L_WITH_LEVERAGE"                                 AS "%_P/L_WITH_LEVERAGE"
    ,SUB.NET_OBLIGATION                                        AS NET_OBLIGATION
    ,SUB."AGG_P/L_NET_OBLIGATION_MATCH_STATUS"                 AS "AGG_P/L_NET_OBLIGATION_MATCH_STATUS"
    ,SUB.BROKERAGE                                             AS BROKERAGE
    ,SUB.EXCHANGE_TRANSACTION_CHARGES                          AS EXCHANGE_TRANSACTION_CHARGES
    ,SUB.IGST                                                  AS IGST
    ,SUB.SECURITIES_TRANSACTION_TAX                            AS SECURITIES_TRANSACTION_TAX
    ,SUB.SEBI_TURN_OVER_FEES                                   AS SEBI_TURN_OVER_FEES
    ,SUB.TOTAL_FEES                                            AS TOTAL_FEES
    ,ROUND(SUB."NET_P/L",4)                                    AS "NET_P/L"
    ,ROUND(SUB."NET_P/L"/SUB.AGG_PERCEIVED_DEPLOYED_CAPITAL
     * 100,2)                                                  AS "NET_%_P/L_WITHOUT_LEVERAGE"
    ,ROUND(SUB."NET_P/L"/SUB.AGG_ACTUAL_DEPLOYED_CAPITAL
     * 100,2)                                                  AS "NET_%_P/L_WITH_LEVERAGE"
    ,SUB.TOTAL_CHARGES                                         AS TOTAL_CHARGES
    ,ROUND(SUB."NET_P/L_MINUS_CHARGES",4)                      AS "NET_P/L_MINUS_CHARGES"
    ,ROUND(SUB."NET_P/L_MINUS_CHARGES"/SUB.AGG_PERCEIVED_DEPLOYED_CAPITAL
     * 100,2)                                                  AS "NET_%_P/L_WITHOUT_LEVERAGE_INCL_CHARGES"
    ,ROUND(SUB."NET_P/L_MINUS_CHARGES"/SUB.AGG_ACTUAL_DEPLOYED_CAPITAL
     * 100,2)                                                  AS "NET_%_P/L_WITH_LEVERAGE_INCL_CHARGES"
FROM
(
SELECT
    ASRPV.TRADE_DATE                                           AS TRADE_DATE
    ,ASRPV.TRADE_TYPE                                          AS TRADE_TYPE
    ,ASRPV.FEE_ID                                              AS FEE_ID
    ,SUM(ASRPV.AGG_PERCEIVED_DEPLOYED_CAPITAL)                 AS AGG_PERCEIVED_DEPLOYED_CAPITAL
    ,SUM(ASRPV.AGG_ACTUAL_DEPLOYED_CAPITAL)                    AS AGG_ACTUAL_DEPLOYED_CAPITAL
    ,SUM(ASRPV."AGG_P/L")                                      AS "AGG_P/L"
    ,ROUND(SUM(ASRPV."AGG_P/L")/SUM(ASRPV.AGG_PERCEIVED_DEPLOYED_CAPITAL)
     * 100,2)                                                  AS "%_P/L_WITHOUT_LEVERAGE"
    ,ROUND(SUM(ASRPV."AGG_P/L")/SUM(ASRPV.AGG_ACTUAL_DEPLOYED_CAPITAL)
     * 100,2)                                                  AS "%_P/L_WITH_LEVERAGE"
    ,FEE.NET_OBLIGATION                                        AS NET_OBLIGATION
    ,CASE WHEN ROUND(SUM(ASRPV."AGG_P/L"),2) = ROUND(FEE.NET_OBLIGATION,2)
          THEN 'Matched'
     ELSE 'Unmatched' END                                      AS "AGG_P/L_NET_OBLIGATION_MATCH_STATUS"
    ,FEE.BROKERAGE                                             AS BROKERAGE
    ,FEE.EXCHANGE_TRANSACTION_CHARGES                          AS EXCHANGE_TRANSACTION_CHARGES
    ,FEE.IGST                                                  AS IGST
    ,FEE.SECURITIES_TRANSACTION_TAX                            AS SECURITIES_TRANSACTION_TAX
    ,FEE.SEBI_TURN_OVER_FEES                                   AS SEBI_TURN_OVER_FEES
    ,FEE.BROKERAGE + FEE.EXCHANGE_TRANSACTION_CHARGES +
     FEE.IGST + FEE.SECURITIES_TRANSACTION_TAX +
     FEE.SEBI_TURN_OVER_FEES                                   AS TOTAL_FEES
    ,FEE.NET_OBLIGATION - (
     FEE.BROKERAGE + FEE.EXCHANGE_TRANSACTION_CHARGES +
     FEE.IGST + FEE.SECURITIES_TRANSACTION_TAX +
     FEE.SEBI_TURN_OVER_FEES)                                  AS "NET_P/L"
    ,FEE.AUTO_SQUARE_OFF_CHARGES                               AS AUTO_SQUARE_OFF_CHARGES
    ,FEE.DEPOSITORY_CHARGES                                    AS DEPOSITORY_CHARGES
    ,FEE.AUTO_SQUARE_OFF_CHARGES + 
     FEE.DEPOSITORY_CHARGES                                    AS TOTAL_CHARGES
    ,FEE.NET_OBLIGATION - (
     FEE.BROKERAGE + FEE.EXCHANGE_TRANSACTION_CHARGES +
     FEE.IGST + FEE.SECURITIES_TRANSACTION_TAX +
     FEE.SEBI_TURN_OVER_FEES) - 
     (FEE.AUTO_SQUARE_OFF_CHARGES + 
     FEE.DEPOSITORY_CHARGES)                                   AS "NET_P/L_MINUS_CHARGES"
FROM
    AGG_STOCK_REALISED_PORTFOLIO_VIEW ASRPV
LEFT OUTER JOIN
    FEE_COMPONENT FEE
ON
    FEE.FEE_ID                  = ASRPV.FEE_ID
    AND FEE.RECORD_DELETED_FLAG = 0
GROUP BY 1,2,3
) SUB
ORDER BY 1,2,3
;
'''