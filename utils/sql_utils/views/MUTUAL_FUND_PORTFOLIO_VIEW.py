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
    ,CASE 
    WHEN PO.NAME = 'Bandhan Nifty Alpha 50 Index Fund' 
                 THEN BNA.PRICE
     WHEN PO.NAME = 'HDFC NIFTY LargeMidcap 250 Index Fund' 
                 THEN HLM.PRICE
    WHEN PO.NAME = 'HDFC Small Cap Fund' 
                 THEN HSC.PRICE
    WHEN PO.NAME = 'ICICI Prudential Nifty50 Value 20 Index Fund' 
                 THEN INV.PRICE
    WHEN PO.NAME = 'Motilal Oswal Active Momentum Fund' 
                 THEN MAM.PRICE
    WHEN PO.NAME = 'Motilal Oswal Midcap Fund' 
                 THEN MMC.PRICE
    WHEN PO.NAME = 'Motilal Oswal Nifty Capital Market Index Fund' 
                 THEN MNC.PRICE
     WHEN PO.NAME = 'Nippon India Gold Savings Fund' 
                 THEN NGS.PRICE
    WHEN PO.NAME = 'Parag Parikh Flexi Cap Fund' 
                 THEN PPF.PRICE
    WHEN PO.NAME = 'UTI Nifty 50 Index Fund' 
                 THEN UNI.PRICE
     END                                                                                  AS CURRENT_NAV
    ,PO.INVESTED_AMOUNT                                                                   AS INVESTED_AMOUNT
    ,PO.AMC_AMOUNT                                                                        AS AMC_AMOUNT
    ,PO.STAMP_FEES_AMOUNT                                                                 AS STAMP_FEES_AMOUNT
    ,PO.UNITS                                                                             AS FUND_UNITS
    ,(SELECT DISTINCT PREV_PROC_DATE FROM PROCESSING_DATE
    WHERE PROC_TYP_CD = 'MF_PROC')                                                        AS PREVIOUS_PROCESSING_DATE
    ,CASE 
    WHEN PO.NAME = 'Bandhan Nifty Alpha 50 Index Fund' 
                 THEN BNA_PREV.PRICE
     WHEN PO.NAME = 'HDFC NIFTY LargeMidcap 250 Index Fund' 
                 THEN HLM_PREV.PRICE
    WHEN PO.NAME = 'HDFC Small Cap Fund' 
                 THEN HSC_PREV.PRICE
    WHEN PO.NAME = 'ICICI Prudential Nifty50 Value 20 Index Fund' 
                 THEN INV_PREV.PRICE
    WHEN PO.NAME = 'Motilal Oswal Active Momentum Fund' 
                 THEN MAM_PREV.PRICE
    WHEN PO.NAME = 'Motilal Oswal Midcap Fund' 
                 THEN MMC_PREV.PRICE
    WHEN PO.NAME = 'Motilal Oswal Nifty Capital Market Index Fund' 
                 THEN MNC_PREV.PRICE
     WHEN PO.NAME = 'Nippon India Gold Savings Fund' 
                 THEN NGS_PREV.PRICE
    WHEN PO.NAME = 'Parag Parikh Flexi Cap Fund' 
                 THEN PPF_PREV.PRICE
    WHEN PO.NAME = 'UTI Nifty 50 Index Fund' 
                 THEN UNI_PREV.PRICE
     END                                                                                                                    AS PREVIOUS_NAV
    
FROM
    MF_ORDER PO
LEFT OUTER JOIN
    METADATA META
ON
    PO.NAME = META.NAME
    
-- CURRENT NAV DATA

LEFT OUTER JOIN
    Bandhan_Nifty_Alpha_50_Index_Fund BNA
ON
    BNA.START_DATE = (SELECT DISTINCT PROC_DATE FROM PROCESSING_DATE WHERE PROC_TYP_CD = 'MF_PROC')
    AND BNA.RECORD_DELETED_FLAG = 0 
LEFT OUTER JOIN
    HDFC_NIFTY_LargeMidcap_250_Index_Fund HLM
ON
    HLM.START_DATE = (SELECT DISTINCT PROC_DATE FROM PROCESSING_DATE WHERE PROC_TYP_CD = 'MF_PROC')
    AND HLM.RECORD_DELETED_FLAG = 0
LEFT OUTER JOIN
    HDFC_Small_Cap_Fund HSC
ON
    HSC.START_DATE = (SELECT DISTINCT PROC_DATE FROM PROCESSING_DATE WHERE PROC_TYP_CD = 'MF_PROC')
    AND HSC.RECORD_DELETED_FLAG = 0
LEFT OUTER JOIN
    ICICI_Prudential_Nifty50_Value_20_Index_Fund INV
ON
    INV.START_DATE = (SELECT DISTINCT PROC_DATE FROM PROCESSING_DATE WHERE PROC_TYP_CD = 'MF_PROC')
    AND INV.RECORD_DELETED_FLAG = 0
LEFT OUTER JOIN
    Motilal_Oswal_Active_Momentum_Fund MAM
ON
    MAM.START_DATE = (SELECT DISTINCT PROC_DATE FROM PROCESSING_DATE WHERE PROC_TYP_CD = 'MF_PROC')
    AND MAM.RECORD_DELETED_FLAG = 0
LEFT OUTER JOIN
    Motilal_Oswal_Midcap_Fund MMC
ON
    MMC.START_DATE = (SELECT DISTINCT PROC_DATE FROM PROCESSING_DATE WHERE PROC_TYP_CD = 'MF_PROC')
    AND MMC.RECORD_DELETED_FLAG = 0
LEFT OUTER JOIN
    Motilal_Oswal_Nifty_Capital_Market_Index_Fund MNC
ON
    MNC.START_DATE = (SELECT DISTINCT PROC_DATE FROM PROCESSING_DATE WHERE PROC_TYP_CD = 'MF_PROC')
    AND MNC.RECORD_DELETED_FLAG = 0
LEFT OUTER JOIN
    Nippon_India_Gold_Savings_Fund NGS
ON
    NGS.START_DATE = (SELECT DISTINCT PROC_DATE FROM PROCESSING_DATE WHERE PROC_TYP_CD = 'MF_PROC')
    AND NGS.RECORD_DELETED_FLAG = 0
LEFT OUTER JOIN
    Parag_Parikh_Flexi_Cap_Fund PPF
ON
    PPF.START_DATE = (SELECT DISTINCT PROC_DATE FROM PROCESSING_DATE WHERE PROC_TYP_CD = 'PPF_MF_PROC')
    AND PPF.RECORD_DELETED_FLAG = 0
LEFT OUTER JOIN
    UTI_Nifty_50_Index_Fund UNI
ON
    UNI.START_DATE = (SELECT DISTINCT PROC_DATE FROM PROCESSING_DATE WHERE PROC_TYP_CD = 'MF_PROC')
    AND UNI.RECORD_DELETED_FLAG = 0

----  PREVIOUS NAV DATA    

LEFT OUTER JOIN
    Bandhan_Nifty_Alpha_50_Index_Fund BNA_PREV
ON
    BNA_PREV.START_DATE = (SELECT DISTINCT PREV_PROC_DATE FROM PROCESSING_DATE WHERE PROC_TYP_CD = 'MF_PROC')
    AND BNA_PREV.RECORD_DELETED_FLAG = 0 
LEFT OUTER JOIN
    HDFC_NIFTY_LargeMidcap_250_Index_Fund HLM_PREV
ON
    HLM_PREV.START_DATE = (SELECT DISTINCT PREV_PROC_DATE FROM PROCESSING_DATE WHERE PROC_TYP_CD = 'MF_PROC')
    AND HLM_PREV.RECORD_DELETED_FLAG = 0
LEFT OUTER JOIN
    HDFC_Small_Cap_Fund HSC_PREV
ON
    HSC_PREV.START_DATE = (SELECT DISTINCT PREV_PROC_DATE FROM PROCESSING_DATE WHERE PROC_TYP_CD = 'MF_PROC')
    AND HSC_PREV.RECORD_DELETED_FLAG = 0
LEFT OUTER JOIN
    ICICI_Prudential_Nifty50_Value_20_Index_Fund INV_PREV
ON
    INV_PREV.START_DATE = (SELECT DISTINCT PREV_PROC_DATE FROM PROCESSING_DATE WHERE PROC_TYP_CD = 'MF_PROC')
    AND INV_PREV.RECORD_DELETED_FLAG = 0
LEFT OUTER JOIN
    Motilal_Oswal_Active_Momentum_Fund MAM_PREV
ON
    MAM_PREV.START_DATE = (SELECT DISTINCT PREV_PROC_DATE FROM PROCESSING_DATE WHERE PROC_TYP_CD = 'MF_PROC')
    AND MAM_PREV.RECORD_DELETED_FLAG = 0
LEFT OUTER JOIN
    Motilal_Oswal_Midcap_Fund MMC_PREV
ON
    MMC_PREV.START_DATE = (SELECT DISTINCT PREV_PROC_DATE FROM PROCESSING_DATE WHERE PROC_TYP_CD = 'MF_PROC')
    AND MMC_PREV.RECORD_DELETED_FLAG = 0
LEFT OUTER JOIN
    Motilal_Oswal_Nifty_Capital_Market_Index_Fund MNC_PREV
ON
    MNC_PREV.START_DATE = (SELECT DISTINCT PREV_PROC_DATE FROM PROCESSING_DATE WHERE PROC_TYP_CD = 'MF_PROC')
    AND MNC_PREV.RECORD_DELETED_FLAG = 0
LEFT OUTER JOIN
    Nippon_India_Gold_Savings_Fund NGS_PREV
ON
    NGS_PREV.START_DATE = (SELECT DISTINCT PREV_PROC_DATE FROM PROCESSING_DATE WHERE PROC_TYP_CD = 'MF_PROC')
    AND NGS_PREV.RECORD_DELETED_FLAG = 0
LEFT OUTER JOIN
    Parag_Parikh_Flexi_Cap_Fund PPF_PREV
ON
    PPF_PREV.START_DATE = (SELECT DISTINCT PREV_PROC_DATE FROM PROCESSING_DATE WHERE PROC_TYP_CD = 'PPF_MF_PROC')
    AND PPF_PREV.RECORD_DELETED_FLAG = 0
LEFT OUTER JOIN
    UTI_Nifty_50_Index_Fund UNI_PREV
ON
    UNI_PREV.START_DATE = (SELECT DISTINCT PREV_PROC_DATE FROM PROCESSING_DATE WHERE PROC_TYP_CD = 'MF_PROC')
    AND UNI_PREV.RECORD_DELETED_FLAG = 0
WHERE
    (SELECT DISTINCT PROC_DATE FROM PROCESSING_DATE WHERE PROC_TYP_CD = 'MF_PROC') BETWEEN PO.START_DATE AND PO.END_DATE
    AND PO.RECORD_DELETED_FLAG = 0
) SUB;
'''