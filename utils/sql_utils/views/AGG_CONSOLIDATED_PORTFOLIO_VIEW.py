AGG_CONSOLIDATED_PORTFOLIO_VIEW = '''
CREATE VIEW AGG_CONSOLIDATED_PORTFOLIO_VIEW AS
SELECT
    CPV_UNREALISED.PORTFOLIO_TYPE                                                     AS PORTFOLIO_TYPE
    ,CPV_UNREALISED.PROCESSING_DATE                                                   AS PROCESSING_DATE
    ,CPV_UNREALISED.PREV_PROCESSING_DATE                                              AS PREV_PROCESSING_DATE
    ,CPV_UNREALISED.NEXT_PROCESSING_DATE                                              AS NEXT_PROCESSING_DATE
    ,CPV_UNREALISED.INVESTED_AMOUNT                                                   AS AGG_INVESTED_AMOUNT
    ,CPV_UNREALISED.CURRENT_VALUE                                                     AS AGG_CURRENT_VALUE
    ,CPV_UNREALISED.PREVIOUS_VALUE                                                    AS AGG_PREVIOUS_VALUE
    ,CPV_UNREALISED."TOTAL_P/L"                                                       AS "AGG_TOTAL_P/L"
    ,CPV_UNREALISED."DAY_P/L"                                                         AS "AGG_DAY_P/L"
    ,CPV_UNREALISED."%_TOTAL_P/L"                                                     AS "%_AGG_TOTAL_P/L"
    ,CPV_UNREALISED."%_DAY_P/L"                                                       AS "%_AGG_DAY_P/L"
FROM
    CONSOLIDATED_PORTFOLIO_VIEW CPV_UNREALISED
INNER JOIN
(
SELECT
    SUB.PORTFOLIO_TYPE         AS PORTFOLIO_TYPE
    ,MAX(SUB.PROCESSING_DATE)  AS PROCESSING_DATE
FROM
    CONSOLIDATED_PORTFOLIO_VIEW SUB
GROUP BY 1
) MX_PROC
ON
    MX_PROC.PORTFOLIO_TYPE      = CPV_UNREALISED.PORTFOLIO_TYPE
    AND MX_PROC.PROCESSING_DATE = CPV_UNREALISED.PROCESSING_DATE
WHERE
    CPV_UNREALISED.PORTFOLIO_TYPE NOT IN ('Intraday Stocks', 'Realised Swing Stocks')

UNION ALL

SELECT
    CPV_INTRADAY.PORTFOLIO_TYPE                                                       AS PORTFOLIO_TYPE
    ,NULL                                                                             AS PROCESSING_DATE
    ,NULL                                                                             AS PREV_PROCESSING_DATE
    ,NULL                                                                             AS NEXT_PROCESSING_DATE
    ,MAX(CPV_INTRADAY.INVESTED_AMOUNT)                                                AS AGG_INVESTED_AMOUNT
    ,MAX(CPV_INTRADAY.INVESTED_AMOUNT) +  SUM(CPV_INTRADAY."TOTAL_P/L")               AS AGG_CURRENT_VALUE
    ,0                                                                                AS AGG_PREVIOUS_VALUE
    ,SUM(CPV_INTRADAY."TOTAL_P/L")                                                    AS "AGG_TOTAL_P/L"
    ,SUM(CPV_INTRADAY."DAY_P/L")                                                      AS "AGG_DAY_P/L"
    ,ROUND(SUM(CPV_INTRADAY."TOTAL_P/L")/MAX(CPV_INTRADAY.INVESTED_AMOUNT) * 100,2)   AS "%_AGG_TOTAL_P/L"
    ,ROUND(SUM(CPV_INTRADAY."TOTAL_P/L")/MAX(CPV_INTRADAY.INVESTED_AMOUNT) * 100,2)   AS "%_AGG_DAY_P/L"
FROM
    CONSOLIDATED_PORTFOLIO_VIEW CPV_INTRADAY
WHERE
    CPV_INTRADAY.PORTFOLIO_TYPE = 'Intraday Stocks'
GROUP BY 1

UNION ALL

SELECT
    CPV_REALISED.PORTFOLIO_TYPE                                                       AS PORTFOLIO_TYPE
    ,NULL                                                                             AS PROCESSING_DATE
    ,NULL                                                                             AS PREV_PROCESSING_DATE
    ,NULL                                                                             AS NEXT_PROCESSING_DATE
    ,SUM(CPV_REALISED.INVESTED_AMOUNT)                                                AS AGG_INVESTED_AMOUNT
    ,SUM(CPV_REALISED.CURRENT_VALUE)                                                  AS AGG_CURRENT_VALUE
    ,0                                                                                AS AGG_PREVIOUS_VALUE
    ,SUM(CPV_REALISED."TOTAL_P/L")                                                    AS "AGG_TOTAL_P/L"
    ,SUM(CPV_REALISED."DAY_P/L")                                                      AS "AGG_DAY_P/L"
    ,ROUND(SUM(CPV_REALISED."TOTAL_P/L")/SUM(CPV_REALISED.INVESTED_AMOUNT) * 100,2)   AS "%_AGG_TOTAL_P/L"
    ,ROUND(SUM(CPV_REALISED."TOTAL_P/L")/SUM(CPV_REALISED.INVESTED_AMOUNT) * 100,2)   AS "%_AGG_DAY_P/L"
FROM
    CONSOLIDATED_PORTFOLIO_VIEW CPV_REALISED
WHERE
    CPV_REALISED.PORTFOLIO_TYPE = 'Realised Swing Stocks'
GROUP BY 1
;
'''