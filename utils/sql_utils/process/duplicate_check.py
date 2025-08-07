from utils.sql_utils.process.fetch_queries import fetch_queries_as_dictionaries
from utils.log_utils.duplicate_log import insert_into_duplicate_logs

def duplicate_check_on_managed_tables():
    duplicate_counter = 0
    duplicate_check_queries = fetch_queries_as_dictionaries("""
SELECT
    OUTER_SUB.TARGET_TABLE
    ,'SELECT ' || OUTER_SUB.KEY_COLUMN_LIST || ' ,COUNT(*) AS CNT FROM ' 
    || OUTER_SUB.TARGET_TABLE || ' WHERE RECORD_DELETED_FLAG = 0 GROUP BY ' || OUTER_SUB.KEY_COLUMN_LIST || ' HAVING CNT > 1;' AS DUPLICATE_CHECK_QUERIES
FROM
(
SELECT DISTINCT
    INNER_SUB.TARGET_TABLE
    ,GROUP_CONCAT(INNER_SUB.KEYCOLUMN_NAME) AS KEY_COLUMN_LIST
FROM
(
SELECT DISTINCT
    PR.TARGET_TABLE
    ,KEY.KEYCOLUMN_NAME
FROM
    METADATA_PROCESS PR
INNER JOIN
    METADATA_KEY_COLUMNS KEY
ON
    PR.OUT_PROCESS_NAME         = KEY.OUT_PROCESS_NAME
    AND PR.RECORD_DELETED_FLAG  = 0
    AND KEY.RECORD_DELETED_FLAG = 0
) INNER_SUB
GROUP BY 1
) OUTER_SUB;
    """)
    for duplicate_check_query in duplicate_check_queries:
        duplicate_data = fetch_queries_as_dictionaries(duplicate_check_query['DUPLICATE_CHECK_QUERIES'])
        for duplicate in duplicate_data:
            if duplicate['CNT']:
                duplicate_counter += 1
                insert_into_duplicate_logs(duplicate_check_query['TARGET_TABLE'], str(duplicate), duplicate['CNT'], duplicate_check_query['DUPLICATE_CHECK_QUERIES'])
    if duplicate_counter == 0:
        return({'message': 'No Duplicates Present in All Tables', 'status': 'Success'})
    else:
        return({'message': f'{duplicate_counter} instance(s) of duplicates present. Please check DUPLICATE_LOGS Table for more info', 'status': 'Duplicate Issue'})