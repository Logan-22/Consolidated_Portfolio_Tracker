from os import getenv
from utils.connection_utils.connection_pool_config import connection_pool
from utils.sql_utils.process.fetch_queries import fetch_queries_as_dictionaries
from datetime import date

env = getenv('ENVIRONMENT')

def create_utility_schema(utility_schema = f"{env}T_UTIL"):
    conn = connection_pool.get_connection()
    cursor = conn.cursor()
    cursor.execute(f"""
CREATE DATABASE IF NOT EXISTS {utility_schema}
CHARACTER SET utf8mb4
COLLATE utf8mb4_unicode_ci;
    """)
    cursor.close()
    conn.close()

def create_processing_date_table(utility_schema = f"{env}T_UTIL"):
    conn = connection_pool.get_connection()
    cursor = conn.cursor()
    cursor.execute(f"""
CREATE TABLE IF NOT EXISTS {utility_schema}.PROCESSING_DATE
(
    ID             INT                      AUTO_INCREMENT PRIMARY KEY,
    PROC_TYP_CD    VARCHAR (100),
    PROC_DATE      DATE,
    NEXT_PROC_DATE DATE,
    PREV_PROC_DATE DATE
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_unicode_ci;
    """)
    processing_date_count_data = fetch_queries_as_dictionaries(f"SELECT COUNT(*) AS RCOUNT FROM {utility_schema}.PROCESSING_DATE;", "return_none_list")
    if processing_date_count_data[0]['RCOUNT'] == 0:
        proc_date_typ_cds = [('MF_PROC', date.today(),date.today(),date.today())
                             ,('STOCK_PROC', date.today(),date.today(),date.today())
                             ,('SIM_MF_PROC', date.today(),date.today(),date.today())
                             ,('SIM_STOCK_PROC', date.today(),date.today(),date.today())
                             ]
        insert_query = f"INSERT INTO {utility_schema}.PROCESSING_DATE (PROC_TYP_CD, PROC_DATE, NEXT_PROC_DATE, PREV_PROC_DATE) VALUES(%s, %s, %s, %s)"
        for proc_date_typ_cd_values in proc_date_typ_cds:
            cursor.execute(insert_query, proc_date_typ_cd_values)
    conn.commit()
    cursor.close()
    conn.close()

def create_processing_type_table(utility_schema = f"{env}T_UTIL"):
    conn = connection_pool.get_connection()
    cursor = conn.cursor()
    cursor.execute(f"""
CREATE TABLE IF NOT EXISTS {utility_schema}.PROCESSING_TYPE
(
    ID               INT                      AUTO_INCREMENT PRIMARY KEY,
    PROC_TYP_CD      VARCHAR (100),
    PROC_TYPE        VARCHAR (100),
    PROC_DESCRIPTION VARCHAR (100) 
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_unicode_ci;
    """)
    processing_type_count_data = fetch_queries_as_dictionaries(f"SELECT COUNT(*) AS RCOUNT FROM {utility_schema}.PROCESSING_TYPE;", "return_none_list")
    if processing_type_count_data[0]['RCOUNT'] == 0:
        cursor.execute(f"INSERT INTO {utility_schema}.PROCESSING_TYPE (PROC_TYP_CD, PROC_TYPE, PROC_DESCRIPTION) VALUES (%s, %s, %s)",
                      ('SIMULATED_RETURNS', 'nifty_50', 'NIFTY 50'))
    conn.commit()
    cursor.close()
    conn.close()