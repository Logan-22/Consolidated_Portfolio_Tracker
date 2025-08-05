import sqlite3
from utils.folder_utils.paths import db_path

def update_proc_date_in_processing_date_table(proc_typ_cd, proc_date, next_proc_date, prev_proc_date):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(f"UPDATE PROCESSING_DATE SET PROC_DATE = '{proc_date}', NEXT_PROC_DATE = '{next_proc_date}', PREV_PROC_DATE = '{prev_proc_date}' WHERE PROC_TYP_CD = '{proc_typ_cd}';")
    conn.commit()
    conn.close()
