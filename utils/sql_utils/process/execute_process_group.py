from utils.sql_utils.process.execute_process import execute_process_using_metadata
from utils.sql_utils.process.fetch_queries import fetch_queries_as_dictionaries

def execute_process_group_using_metadata(process_group_name, start_date = None, end_date = None, payload_from_source = None, payload_sent_from_source = None):
    process_group_metadata_rows = fetch_queries_as_dictionaries(f"SELECT * FROM METADATA_PROCESS_GROUP WHERE PROCESS_GROUP = '{process_group_name}' AND CONSIDER_FOR_PROCESSING = 1 ORDER BY EXECUTION_ORDER;")
    if not process_group_metadata_rows[0]['PROCESS_GROUP']:
        message = f'Process {process_group_name} is Not Present in METADATA_PROCESS_GROUP table'
        return({'message': message, 'status': 'Failed'})

    process_group_logs = {}
    process_group_message = f"Process Group {process_group_name} Completed Successfully"
    process_group_status = "Success"
    
    try:
        for process_group_metadata in process_group_metadata_rows:
            process_group_logs[process_group_metadata['OUT_PROCESS_NAME']] = execute_process_using_metadata(process_group_metadata['OUT_PROCESS_NAME'], start_date, end_date, payload_from_source, payload_sent_from_source)
            if process_group_logs[process_group_metadata['OUT_PROCESS_NAME']]['status'] == "Failed":
                process_group_status = "Failed"
                process_group_message = f"Failures in Process Group {process_group_name}"
        return({'message': process_group_message, 'status': process_group_status})
    except Exception as e:
        return ({'message': repr(e), 'status': 'Failed'})
