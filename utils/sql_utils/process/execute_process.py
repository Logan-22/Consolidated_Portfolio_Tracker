from datetime import datetime
from flask import current_app

from utils.sql_utils.scd2_framework.upsert_scd2_table import upsert_scd2
from utils.sql_utils.scd1_framework.delsert_scd1_table import delsert_scd1
from utils.sql_utils.insert_append_framework.insert_append_table import insert_append
from utils.sql_utils.process.fetch_queries import fetch_queries_as_dictionaries
from utils.log_utils.insert_initial_log import insert_intitial_log_record
from utils.log_utils.update_log import update_log_record
from utils.sql_utils.query_db.get_or_process_in_db import \
get_first_purchase_date_from_all_portfolios,\
get_first_purchase_date_from_mf_txn_table,\
get_first_trade_date_from_trades_table,\
get_date_setup_from_holiday_calendar,\
get_max_value_date_by_portfolio_type,\
get_max_next_processing_date_from_table
from utils.sql_utils.query_db.update_in_db import \
update_table_with_payload

def execute_process_using_metadata(process_name, start_date = None, end_date = None, payload_from_source = None, process_frequency = None, user_id = None, instrument_ids = None):
    try:
        payloads          = []
        proc_typ_cds_list = []
        env = current_app.config['ENVIRONMENT']

        # Insert Initial Log
        process_id = insert_intitial_log_record(process_name)
        if end_date:
            end_date = datetime.strptime(end_date,'%Y-%m-%d').date()
            log_end_date = datetime.strftime(end_date,'%Y-%m-%d')
        if start_date:
            start_date = datetime.strptime(start_date,'%Y-%m-%d').date()

        # Get process metadata and validate
        process_metadata = fetch_queries_as_dictionaries(f"""
SELECT
    OUT_PROCESS_NAME
    ,PROCESS_TYPE
    ,PROC_TYP_CD_LIST
    ,INPUT_DATABASE
    ,INPUT_VIEW
    ,TARGET_DATABASE
    ,TARGET_TABLE
    ,DEFAULT_START_DATE_TYPE_CD
    ,PROCESS_DECOMMISSIONED
    ,INSTRUMENT_LEVEL_PROCESS
    ,USER_LEVEL_PROCESS
FROM
    {env}T_META.METADATA_PROCESS
WHERE
    OUT_PROCESS_NAME           = '{process_name}'
    AND RECORD_DELETED_FLAG    = 0;
    """, "return_none", fetch = 'One')
        if not process_metadata:
            message = f'Process {process_name} is Not Present in METADATA_PROCESS table'
            update_log_record(process_name, process_id, 'Failed', message, None, None, None, None, None, None, None, None, None)
            return({'message': message, 'status': 'Failed'})
        # Check if Process is decommissioned
        elif process_metadata['PROCESS_DECOMMISSIONED'] == 1:
            message = f'Process {process_name} has been Decommissioned in METADATA_PROCESS table'
            update_log_record(process_name, process_id, 'Failed', message, None, None, None, None, None, None, None, None, None)
            return({'message': message, 'status': 'Failed'})

        if payload_from_source is not None:
            if type(payload_from_source).__name__ == 'dict':
                payloads = [payload_from_source] # Skip to data load if the payload is already present
            elif type(payload_from_source).__name__ == 'list':
                payloads = payload_from_source
            else:
                message = f'Expected dict/list payloads, but received {type(payload_from_source).__name__}'
                update_log_record(process_name, process_id, 'Failed', message, None, None, None, None, None, None, None, None, None)
                return({'message': message, 'status': 'Failed'})
        elif not process_metadata['INPUT_DATABASE'] and not process_metadata['INPUT_VIEW']:
            update_log_record(process_name, process_id, 'Skipped', 'No Payload is provided for the process and No Associated Input Views Found for the process', None, None, None, None, None, None, None, None, None)
            return({'message': 'No Payload is provided for the process and No Associated Input Views Found for the process', 'status': 'Skipped'})
        else:
            # Get the Associated Proc Type Codes from Metadata
            if process_metadata['PROC_TYP_CD_LIST']:
                proc_typ_cds_list = process_metadata['PROC_TYP_CD_LIST'].split(",")

            # Prepare Start Date
            if process_frequency == 'Ad hoc':
                if not start_date:
                    if process_metadata['DEFAULT_START_DATE_TYPE_CD'] == 'ALL':
                        first_purchase_data_across_portfolio_type = get_first_purchase_date_from_all_portfolios(user_id)
                        start_date = first_purchase_data_across_portfolio_type['FIRST_PURCHASE_DATE']
                    if process_metadata['DEFAULT_START_DATE_TYPE_CD'] == 'MUTUAL_FUND':
                        first_mf_purchase_data = get_first_purchase_date_from_mf_txn_table(user_id)
                        start_date = first_mf_purchase_data['MF_FIRST_PURCHASE_DATE']
                    if process_metadata['DEFAULT_START_DATE_TYPE_CD'] == 'STOCK':
                        first_swing_trade_data = get_first_trade_date_from_trades_table(user_id)
                        start_date = first_swing_trade_data['FIRST_TRADE_DATE']
            elif process_frequency == 'On Start':
                if not start_date:
                    max_next_proc_date_from_target_table = get_max_next_processing_date_from_table(process_metadata['TARGET_DATABASE'], process_metadata['TARGET_TABLE'])
                    start_date = max_next_proc_date_from_target_table['NEXT_PROCESSING_DATE']

            # Prepare End Date
            if not end_date:
                if process_metadata['DEFAULT_START_DATE_TYPE_CD'] == 'ALL':
                    max_value_date_for_each_portfolio = get_max_value_date_by_portfolio_type()
                if process_metadata['DEFAULT_START_DATE_TYPE_CD'] == 'MUTUAL_FUND':
                    max_value_date_for_each_portfolio = get_max_value_date_by_portfolio_type('Mutual Fund')
                if process_metadata['DEFAULT_START_DATE_TYPE_CD'] == 'STOCK':
                    max_value_date_for_each_portfolio = get_max_value_date_by_portfolio_type('Stock')
                min_value_date = datetime.strptime('9998-12-31','%Y-%m-%d').date() # Setting Minimum to high end date
                for portfolio in max_value_date_for_each_portfolio:
                    if portfolio.get('MAX_PRICE_DATE') is not None:
                        if portfolio['MAX_PRICE_DATE'] < min_value_date:
                            min_value_date = portfolio['MAX_PRICE_DATE']
                end_date = min_value_date

            counter_date = start_date
            log_end_date = datetime.strftime(end_date,'%Y-%m-%d')

            if counter_date > end_date and process_frequency == 'On Start':
                message = f"Start Date {start_date} is greater than End Date {log_end_date} for {process_name}"
                update_log_record(process_name, process_id, 'Skipped', message, start_date, log_end_date, None, None, None, None, None, None, None)
                return({'message': message, 'status': 'Success'})
            elif counter_date > end_date and process_frequency != 'On Start':
                message = f"Start Date {start_date} is greater than End Date {log_end_date} for {process_name}"
                update_log_record(process_name, process_id, 'Failed', message, start_date, log_end_date, None, None, None, None, None, None, None)
                return({'message': message, 'status': 'Failed'})

            # Prepare Loop
            while(counter_date <= end_date):
                    holiday_calendar_data = get_date_setup_from_holiday_calendar(counter_date.strftime('%Y-%m-%d'))
                    processing_date          = holiday_calendar_data['PROCESSING_DATE']
                    next_processing_date     = holiday_calendar_data['NEXT_PROCESSING_DATE']
                    previous_processing_date = holiday_calendar_data['PREVIOUS_PROCESSING_DATE']

                    # Update Processing Dates Table
                    for proc_typ_cd in proc_typ_cds_list:
                        update_user_payload = {
                            'data': {
                                'PROCESSING_DATE'           : processing_date
                                ,'PREVIOUS_PROCESSING_DATE' : previous_processing_date
                                ,'NEXT_PROCESSING_DATE'     : next_processing_date
                            },
                            'conditions': {
                                'PROC_TYP_CD' : proc_typ_cd
                            }
                        }
                        update_table_with_payload(f"{env}T_UTIL", "PROCESSING_DATE", update_user_payload)

                    # Input View Payload
                    user_id_filter = f"AND INP.USER_ID = {user_id}" if user_id and process_metadata['USER_LEVEL_PROCESS'] == 1 else ""
                    instrument_id_filter = f"AND INP.INSTRUMENT_ID IN ({instrument_ids})" if instrument_ids and process_metadata['INSTRUMENT_LEVEL_PROCESS'] == 1 else ""
                    input_view_rows = fetch_queries_as_dictionaries(f"SELECT INP.* FROM {process_metadata['INPUT_DATABASE']}.{process_metadata['INPUT_VIEW']} INP WHERE 1 = 1 {user_id_filter} {instrument_id_filter};")
                    for row in input_view_rows:
                        payloads.append(row)
                    counter_date = next_processing_date

        # SCD2 Processing
        if process_metadata['PROCESS_TYPE'] == 'SCD2':
            logs = upsert_scd2(process_name, process_metadata['TARGET_DATABASE'], process_metadata['TARGET_TABLE'], payloads, process_id)
        elif process_metadata['PROCESS_TYPE'] == 'SCD1':
            logs = delsert_scd1(process_name, process_metadata['TARGET_DATABASE'], process_metadata['TARGET_TABLE'], payloads, process_id)
        elif process_metadata['PROCESS_TYPE'] == 'INS':
            logs = insert_append(process_name, process_metadata['TARGET_DATABASE'], process_metadata['TARGET_TABLE'], payloads, process_id)

        if end_date:
            update_log_record(process_name, process_id, logs['status'], logs['message'], start_date, log_end_date, logs['payload_count'], logs['inserted_count'], logs['updated_count'], logs['deleted_count'], logs['no_change_count'], logs['skipped_count'], logs['null_count'], str(logs['skipped_due_to_schema_mismatch']))
        else:
            update_log_record(process_name, process_id, logs['status'], logs['message'], None, None, logs['payload_count'], logs['inserted_count'], logs['updated_count'], logs['deleted_count'], logs['no_change_count'], logs['skipped_count'], logs['null_count'], str(logs['skipped_due_to_schema_mismatch']))

        return({'message': logs['message'], 'status': logs['status']})
    except Exception as e:
        update_log_record(process_name, process_id, 'Failed', repr(e), None, None, None, None, None, None, None, None, None)
