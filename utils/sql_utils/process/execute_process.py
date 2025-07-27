from datetime import datetime

from utils.sql_utils.scd2_framework.upsert_scd2_table import upsert_scd2
from utils.sql_utils.process.fetch_queries import fetch_queries_as_dictionaries
from utils.log_utils.insert_initial_log import insert_intitial_log_record
from utils.log_utils.update_log import update_log_record
from utils.sql_utils.sql_utils import \
get_first_purchase_date_from_all_portfolios,\
get_first_purchase_date_from_mf_order_date_table,\
get_first_swing_trade_date_from_trades_table,\
get_date_setup_from_holiday_calendar,\
update_proc_date_in_processing_date_table,\
get_max_value_date_by_portfolio_type,\
get_max_next_processing_date_from_table

def execute_process_using_metadata(process_name, start_date = None, end_date = None):
    payloads = []

    # Insert Initial Log
    process_id = insert_intitial_log_record(process_name)

    # Get process metadata and validate
    process_metadata_row = fetch_queries_as_dictionaries(f"SELECT * FROM METADATA_PROCESS WHERE PROCESS_NAME = '{process_name}';")
    if not process_metadata_row[0]['PROCESS_NAME']:
        message = f'Process {process_name} is Not Present in METADATA_PROCESS table'
        update_log_record(process_name, process_id, 'Failed', message, start_date, end_date.date(), None, None, None, None, None, None)
        return({'message': message, 'status': 'Failed'})
    # Check for Duplicate Process Entry
    elif len(process_metadata_row) > 1:
        message = f'Duplicate entry present for Process {process_name} in METADATA_PROCESS table'
        update_log_record(process_name, process_id, 'Failed', message, start_date, end_date.date(), None, None, None, None, None, None)
        return({'message': message, 'status': 'Failed'})
    # Check if Process is decommissioned
    elif process_metadata_row[0]['PROCESS_DECOMMISSIONED'] == 1:
        message = f'Process {process_name} has been Decommissioned in METADATA_PROCESS table'
        update_log_record(process_name, process_id, 'Failed', message, start_date, end_date.date(), None, None, None, None, None, None)
        return({'message': message, 'status': 'Failed'})

    process_metadata = process_metadata_row[0]

    # Get the Associated Proc Type Codes from Metadata
    proc_typ_cds_list = process_metadata['PROC_TYP_CD_LIST'].split(",")

    # Prepare Start Date
    if process_metadata['FREQUENCY'] == 'Ad hoc':
        if not start_date:
            if process_metadata['DEFAULT_START_DATE_TYPE_CD'] == 'ALL':
                first_purchase_data_across_portfolio_type = get_first_purchase_date_from_all_portfolios()
                first_purchase_data_across_portfolio_type = first_purchase_data_across_portfolio_type.get_json()
                start_date = first_purchase_data_across_portfolio_type['first_purchase_date']
            if process_metadata['DEFAULT_START_DATE_TYPE_CD'] == 'MUTUAL_FUND':
                first_mf_purchase_data = get_first_purchase_date_from_mf_order_date_table()
                first_mf_purchase_data = first_mf_purchase_data.get_json()
                start_date = first_mf_purchase_data['first_purchase_date']
            if process_metadata['DEFAULT_START_DATE_TYPE_CD'] == 'STOCK':
                first_swing_trade_data = get_first_swing_trade_date_from_trades_table()
                first_swing_trade_data = first_swing_trade_data.get_json()
                start_date = first_swing_trade_data['first_trade_date']
    elif process_metadata['FREQUENCY'] == 'On Start':
        if not start_date:
            max_next_proc_date_from_target_table = get_max_next_processing_date_from_table(process_metadata['TARGET_TABLE'])
            start_date = max_next_proc_date_from_target_table[0]['MAX(NEXT_PROCESSING_DATE)']

    # Prepare End Date
    if end_date:
        end_date = datetime.strptime(end_date,'%Y-%m-%d')
    else:
        if process_metadata['DEFAULT_START_DATE_TYPE_CD'] == 'ALL':
            max_value_date_for_each_portfolio = get_max_value_date_by_portfolio_type()
        if process_metadata['DEFAULT_START_DATE_TYPE_CD'] == 'MUTUAL_FUND':
            max_value_date_for_each_portfolio = get_max_value_date_by_portfolio_type('Mutual Fund')
        if process_metadata['DEFAULT_START_DATE_TYPE_CD'] == 'STOCK':
            max_value_date_for_each_portfolio = get_max_value_date_by_portfolio_type('Stock')
        min_value_date = datetime.strptime('9998-12-31','%Y-%m-%d') # Setting Minumum to high end date
        for portfolio in max_value_date_for_each_portfolio:
            max_value_date_in_portfolio = datetime.strptime(portfolio['MAX(PT.VALUE_DATE)'],'%Y-%m-%d')
            if max_value_date_in_portfolio < min_value_date:
                min_value_date = max_value_date_in_portfolio
        end_date = min_value_date

    counter_date = datetime.strptime(start_date,'%Y-%m-%d')

    if counter_date > end_date and process_metadata['FREQUENCY'] == 'On Start':
        message = f'Start Date {start_date} is greater than End Date {end_date.date()} for {process_name}'
        update_log_record(process_name, process_id, 'Skipped', message, start_date, end_date.date(), None, None, None, None, None, None)
        return({'message': message, 'status': 'Success'})
    elif counter_date > end_date and process_metadata['FREQUENCY'] != 'On Start':
        message = f'Start Date {start_date} is greater than End Date {end_date.date()} for {process_name}'
        update_log_record(process_name, process_id, 'Failed', message, start_date, end_date.date(), None, None, None, None, None, None)
        return({'message': message, 'status': 'Failed'})

    # Prepare Loop
    while(counter_date <= end_date):
            holiday_calendar_data = get_date_setup_from_holiday_calendar(counter_date.strftime('%Y-%m-%d'))
            holiday_calendar_data = holiday_calendar_data.get_json()
            processing_date = holiday_calendar_data[0]['processing_date']
            next_processing_date = holiday_calendar_data[0]['next_processing_date']
            prev_processing_date = holiday_calendar_data[0]['prev_processing_date']

            # Update Processing Dates Table
            for proc_typ_cd in proc_typ_cds_list:
                update_proc_date_in_processing_date_table(proc_typ_cd, processing_date, next_processing_date, prev_processing_date)

            # Input View Payload
            input_view_rows = fetch_queries_as_dictionaries(f"SELECT INP.* FROM {process_metadata['INPUT_VIEW']} INP;")
            for row in input_view_rows:
                payloads.append(row)

            counter_date = datetime.strptime(next_processing_date,'%Y-%m-%d')

    # SCD2 Processing
    if process_metadata['PROCESS_TYPE'] == 'SCD2':
        logs = upsert_scd2(process_name, process_metadata['TARGET_TABLE'], payloads, process_id)

    update_log_record(process_name, process_id, logs['status'], logs['message'], start_date, end_date.date(), logs['payload_count'], logs['inserted_count'], logs['updated_count'], logs['no_change_count'], logs['skipped_count'], logs['null_count'], str(logs['skipped_due_to_schema_mismatch']))

    return({'message': logs['message'], 'status': logs['status']})
