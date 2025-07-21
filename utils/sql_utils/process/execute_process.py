from datetime import datetime, timedelta

from utils.sql_utils.scd2_framework.upsert_scd2_table import upsert_scd2
from utils.sql_utils.process.fetch_queries import fetch_queries_as_dictionaries
from utils.log_utils.insert_initial_log import insert_intitial_log_record
from utils.log_utils.update_log import update_log_record
from utils.sql_utils.sql_utils import \
get_first_purchase_date_from_all_portfolios,\
get_first_purchase_date_from_mf_order_date_table,\
get_first_swing_trade_date_from_trades_table,\
get_date_setup_from_holiday_calendar,\
update_proc_date_in_processing_date_table

def execute_process_using_metadata(process_name, start_date = None, end_date = None):
    payloads = []

    # Insert Initial Log
    process_id = insert_intitial_log_record(process_name)

    # Get process metadata and validate
    process_metadata_row = fetch_queries_as_dictionaries(f"SELECT * FROM METADATA_PROCESS WHERE PROCESS_NAME = '{process_name}';")
    if not process_metadata_row:
        return({'message': f'Process {process_name} Not Present in METADATA_PROCESS table', 'status': 'Failed'})
    # Check for Duplicate Process Entry
    elif len(process_metadata_row) > 1:
        return({'message': f'Duplicate entry present for Process {process_name} in METADATA_PROCESS table', 'status': 'Failed'})
    # Check if Process is decommissioned
    elif process_metadata_row[0]['PROCESS_DECOMMISSIONED'] == 1:
        return({'message': f'Process {process_name} has been Decommissioned in METADATA_PROCESS table', 'status': 'Failed'})
    
    process_metadata = process_metadata_row[0]

    # Get the Associated Proc Type Codes from Metadata
    proc_typ_cds_list = process_metadata['PROC_TYP_CD_LIST'].split(",")

    # Prepare Dates
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
    if end_date:
        end_date = datetime.strptime(end_date,'%Y-%m-%d')
    else:
        end_date = datetime.today() + timedelta(days = -7)

    counter_date = datetime.strptime(start_date,'%Y-%m-%d')

    if counter_date > end_date:
        return({'message': f'Start Date {start_date} is greater than End Date {end_date} for {process_name} Call', 'status': 'Failed'})

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
            
            # Input View Palyload
            input_view = process_metadata['INPUT_VIEW']
            input_view_rows = fetch_queries_as_dictionaries(f'SELECT * FROM {input_view};')
            for row in input_view_rows:
                payloads.append(row)

            counter_date = datetime.strptime(next_processing_date,'%Y-%m-%d')

    # SCD2 Processing
    if process_metadata['PROCESS_TYPE'] == 'SCD2':
        logs = upsert_scd2(process_name, process_metadata['TARGET_TABLE'], payloads, process_id, processing_date, prev_processing_date, next_processing_date)
    
    # Update the Initial Log Record
    update_log_record(process_name, process_id, logs['status'], logs['message'], logs['payload_count'], logs['inserted_count'], logs['updated_count'], logs['no_change_count'], str(logs['skipped_due_to_schema_mismatch']))

    return({'message': f'Process {process_name} Successfully Completed', 'status': 'Success'})
