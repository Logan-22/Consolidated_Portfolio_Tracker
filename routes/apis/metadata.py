from flask import Blueprint, request, jsonify
from json import loads
from datetime import datetime, timedelta
from utils.sql_utils.process.execute_process_group import execute_process_group_using_metadata
from utils.sql_utils.query_db.get_or_process_in_db import\
get_or_create_instrument_id,\
get_all_symbols_list_from_metadata_store,\
get_holiday_dates,\
get_working_dates
from utils.date_utils.date_utils import convert_weekday_from_int_to_char

metadata_bp = Blueprint('metadata', __name__)

@metadata_bp.route('/instruments/', methods = ['POST'])
def metadata_instruments_entry():
    try:
        metadata_instruments_payload_unprocessed = loads(request.form.get('metadata_instruments_payload'))
        metadata_instruments_payload = {key : (None if value == "" else value) for key, value in metadata_instruments_payload_unprocessed.items()} # Convert "" to None so that it can be inserted as NULL in Mysql DB
        metadata_instruments_payload['INSTRUMENT_ID'] = get_or_create_instrument_id(metadata_instruments_payload['EXCHANGE_SYMBOL'], process_type = 'create')

        metadata_entry_final_payload = {
            'PR_METADATA_INSTRUMENTS_LOAD' : metadata_instruments_payload
        }
        metadata_entry_logs = execute_process_group_using_metadata('PG_METADATA_INSTRUMENTS_LOAD', payloads = metadata_entry_final_payload)
        return jsonify(metadata_entry_logs)
    except Exception as e:
        return jsonify({'message': repr(e), 'status': "Failed"})

@metadata_bp.route('/instruments/symbols/', methods = ['GET'])
def get_all_symbols_list():
    try:
        portfolio_type = request.args.get('portfolio_type') or None
        all_symbols_data = get_all_symbols_list_from_metadata_store(portfolio_type)
        return jsonify({'all_symbols_list': all_symbols_data, 'message': "Successfully retrieved All Symbols List from METADATA_INSTRUMENTS Table", 'status': "Success"})
    except Exception as e:
        return jsonify({'message': repr(e), 'status': "Failed"})

@metadata_bp.route('/holiday_date/', methods = ['GET'])
def holiday_dates_lookup():
    try:
        year = request.args.get('year') or None
        data = get_holiday_dates(year)
        return jsonify({'data': data,'message': 'Successfully retrieved from HOLIDAY_DATES Table','status': 'Success'})
    except Exception as e:
        return jsonify({'message': repr(e), 'status': 'Failed'})

@metadata_bp.route('/holiday_date/', methods = ['POST'])
def holiday_date_entry():
    try:
        holiday_date_payload = loads(request.form.get('holiday_date_payload'))
        holiday_dates_final_payload = {
            'PR_HOLIDAY_DATE_LOAD' : holiday_date_payload
        }
        holiday_load_logs = execute_process_group_using_metadata('PG_HOLIDAY_DATE_LOAD', payloads = holiday_dates_final_payload)
        return jsonify(holiday_load_logs)
    except Exception as e:
        return jsonify({'message': repr(e), 'status': 'Failed'})

@metadata_bp.route('/working_date/', methods = ['POST'])
def working_date_entry():
    try:
        working_date_payload = loads(request.form.get('working_date_payload'))
        working_dates_final_payload = {
            'PR_WORKING_DATE_LOAD' : working_date_payload
        }
        working_day_load_logs = execute_process_group_using_metadata('PG_WORKING_DATE_LOAD', payloads = working_dates_final_payload)
        return jsonify(working_day_load_logs)
    except Exception as e:
        return jsonify({'message': repr(e), 'status': 'Failed'})

@metadata_bp.route('/holiday_calendar_setup/', methods = ['GET'])
def holiday_calendar_setup():
    try:
        start_date       = request.args.get('start_date')
        end_date         = request.args.get('end_date')
        holiday_data     = get_holiday_dates()
        working_day_data = get_working_dates()
        holiday_dates    = [holiday['HOLIDAY_DATE'] for holiday in holiday_data]
        working_dates    = [workday['WORKING_DATE'] for workday in working_day_data]
        holiday_payloads = []

        if start_date and end_date:
            counter_date = datetime.strptime(start_date,'%Y-%m-%d')
            while(counter_date <= datetime.strptime(end_date,'%Y-%m-%d')):
                counter_day = convert_weekday_from_int_to_char(counter_date.weekday())
                next_counter_date = counter_date
                prev_counter_date = counter_date
                if (counter_date.weekday() >= 0 and counter_date.weekday() <= 4 and str(counter_date.strftime('%Y-%m-%d')) not in holiday_dates) or (str(counter_date.strftime('%Y-%m-%d')) in working_dates):
                    next_weekday_flag = 0
                    prev_weekday_flag = 0
                    while(next_weekday_flag == 0):
                        next_counter_date = next_counter_date + timedelta(days = 1)
                        if (next_counter_date.weekday() >= 0 and next_counter_date.weekday() <= 4 and str(next_counter_date.strftime('%Y-%m-%d')) not in holiday_dates) or (str(next_counter_date.strftime('%Y-%m-%d')) in working_dates):
                            next_weekday_flag = 1
                    while(prev_weekday_flag == 0):
                        prev_counter_date = prev_counter_date + timedelta(days = -1)
                        if (prev_counter_date.weekday() >= 0 and prev_counter_date.weekday() <= 4 and str(prev_counter_date.strftime('%Y-%m-%d')) not in holiday_dates) or (str(prev_counter_date.strftime('%Y-%m-%d')) in working_dates):
                            prev_weekday_flag = 1
                    next_counter_day = convert_weekday_from_int_to_char(next_counter_date.weekday())
                    prev_counter_day = convert_weekday_from_int_to_char(prev_counter_date.weekday())
                    
                    holiday_computed_payload = {
                        'PROCESSING_DATE'           : counter_date.strftime('%Y-%m-%d')
                        ,'PROCESSING_DAY'           : counter_day
                        ,'NEXT_PROCESSING_DATE'     : next_counter_date.strftime('%Y-%m-%d')
                        ,'NEXT_PROCESSING_DAY'      : next_counter_day
                        ,'PREVIOUS_PROCESSING_DATE' : prev_counter_date.strftime('%Y-%m-%d')
                        ,'PREVIOUS_PROCESSING_DAY'  : prev_counter_day
                    }
                    holiday_payloads.append(holiday_computed_payload)
                counter_date = counter_date + timedelta(days = 1)

            holiday_setup_final_payload = {
                'PR_HOLIDAY_CALENDAR_SETUP' : holiday_payloads
            }
            holiday_calendar_setup_logs = execute_process_group_using_metadata('PG_HOLIDAY_CALENDAR_SETUP', start_date, end_date, holiday_setup_final_payload)
            return jsonify(holiday_calendar_setup_logs)
        else:
            return jsonify({'message': 'Start Date and End Date is required for Holiday Calendar Setup', 'status': 'Failed'})
    except Exception as e:
        return jsonify({'message': repr(e), 'status': 'Failed'})

@metadata_bp.route('/process_entry/', methods = ['POST'])
def add_process_entry():
    try:
        process_entry_payloads   = loads(request.form.get('process_entry_values')) # json.loads
        process_group_payloads   = []
        process_payloads         = []
        process_columns_payloads = []
        for payload in process_entry_payloads:
            process_group_payload = {
                'PROCESS_GROUP'             : payload['PROCESS_GROUP']
                ,'OUT_PROCESS_NAME'         : payload['OUT_PROCESS_NAME']
                ,'CONSIDER_FOR_PROCESSING'  : payload['CONSIDER_FOR_PROCESSING']
                ,'EXECUTION_ORDER'          : payload['EXECUTION_ORDER']
            }
            process_group_payloads.append(process_group_payload)

            process_payload = {
                'OUT_PROCESS_NAME'            : payload['OUT_PROCESS_NAME']
                ,'PROCESS_TYPE'               : payload['PROCESS_TYPE']
                ,'PROC_TYP_CD_LIST'           : payload['PROC_TYP_CD_LIST']
                ,'INPUT_DATABASE'             : payload['INPUT_DATABASE']
                ,'INPUT_VIEW'                 : payload['INPUT_VIEW']
                ,'TARGET_DATABASE'            : payload['TARGET_DATABASE']
                ,'TARGET_TABLE'               : payload['TARGET_TABLE']
                ,'PROCESS_DESCRIPTION'        : payload['PROCESS_DESCRIPTION']
                ,'AUTO_TRIGGER_ON_LAUNCH'     : payload['AUTO_TRIGGER_ON_LAUNCH']
                ,'PROCESS_DECOMMISSIONED'     : payload['PROCESS_DECOMMISSIONED']
                ,'INSTRUMENT_LEVEL_PROCESS'   : payload['INSTRUMENT_LEVEL_PROCESS']
                ,'USER_LEVEL_PROCESS'         : payload['USER_LEVEL_PROCESS']
                ,'DEFAULT_START_DATE_TYPE_CD' : payload['DEFAULT_START_DATE_TYPE_CD']
            }
            process_payloads.append(process_payload)

            for column_name in payload['PROCESS_KEYCOLUMNS']:
                process_keycolumn_payload = {
                    'OUT_PROCESS_NAME'         : payload['OUT_PROCESS_NAME']
                    ,'COLUMN_NAME'             : column_name
                    ,'COLUMN_TYP_CD'           : 'KEY_COLUMN'
                    ,'CONSIDER_FOR_PROCESSING' : payload['CONSIDER_FOR_PROCESSING']
                }
                process_columns_payloads.append(process_keycolumn_payload)
            
            for column_name in payload['PROCESS_EXCL_COLUMNS']:
                process_exclcolumn_payload = {
                    'OUT_PROCESS_NAME'         : payload['OUT_PROCESS_NAME']
                    ,'COLUMN_NAME'             : column_name
                    ,'COLUMN_TYP_CD'           : 'EXCL_COLUMN'
                    ,'CONSIDER_FOR_PROCESSING' : payload['CONSIDER_FOR_PROCESSING']
                }
                process_columns_payloads.append(process_exclcolumn_payload)
        
        process_group_final_payloads = {
            'PR_METADATA_PROCESS_GROUP_LOAD': process_group_payloads
            ,'PR_METADATA_PROCESS_LOAD'     : process_payloads
            ,'PR_METADATA_KEYCOLUMNS_LOAD'  : process_columns_payloads
        }
            
        process_group_logs = execute_process_group_using_metadata('PG_METADATA_PROCESS_LOAD', payloads = process_group_final_payloads)
        return jsonify(process_group_logs)
    except Exception as e:
        return jsonify({'message': repr(e), 'status': "Failed"})