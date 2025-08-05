from flask import Blueprint, jsonify, request
import yfinance as yf
from dateutil import parser
from datetime import datetime, timedelta, date
import os
from json import loads
from uuid import NAMESPACE_URL, uuid5

from utils.sql_utils.process.execute_process_group import execute_process_group_using_metadata
from utils.sql_utils.process.duplicate_check import duplicate_check_on_managed_tables

from utils.sql_utils.tables.create_metadata_tables import\
create_metadata_store_table,\
create_processing_date_table,\
create_processing_type_table,\
create_metadata_process_group_table,\
create_metadata_process_table,\
create_metadata_key_columns_table,\
create_execution_logs_table,\
create_holiday_date_table,\
create_working_date_table,\
create_holiday_calendar_table,\
create_duplicate_logs_table

from utils.sql_utils.tables.create_metrics_tables import\
create_price_table,\
create_mutual_fund_returns_table,\
create_agg_mutual_fund_returns_table,\
create_fin_mutual_fund_returns_table,\
create_realised_intraday_stock_returns_table,\
create_agg_realised_intraday_stock_returns_table,\
create_fin_realised_intraday_stock_returns_table,\
create_realised_swing_stock_returns_table,\
create_agg_realised_swing_stock_returns_table,\
create_fin_realised_swing_stock_returns_table,\
create_unrealised_stock_returns_table,\
create_agg_unrealised_stock_returns_table,\
create_fin_unrealised_stock_returns_table,\
create_consolidated_returns_table,\
create_agg_consolidated_returns_table,\
create_fin_consolidated_returns_table,\
create_consolidated_allocation_table,\
create_agg_consolidated_allocation_table,\
create_fin_consolidated_allocation_table,\
create_simulated_portfolio_table,\
create_agg_simulated_portfolio_table,\
create_fin_simulated_portfolio_table

from utils.sql_utils.tables.create_user_data_tables import\
create_mf_order_table,\
create_trade_table,\
create_fee_table,\
create_close_trades_table

from utils.sql_utils.views.create_views import\
create_mf_portfolio_views_in_db,\
create_stock_portfolio_views_in_db,\
create_consolidated_portfolio_views_in_db,\
create_simulated_portfolio_views_in_db

from utils.sql_utils.query_db.get_or_process_in_db import\
get_price_from_price_table,\
get_proc_date_from_processing_date_table,\
get_max_value_date_for_alt_symbol,\
get_holiday_date_from_holiday_dates_table,\
get_working_date_from_working_dates_table,\
get_date_setup_from_holiday_calendar,\
get_mf_returns,\
get_all_symbols_list_from_metadata_store,\
get_realised_intraday_and_swing_stock_returns,\
get_open_trades_from_trades_table,\
get_unrealised_swing_stock_returns,\
get_consolidated_returns,\
get_all_from_consolidated_returns_table,\
get_consolidated_allocation,\
get_all_consolidated_allocation,\
get_simulated_returns_from_fin_simulated_returns_table,\
get_component_info_from_db,\
get_missing_prices_from_price_table

from utils.sql_utils.query_db.update_in_db import\
update_proc_date_in_processing_date_table

from utils.date_utils.date_utils import convert_weekday_from_int_to_char

# Folders

from utils.folder_utils.paths import upload_folder_path
from utils.folder_utils.paths import db_folder_path
from werkzeug.utils import secure_filename
from PyPDF2 import PdfReader

api = Blueprint('api', __name__)

app_namespace = uuid5(NAMESPACE_URL,"Consolidated_Portfolio_Tracker") # Change Later

@api.route('/api/price_table/max_value_date/', methods = ['GET'])
def get_max_value_date_from_price_table():
    try:
        process_flag              = request.args.get('process_flag') or None
        consider_for_returns      = request.args.get('consider_for_returns') or None
        portfolio_type            = request.args.get('portfolio_type') or None

        max_value_date_data = get_max_value_date_for_alt_symbol(process_flag, consider_for_returns, portfolio_type)
        return jsonify({'max_value_date_data': max_value_date_data, 'message': "Successfully retrieved Maximum Value Date data from PRICE_TABLE table", 'status': "Success"})
    except Exception as e:
        return jsonify({'message': repr(e), 'status': "Failed"})

@api.route('/api/price_table/close_price/<alt_symbol>/', methods = ['POST'])
def upsert_price_table_for_alt_symbol(alt_symbol):
    try:
        yahoo_symbol   = request.form.get('yahoo_symbol')
        portfolio_type = request.form.get('portfolio_type')
        start_date     = request.args.get('start_date') or None
        end_date       = request.args.get('end_date') or None
        on_start       = request.args.get('on_start') or None

        create_price_table()
        price_payloads = []

        ticker = yf.Ticker(yahoo_symbol)
        pandas_data = ticker.history(start = start_date, end = end_date)

        for index, value in pandas_data['Close'].items():
            value_date = str(index)[:10]
            if (parser.parse(value_date, fuzzy = 'fuzzy')):
                value_date = datetime.strptime(value_date,'%Y-%m-%d')
                value_date = value_date.strftime('%Y-%m-%d')

                holiday_calendar_data = get_date_setup_from_holiday_calendar(value_date)

                price_payload_from_yahoo_finance = {
                    'ALT_SYMBOL'               : alt_symbol
                    ,'PORTFOLIO_TYPE'          : portfolio_type
                    ,'VALUE_DATE'              : value_date
                    ,'VALUE_TIME'              : '15:30:00'
                    ,'PRICE'                   : round(value,4)
                    ,'PRICE_TYP_CD'            : 'CLOSE_PRICE'
                    ,'PROCESSING_DATE'         : holiday_calendar_data['PROCESSING_DATE']
                    ,'PREVIOUS_PROCESSING_DATE': holiday_calendar_data['PREVIOUS_PROCESSING_DATE']
                    ,'NEXT_PROCESSING_DATE'    : holiday_calendar_data['NEXT_PROCESSING_DATE']
                }
                price_payloads.append(price_payload_from_yahoo_finance)
            else:
                return jsonify({'message': f'Invalid Date from Yahoo Finance for {alt_symbol}', 'status': "Failed"})

        if on_start == "true":
            process_price_logs = execute_process_group_using_metadata('PRICE_DAILY_PROCESS_GROUP', start_date, end_date, price_payloads, "true")
            return jsonify(process_price_logs)
        elif start_date and end_date:
            process_price_logs = execute_process_group_using_metadata('PRICE_HIST_PROCESS_GROUP', start_date, end_date, price_payloads, "true")
            return jsonify(process_price_logs)
        else:
            return jsonify({'message': 'Start Date and End Date are required to Process Prices', 'status': 'Failed'})

    except Exception as e:
        return jsonify({'message': repr(e), 'status': 'Failed'})

@api.route('/api/metadata_store/', methods = ['POST'])
def metadata_entry():
    try:
        metadata_payload = loads(request.form.get('metadata_payload'))
        create_metadata_store_table()
        holiday_calendar_data                        = get_date_setup_from_holiday_calendar(date.today().strftime('%Y-%m-%d'))
        metadata_payload['PROCESSING_DATE']          = holiday_calendar_data['PROCESSING_DATE']
        metadata_payload['NEXT_PROCESSING_DATE']     = holiday_calendar_data['NEXT_PROCESSING_DATE']
        metadata_payload['PREVIOUS_PROCESSING_DATE'] = holiday_calendar_data['PREVIOUS_PROCESSING_DATE']

        metadata_entry_logs = execute_process_group_using_metadata('METADATA_STORE_ENTRY_PROCESS_GROUP', None, None, metadata_payload, "true")
        return jsonify(metadata_entry_logs)
    except Exception as e:
        return jsonify({'message': repr(e), 'status': "Failed"})

@api.route('/api/metadata_store/symbols/', methods = ['GET'])
def get_all_symbols_list():
    try:
        portfolio_type = request.args.get('portfolio_type') or None
        all_symbols_data = get_all_symbols_list_from_metadata_store(portfolio_type)
        return jsonify({'all_symbols_list': all_symbols_data, 'message': "Successfully retrieved All Symbols List from METADATA_STORE Table", 'status': "Success"})
    except Exception as e:
        return jsonify({'message': repr(e), 'status': "Failed"})

@api.route('/api/price_table/close_price/', methods = ['GET'])
def price_table_lookup():
    try:
        alt_symbol    = request.args.get('alt_symbol') or None
        purchase_date = request.args.get('purchase_date') or None
        price_data = get_price_from_price_table(alt_symbol, purchase_date)
        return jsonify({'price_data': price_data, 'message': "Successfully retrieved Price data from PRICE_TABLE", 'status': "Success"})
    except Exception as e:
        return jsonify({'message': repr(e), 'status': "Failed"})

@api.route('/api/mf_order/', methods = ['POST'])
def mf_order():
    try:
        mf_order_payload = loads(request.form.get('mf_order_payload'))
        mf_order_payload['STAMP_FEES_AMOUNT'] = round(float(mf_order_payload['INVESTED_AMOUNT']) - float(mf_order_payload['AMC_AMOUNT']),2)
        create_mf_order_table()
        holiday_calendar_data                        = get_date_setup_from_holiday_calendar(mf_order_payload['PURCHASED_ON'])
        mf_order_payload['PROCESSING_DATE']          = holiday_calendar_data['PROCESSING_DATE']
        mf_order_payload['NEXT_PROCESSING_DATE']     = holiday_calendar_data['NEXT_PROCESSING_DATE']
        mf_order_payload['PREVIOUS_PROCESSING_DATE'] = holiday_calendar_data['PREVIOUS_PROCESSING_DATE']

        mf_order_entry_logs = execute_process_group_using_metadata('MF_ORDER_ENTRY_PROCESS_GROUP', mf_order_payload['PURCHASED_ON'], None, mf_order_payload, "true")
        return jsonify(mf_order_entry_logs)
    except Exception as e:
        return jsonify({'message': repr(e), 'status': "Failed"})

@api.route('/api/processing_date/', methods = ['GET'])
def proc_date_lookup():
    try:
        proc_date_data = get_proc_date_from_processing_date_table()
        return jsonify({'proc_date_data': proc_date_data,'message': "Successfully retrieved data from PROCESSING_DATE Table", 'status': "Success"})
    except Exception as e:
        return jsonify({'message': repr(e), 'status': 'Failed'})

@api.route('/api/processing_date/', methods = ['POST'])
def proc_date_update():
    try:
        create_processing_date_table()

        processing_date_payload = loads(request.form.get('processing_date_payload'))
        update_proc_date_in_processing_date_table('MF_PROC',         processing_date_payload['mf_proc_date'],        processing_date_payload['mf_next_proc_date'],        processing_date_payload['mf_prev_proc_date'])
        update_proc_date_in_processing_date_table('PPF_MF_PROC',     processing_date_payload['ppf_mf_proc_date'],    processing_date_payload['ppf_mf_next_proc_date'],    processing_date_payload['ppf_mf_prev_proc_date'])
        update_proc_date_in_processing_date_table('STOCK_PROC',      processing_date_payload['stock_proc_date'],     processing_date_payload['stock_next_proc_date'],     processing_date_payload['stock_prev_proc_date'])
        update_proc_date_in_processing_date_table('SIM_MF_PROC',     processing_date_payload['sim_mf_proc_date'],    processing_date_payload['sim_mf_next_proc_date'],    processing_date_payload['sim_mf_prev_proc_date'])
        update_proc_date_in_processing_date_table('SIM_STOCK_PROC',  processing_date_payload['sim_stock_proc_date'], processing_date_payload['sim_stock_next_proc_date'], processing_date_payload['sim_stock_prev_proc_date'])
        return jsonify({'message': "Successfully updated Processing Dates Table", 'status': "Success"})
    except Exception as e:
        return jsonify({'message': repr(e), 'status': 'Failed'})

@api.route('/api/duplicate_check/', methods = ['GET'])
def duplicate_check_managed_table():
    try:
        create_duplicate_logs_table()
        dup_check_response = duplicate_check_on_managed_tables()
        return jsonify(dup_check_response)
    except Exception as e:
        return jsonify({'message': repr(e), 'status': 'Failed'})

@api.route('/api/create_managed_views/', methods = ['GET'])
def create_managed_views():
    try:
        create_mf_portfolio_views_in_db()
        create_stock_portfolio_views_in_db()
        create_consolidated_portfolio_views_in_db()
        create_simulated_portfolio_views_in_db()
        return jsonify({'message': 'Successfully replaced Portfolio Views in DB','status': 'Success'})
    except Exception as e:
        return jsonify({'message': repr(e), 'status': 'Failed'})

@api.route('/api/create_managed_folders/', methods = ['GET'])
def create_managed_folders():
    try:
        # Create the Upload Directory if it doesn't exist
        os.makedirs(upload_folder_path, exist_ok = True)
        os.makedirs(db_folder_path, exist_ok = True)
        return jsonify({'message': 'Successfully created Managed Folders in Directory','status': 'Success'})
    except Exception as e:
        return jsonify({'message': repr(e), 'status': 'Failed'}) 

@api.route('/api/create_managed_tables/', methods = ['GET'])
def create_managed_tables():
    try:
        create_metadata_store_table()
        create_price_table()
        create_processing_date_table()
        create_processing_type_table()
        create_mf_order_table()
        create_holiday_date_table()
        create_working_date_table()
        create_holiday_calendar_table()
        create_trade_table()
        create_fee_table()
        create_close_trades_table()
        create_metadata_key_columns_table()
        create_simulated_portfolio_table()
        create_agg_simulated_portfolio_table()
        create_fin_simulated_portfolio_table()
        create_metadata_process_table()
        create_execution_logs_table()
        create_metadata_process_group_table()
        create_mutual_fund_returns_table()
        create_agg_mutual_fund_returns_table()
        create_fin_mutual_fund_returns_table()
        create_unrealised_stock_returns_table()
        create_agg_unrealised_stock_returns_table()
        create_fin_unrealised_stock_returns_table()
        create_realised_intraday_stock_returns_table()
        create_agg_realised_intraday_stock_returns_table()
        create_fin_realised_intraday_stock_returns_table()
        create_realised_swing_stock_returns_table()
        create_agg_realised_swing_stock_returns_table()
        create_fin_realised_swing_stock_returns_table()
        create_consolidated_returns_table()
        create_agg_consolidated_returns_table()
        create_fin_consolidated_returns_table()
        create_consolidated_allocation_table()
        create_agg_consolidated_allocation_table()
        create_fin_consolidated_allocation_table()
        create_duplicate_logs_table()
        return jsonify({'message': 'Successfully created Managed Tables in DB','status': 'Success'})
    except Exception as e:
        return jsonify({'message': repr(e), 'status': 'Failed'})

@api.route('/api/holiday_date/', methods = ['POST'])
def holiday_date_entry():
    try:
        create_holiday_date_table()
        holiday_date_payload = loads(request.form.get('holiday_date_payload'))
        holiday_calendar_data                            = get_date_setup_from_holiday_calendar(date.today().strftime('%Y-%m-%d'))
        holiday_date_payload['PROCESSING_DATE']          = holiday_calendar_data['PROCESSING_DATE']
        holiday_date_payload['NEXT_PROCESSING_DATE']     = holiday_calendar_data['NEXT_PROCESSING_DATE']
        holiday_date_payload['PREVIOUS_PROCESSING_DATE'] = holiday_calendar_data['PREVIOUS_PROCESSING_DATE']
        holiday_entry_logs = execute_process_group_using_metadata('HOLIDAY_DATES_ENTRY_PROCESS_GROUP', None, None, holiday_date_payload, "true")
        return jsonify(holiday_entry_logs)
    except Exception as e:
        return jsonify({'message': repr(e), 'status': 'Failed'})

@api.route('/api/holiday_date/', methods = ['GET'])
def holiday_date_lookup():
    try:
        current_year = request.args.get('current_year') or None
        data = get_holiday_date_from_holiday_dates_table(current_year)
        return jsonify({'data': data,'message': 'Successfully retrieved from HOLIDAY_DATES Table','status': 'Success'})
    except Exception as e:
        return jsonify({'message': repr(e), 'status': 'Failed'})

@api.route('/api/working_date/', methods = ['POST'])
def working_date_entry():
    try:
        create_working_date_table()
        working_date_payload = loads(request.form.get('working_date_payload'))
        holiday_calendar_data                            = get_date_setup_from_holiday_calendar(date.today().strftime('%Y-%m-%d'))
        working_date_payload['PROCESSING_DATE']          = holiday_calendar_data['PROCESSING_DATE']
        working_date_payload['NEXT_PROCESSING_DATE']     = holiday_calendar_data['NEXT_PROCESSING_DATE']
        working_date_payload['PREVIOUS_PROCESSING_DATE'] = holiday_calendar_data['PREVIOUS_PROCESSING_DATE']
        working_day_entry_logs = execute_process_group_using_metadata('WORKING_DATES_ENTRY_PROCESS_GROUP', None, None, working_date_payload, "true")
        return jsonify(working_day_entry_logs)
    except Exception as e:
        return jsonify({'message': repr(e), 'status': 'Failed'})

@api.route('/api/working_date/', methods = ['GET'])
def working_date_lookup():
    try:
        data = get_working_date_from_working_dates_table()
        if data[0]['WORKING_DATE']:
            return jsonify({'data': data,'message': 'Successfully retrieved from WORKING_DATES Table','status': 'Success'})
        else:
            return jsonify({'data': [],'message': 'No records present in WORKING_DATES Table','status': 'Success'})
    except Exception as e:
        return jsonify({'message': repr(e), 'status': 'Failed'})

@api.route('/api/holiday_calendar_setup/', methods = ['POST'])
def holiday_calendar_setup():
    try:
        start_date       = request.args.get('start_date')
        end_date         = request.args.get('end_date')
        holiday_data     = request.form.get('holiday_data')
        working_day_data = request.form.get('working_day_data')
        
        holiday_dates = loads(holiday_data)
        working_dates = loads(working_day_data)

        create_holiday_calendar_table()
        holiday_payloads = []

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

        if start_date and end_date:
            process_price_logs = execute_process_group_using_metadata('HOLIDAY_CALENDAR_HIST_PROCESS_GROUP', start_date, end_date, holiday_payloads, "true")
            return jsonify(process_price_logs)
        else:
            return jsonify({'message': 'Start Date and End Date is required for Holiday Calendar Setup', 'status': 'Failed'})
    except Exception as e:
        return jsonify({'message': repr(e), 'status': 'Failed'})

@api.route('/api/process_mf_returns/', methods = ['GET'])
def process_mf_returns():
    try:
        start_date = request.args.get('start_date') or None
        end_date   = request.args.get('end_date') or None
        on_start   = request.args.get('on_start') or None

        create_mutual_fund_returns_table()
        create_agg_mutual_fund_returns_table()
        create_fin_mutual_fund_returns_table()
        
        if on_start == "true" or (start_date):
            mf_returns_process_group_logs = execute_process_group_using_metadata('MF_RETURNS_DAILY_PROCESS_GROUP', start_date, end_date)
        elif not start_date and not end_date:
            mf_returns_process_group_logs = execute_process_group_using_metadata('MF_RETURNS_HIST_PROCESS_GROUP')
        return jsonify(mf_returns_process_group_logs)
    except Exception as e:
        return jsonify({'message': repr(e), 'status': 'Failed'})

@api.route('/api/mf_returns/', methods = ['GET'])
def mf_returns_lookup():
    try:
        data = get_mf_returns()
        return jsonify({'data': data, 'message': 'Successfully retrieved Mutual Fund Returns','status': 'Success'})
    except Exception as e:
        return jsonify({'message': repr(e), 'status': 'Failed'})

@api.route('/api/stock_pdf/', methods = ['POST'])
def stock_order_entry_from_pdf():
    try:
        # Individual Trade Info
        trade_info = {}
        trade_list = []
        fees_info = {}

        # Remarks line number - The Next lines are usually trades info
        remarks_line_number = 0

        # Trade Agnostic Info
        trade_entry_date = ""
        net_obligation = ""
        brokerage = ""
        exc_trans_charges= ""
        igst = ""
        sec_trans_tax = ""
        sebi_turn_fees = ""

        # Derived Info
        derived_trade_info = {}


        if 'stock_pdf_file' not in request.files:
            return jsonify({'message': 'File was not Uploaded Successfully', 'status': 'Failed'})
        stock_pdf_file = request.files['stock_pdf_file']
        if stock_pdf_file.filename == "":
            return jsonify({'message': 'No File was Selected', 'status': 'Failed'})
        if stock_pdf_file and stock_pdf_file.filename.lower().endswith('.pdf'):
            stock_pdf_file_name = stock_pdf_file.filename.lower().replace(" ", "_")
            safe_stock_pdf_file_name = secure_filename(stock_pdf_file_name)

            stock_pdf_file_name_path = os.path.join(upload_folder_path, safe_stock_pdf_file_name)
            
            stock_pdf_file.save(stock_pdf_file_name_path)
        else:
            return jsonify({'message': 'Only PDF File Format is Accepted', 'status': 'Failed'})

        # Parse the uploaded PDF file from Upload Folder
        file_password = request.form.get('file_password')
        pdf_reader = PdfReader(stock_pdf_file_name_path)

        if pdf_reader.is_encrypted:
            pdf_reader.decrypt(file_password)

        text = ""

        for page in pdf_reader.pages: # Cleanse and Add to Variable 'Text'
            text += page.extract_text().replace("\ufb03","").replace("\ufb00","").replace("\u2074","").replace("\ufb01","").replace("\u20b9","").replace("�","").replace("^M","").replace("\r","").replace("�","")

        stock_text_file_name_path = stock_pdf_file_name_path.replace(".pdf", ".txt")

        with open(stock_text_file_name_path, "w+", encoding="utf-8") as text_file:
            text_file.write(text)

        with open(stock_text_file_name_path, "r") as read_text_file:
            for line_number, line in enumerate(read_text_file, start = 1):
                ## Trade Agnostic
                if "T rade Date" in line:  ### Check with time
                    semi_colon_index = line.rfind(":")
                    trade_entry_date = datetime.strptime(line[semi_colon_index + 1:].strip(), '%d/%m/%Y')
                    trade_entry_date = trade_entry_date.strftime('%Y-%m-%d')

                if "Pay" in line:  ### Check with time
                    space_index = line.rfind(" ")
                    net_obligation = line[space_index:]
                    net_obligation = net_obligation.strip(" ")
                    if "(" in net_obligation or ")" in net_obligation:
                        net_obligation = float(net_obligation.strip("(").replace(")","")) * -1
                    fees_info['net_obligation'] = net_obligation
                    
                if "T axable" in line:  ### Check with time
                    space_index = line.rfind(" ")
                    brokerage = line[space_index:]
                    brokerage = float(brokerage.strip(" ").strip("(").replace(")",""))
                    fees_info['brokerage'] = brokerage

                if "Exchange transaction charges" in line:  ### Check with time
                    space_index = line.rfind(" ")
                    exc_trans_charges = line[space_index:]
                    exc_trans_charges = float(exc_trans_charges.strip(" ").strip("(").replace(")",""))
                    fees_info['exc_trans_charges'] = exc_trans_charges

                if line.startswith("IGST"):  ### Check with time
                    space_index = line.rfind(" ")
                    igst = line[space_index:]
                    igst = float(igst.strip(" ").strip("(").replace(")",""))
                    fees_info['igst'] = igst

                if "transaction tax" in line:  ### Check with time
                    space_index = line.rfind(" ")
                    sec_trans_tax = line[space_index:]
                    if sec_trans_tax.strip(" ").strip("\n") == "tax":
                        sec_trans_tax = 0
                    else:
                        sec_trans_tax = float(sec_trans_tax.strip(" ").strip("(").replace(")",""))
                    fees_info['sec_trans_tax'] = sec_trans_tax

                if "SEBI turno" in line:  ### Check with time
                    space_index = line.rfind(" ")
                    sebi_turn_fees = line[space_index:]
                    if sebi_turn_fees.strip(" ").strip("\n") == "fe es":
                        sebi_turn_fees = 0
                    else:
                        sebi_turn_fees = float(sebi_turn_fees.strip(" ").strip("(").replace(")",""))
                    fees_info['sebi_turn_fees'] = sebi_turn_fees
                
                if "Remarks" in line: ### Check with Time
                    remarks_line_number = line_number

        with open(stock_text_file_name_path, "r") as read_text_file_for_trades:
            for line_number, line in enumerate(read_text_file_for_trades, start = 1):
                # Specific to each Trade
                if line_number > remarks_line_number: ### Check with Time
                    line_split                                          = line.split(" ")
                    order_number                                        = line_split[0]
                    trade_info[order_number]                            = {}
                    individual_trade_info                               = trade_info[order_number]
                    individual_trade_info['order_number']               = order_number                                       # 1000000005038948
                    individual_trade_info['order_time']                 = line_split[1]                                      # 09:28:11
                    individual_trade_info['trade_number']               = line_split[2]                                      # 990607
                    individual_trade_info['trade_time']                 = line_split[3]                                      # 09:28:11

                    remaining_line_split                                = line_split[4:]                                     # AD ANIEN T -EQ/INE423A01024 S NSE 1 2202.20 2202.2 2202.20
                    remaining_line_join                                 = ' '.join(remaining_line_split)                     # AD ANIEN T -EQ/INE423A01024 S NSE 1 2202.20 2202.2 2202.20
                    hyphen_index                                        = remaining_line_join.find("-")                      # 11
                    individual_trade_info['stock_symbol']               = remaining_line_join[:hyphen_index].replace(" ","") # ADANIENT

                    stock_type_isin_exchange_metrics                    = remaining_line_join[hyphen_index+1:]               # EQ/INE423A01024 S NSE 1 2202.20 2202.2 2202.20 # +1 to ignore Hyphen
                    stock_type_isin_split                               = stock_type_isin_exchange_metrics.split("/")        # EQ | INE423A01024 S NSE 1 2202.20 2202.2 2202.20
                    individual_trade_info['asset_type']                 = stock_type_isin_split[0]                           # EQ
                    stock_isin_exchange_metrics                         = stock_type_isin_split[1]                           # INE0LX G01040 B NSE 100 1.4889 49.63 (4963.00)
                    if " B " in stock_isin_exchange_metrics:
                        buy_or_sell_index                               = stock_isin_exchange_metrics.find(" B ")            # 13
                    if " S " in stock_isin_exchange_metrics:
                        buy_or_sell_index                               = stock_isin_exchange_metrics.find(" S ")            # 13
                    stock_isin_string                                   = stock_isin_exchange_metrics[:buy_or_sell_index]    # INE0LX G01040
                    individual_trade_info['stock_isin']                 = stock_isin_string.replace(" ","")                  # INE0LXG01040
                    stock_exchange_metrics                              = stock_isin_exchange_metrics[buy_or_sell_index:]    #  B NSE 100 1.4889 49.63 (4963.00)
                    stock_exchange_metrics_split                        = stock_exchange_metrics.split(" ")                  # B NSE 100 1.4889 49.63 (4963.00)
                    individual_trade_info['buy_or_sell']                = stock_exchange_metrics_split[1]                    # S
                    individual_trade_info['stock_exchange']             = stock_exchange_metrics_split[2]                    # NSE
                    individual_trade_info['stock_quantity']             = int(stock_exchange_metrics_split[3])               # 1

                    # Due to Contract Note Format Changes as of 01-Apr-2025
                    if trade_entry_date < '2025-04-01':
                        individual_trade_info['gross_trade_price_per_unit'] = float(stock_exchange_metrics_split[4])         # 2202.20
                        individual_trade_info['brokerage_per_trade']        = "Not Present"                                  # Not Present
                    elif trade_entry_date >= '2025-04-01':
                        individual_trade_info['brokerage_per_trade']        = float(stock_exchange_metrics_split[4])         # 1.4889
                        individual_trade_info['gross_trade_price_per_unit'] = "Not Present"                                  # Not Present
                    individual_trade_info['net_trade_price_per_unit']       = float(stock_exchange_metrics_split[5])         # 2202.2
                    net_total_before_levies                                 = stock_exchange_metrics_split[6]                # 2202.20

                    if "(" in net_total_before_levies:                                                                       # (1249.40)
                        net_total_before_levies = net_total_before_levies.replace(" ","").replace("(","").replace(")","")
                        
                    individual_trade_info['net_total_before_levies']    = float(net_total_before_levies)

                    trade_list.append(individual_trade_info)
        # Derived Trade Info
        for info in trade_list:
            derived_trade_info[info['stock_symbol']] = {}
        # Mark trades with trade set
        for stock_name in derived_trade_info:
            derived_trade_info[stock_name]['trade_entry_time'] = '15:30:00'
            derived_trade_info[stock_name]['trade_exit_time']  = '09:15:00'
            derived_trade_info[stock_name]['final_stock_quantity']   = 0
            trade_set = 1
            for info in trade_list:
                if info['stock_symbol'] == stock_name:
                    if datetime.strptime(info['trade_time'],'%H:%M:%S') < datetime.strptime(derived_trade_info[stock_name]['trade_entry_time'], '%H:%M:%S'):
                        derived_trade_info[stock_name]['trade_entry_time'] = info['trade_time']
                    if datetime.strptime(info['trade_time'],'%H:%M:%S') > datetime.strptime(derived_trade_info[stock_name]['trade_exit_time'], '%H:%M:%S'):
                        derived_trade_info[stock_name]['trade_exit_time'] = info['trade_time']
                    if info['buy_or_sell'] == 'B':
                        derived_trade_info[stock_name]['final_stock_quantity'] += info['stock_quantity']
                        info['trade_set'] = trade_set
                        if derived_trade_info[stock_name]['final_stock_quantity'] == 0:
                            trade_set += 1
                    if info['buy_or_sell'] == 'S':
                        derived_trade_info[stock_name]['final_stock_quantity'] -= info['stock_quantity']
                        info['trade_set'] = trade_set
                        if derived_trade_info[stock_name]['final_stock_quantity'] == 0:
                            trade_set += 1

            # To Determine Long or Short Position
            trade_position = ""
            # Get Maximum sets of trades
            max_trade_set_info = 1
            for info in trade_list:
                if info['stock_symbol'] == stock_name:
                    trade_set_info = info['trade_set']
                    if trade_set_info > max_trade_set_info:
                        max_trade_set_info = trade_set_info
            
            for trade_set_var in range(1, max_trade_set_info + 1): # +1 To Circumvent range function restriction
                trade_position_entry_time = '15:30:00'
                trade_position_exit_time  = '09:15:00'
                # Get Trade Entry Time and Trade Exit Time within a trade set
                for info in trade_list:
                    if info['stock_symbol'] == stock_name:
                        if info['trade_set'] == trade_set_var:
                            if datetime.strptime(info['trade_time'],'%H:%M:%S') < datetime.strptime(trade_position_entry_time, '%H:%M:%S'):
                                trade_position_entry_time = info['trade_time']
                            if datetime.strptime(info['trade_time'],'%H:%M:%S') > datetime.strptime(trade_position_exit_time, '%H:%M:%S'):
                                trade_position_exit_time = info['trade_time']
                # Based on the Trade Entry time determine the Trade Position
                for info in trade_list:
                    if info['stock_symbol'] == stock_name:
                        if info['trade_time'] == trade_position_entry_time:
                            if info['buy_or_sell'] == 'B':
                                trade_position = 'Long'
                            elif info['buy_or_sell'] == 'S':
                                trade_position = 'Short'
                # Update Trade Position to all trades under the trade set
                for info in trade_list:
                    if info['stock_symbol'] == stock_name:
                        if info['trade_set'] == trade_set_var:
                            info['trade_position'] = trade_position

            if derived_trade_info[stock_name]['final_stock_quantity'] == 0: # Buy and Sell are squared off
                derived_trade_info[stock_name]['trade_type'] = "Intraday Trading"
                derived_trade_info[stock_name]['trade_exit_date'] = trade_entry_date
                derived_trade_info[stock_name]['leverage'] = 5 # Default Leverage for Intraday Trading
            else:
                derived_trade_info[stock_name]['trade_type'] = "Swing Trading"
                derived_trade_info[stock_name]['trade_exit_date'] = None
                derived_trade_info[stock_name]['trade_exit_time'] = None
                derived_trade_info[stock_name]['leverage'] = 1
        # Append the Derived info into Trade List
        for stock_name in derived_trade_info:
            for info in trade_list:
                if info['stock_symbol'] == stock_name:
                    info['trade_entry_date'] = trade_entry_date
                    info['trade_entry_time'] = derived_trade_info[stock_name]['trade_entry_time']
                    info['trade_exit_date']  = derived_trade_info[stock_name]['trade_exit_date']
                    info['trade_exit_time'] = derived_trade_info[stock_name]['trade_exit_time']
                    info['final_stock_quantity'] = derived_trade_info[stock_name]['final_stock_quantity']
                    info['trade_type'] = derived_trade_info[stock_name]['trade_type']
                    info['leverage'] = derived_trade_info[stock_name]['leverage']

        os.remove(stock_pdf_file_name_path)
        os.remove(stock_text_file_name_path)
        return jsonify({'data': trade_list, 'fees': fees_info, 'message': 'Successfully uploaded the Stock PDF File and Parsed the File.','status': 'Success'})
    except Exception as e:
        return jsonify({'data': None, 'message': repr(e), 'status': 'Failed'})
    
@api.route('/api/stock_pdf/', methods = ['PUT'])
def upsert_trade_entry():
    try:
        trades_array = loads(request.form.get('trade_data')) # json.loads()
        fee_data = loads(request.form.get('fee_data'))
        unique_fee_id = ""
        trade_date = ""

        trades_payloads = []
        fee_payloads    = []

        create_trade_table()
        create_fee_table()
        holiday_calendar_data = get_date_setup_from_holiday_calendar(date.today().strftime('%Y-%m-%d'))

        for trade in trades_array:
            unique_trade_id     = uuid5(app_namespace, str(trade['trade_number'])     + str(trade['trade_entry_date']))
            unique_fee_id       = uuid5(app_namespace, str(trade['trade_entry_date']) + str(trade['trade_type']))
            unique_trade_set_id = uuid5(app_namespace, str(trade['trade_entry_date']) + str(trade['stock_symbol']) + str(trade['trade_set']))
            trade_payload = {
                'TRADE_ID'                  : str(unique_trade_id)
                ,'FEE_ID'                   : str(unique_fee_id)
                ,'TRADE_SET_ID'             : str(unique_trade_set_id)
                ,'STOCK_NAME'               : trade['stock_symbol']
                ,'STOCK_ISIN'               : trade['stock_isin']
                ,'TRADE_DATE'               : trade['trade_entry_date']
                ,'ORDER_NUMBER'             : trade['order_number']
                ,'ORDER_TIME'               : trade['order_time']
                ,'TRADE_NUMBER'             : trade['trade_number']
                ,'TRADE_TIME'               : trade['trade_time']
                ,'BUY_OR_SELL'              : trade['buy_or_sell']
                ,'STOCK_QUANTITY'           : trade['stock_quantity']
                ,'BROKERAGE_PER_TRADE'      : trade['brokerage_per_trade']
                ,'NET_TRADE_PRICE_PER_UNIT' : trade['net_trade_price_per_unit']
                ,'NET_TOTAL_BEFORE_LEVIES'  : trade['net_total_before_levies']
                ,'TRADE_SET'                : trade['trade_set']
                ,'TRADE_POSITION'           : trade['trade_position']
                ,'TRADE_ENTRY_DATE'         : trade['trade_entry_date']
                ,'TRADE_ENTRY_TIME'         : trade['trade_entry_time']
                ,'TRADE_EXIT_DATE'          : trade['trade_exit_date']
                ,'TRADE_EXIT_TIME'          : trade['trade_exit_time']
                ,'TRADE_TYPE'               : trade['trade_type']
                ,'LEVERAGE'                 : trade['leverage']
                ,'PROCESSING_DATE'          : holiday_calendar_data['PROCESSING_DATE']
                ,'NEXT_PROCESSING_DATE'     : holiday_calendar_data['NEXT_PROCESSING_DATE']
                ,'PREVIOUS_PROCESSING_DATE' : holiday_calendar_data['PREVIOUS_PROCESSING_DATE']
            }
            trades_payloads.append(trade_payload)
        trade_entry_logs = execute_process_group_using_metadata('TRADE_ENTRY_PROCESS_GROUP', None, None, trades_payloads, "true")

        for trade_type in fee_data['trade_types']:
            if trade_type == "Intraday Trading":
                unique_fee_id = uuid5(app_namespace, str(trade['trade_entry_date']) + "Intraday Trading")
                fee_payload = {
                    'FEE_ID'                        : str(unique_fee_id)
                    ,'TRADE_DATE'                   : trade['trade_entry_date']
                    ,'NET_OBLIGATION'               : fee_data['intraday_net_obligation']
                    ,'BROKERAGE'                    : fee_data['intraday_brokerage']
                    ,'EXCHANGE_TRANSACTION_CHARGES' : fee_data['intraday_exc_trans_charges']
                    ,'IGST'                         : fee_data['intraday_igst']
                    ,'SECURITIES_TRANSACTION_TAX'   : fee_data['intraday_sec_trans_tax']
                    ,'SEBI_TURN_OVER_FEES'          : fee_data['intraday_sebi_turn_fees']
                    ,'AUTO_SQUARE_OFF_CHARGES'      : fee_data['intraday_auto_square_off_charges']
                    ,'DEPOSITORY_CHARGES'           : fee_data['intraday_depository_charges']
                    ,'PROCESSING_DATE'              : holiday_calendar_data['PROCESSING_DATE']
                    ,'NEXT_PROCESSING_DATE'         : holiday_calendar_data['NEXT_PROCESSING_DATE']
                    ,'PREVIOUS_PROCESSING_DATE'     : holiday_calendar_data['PREVIOUS_PROCESSING_DATE']
                }
                fee_payloads.append(fee_payload)
            elif trade_type == "Swing Trading":
                unique_fee_id = uuid5(app_namespace, str(trade['trade_entry_date']) + "Swing Trading")
                fee_payload = {
                    'FEE_ID'                        : str(unique_fee_id)
                    ,'TRADE_DATE'                   : trade['trade_entry_date']
                    ,'NET_OBLIGATION'               : fee_data['swing_net_obligation']
                    ,'BROKERAGE'                    : fee_data['swing_brokerage']
                    ,'EXCHANGE_TRANSACTION_CHARGES' : fee_data['swing_exc_trans_charges']
                    ,'IGST'                         : fee_data['swing_igst']
                    ,'SECURITIES_TRANSACTION_TAX'   : fee_data['swing_sec_trans_tax']
                    ,'SEBI_TURN_OVER_FEES'          : fee_data['swing_sebi_turn_fees']
                    ,'AUTO_SQUARE_OFF_CHARGES'      : fee_data['swing_auto_square_off_charges']
                    ,'DEPOSITORY_CHARGES'           : fee_data['swing_depository_charges']
                    ,'PROCESSING_DATE'              : holiday_calendar_data['PROCESSING_DATE']
                    ,'NEXT_PROCESSING_DATE'         : holiday_calendar_data['NEXT_PROCESSING_DATE']
                    ,'PREVIOUS_PROCESSING_DATE'     : holiday_calendar_data['PREVIOUS_PROCESSING_DATE']
                }
                fee_payloads.append(fee_payload)
        fee_entry_logs = execute_process_group_using_metadata('FEE_ENTRY_PROCESS_GROUP', None, None, fee_payloads, "true")
        combined_status = 'Success' if trade_entry_logs['status'] == 'Success' and fee_entry_logs['status'] == 'Success' else 'Failed'
        return jsonify({'message': f"{trade_entry_logs['message']} and {fee_entry_logs['message']}",'status': combined_status})
    except Exception as e:
        return jsonify({'message': repr(e), 'status': 'Failed'})

@api.route('/api/realised_intraday_and_swing_stock_returns/', methods = ['GET'])
def realised_stock_returns_lookup():
    try:
        data = get_realised_intraday_and_swing_stock_returns()
        return jsonify({'data': data,'message': 'Successfully retrieved from Realised Returns','status': 'Success'})
    except Exception as e:
        return jsonify({'message': repr(e), 'status': 'Failed'})

@api.route('/api/process_realised_intraday_stock_returns/', methods = ['GET'])
def process_realised_intraday_stock_returns():
    try:
        start_date = request.args.get('start_date') or None
        end_date   = request.args.get('end_date') or None
        on_start   = request.args.get('on_start') or None

        create_realised_intraday_stock_returns_table()
        create_agg_realised_intraday_stock_returns_table()
        create_fin_realised_intraday_stock_returns_table()
        
        if on_start == "true" or (start_date):
            realised_intraday_stock_returns_process_group_logs = execute_process_group_using_metadata('REALISED_INTRADAY_STOCK_RETURNS_DAILY_PROCESS_GROUP', start_date, end_date)
        elif not start_date and not end_date:
            realised_intraday_stock_returns_process_group_logs = execute_process_group_using_metadata('REALISED_INTRADAY_STOCK_RETURNS_HIST_PROCESS_GROUP')
        return jsonify(realised_intraday_stock_returns_process_group_logs)
    except Exception as e:
        return jsonify({'message': repr(e), 'status': 'Failed'})

@api.route('/api/trades/open/', methods = ['GET'])
def get_open_trades_list():
    try:
        open_trades_list = get_open_trades_from_trades_table()
        return jsonify({'data': open_trades_list,'message': 'Successfully retrieved Open Trades from TRADES Table','status': 'Success'})
    except Exception as e:
        return jsonify({'message': repr(e), 'status': 'Failed'})

@api.route('/api/close_trades/', methods = ['POST'])
def close_trade_entry():
    try:
        close_trades_payloads = loads(request.form.get('close_trades_payloads'))
        create_close_trades_table()

        min_close_trade_date = datetime.today()

        for close_trade_payload in close_trades_payloads:
            holiday_calendar_data                           = get_date_setup_from_holiday_calendar(close_trade_payload['CLOSING_TRADE_DATE'])
            close_trade_payload['PROCESSING_DATE']          = holiday_calendar_data['PROCESSING_DATE']
            close_trade_payload['NEXT_PROCESSING_DATE']     = holiday_calendar_data['NEXT_PROCESSING_DATE']
            close_trade_payload['PREVIOUS_PROCESSING_DATE'] = holiday_calendar_data['PREVIOUS_PROCESSING_DATE']

            if datetime.strptime(close_trade_payload['CLOSING_TRADE_DATE'], '%Y-%m-%d') < min_close_trade_date:
                min_close_trade_date = datetime.strptime(close_trade_payload['CLOSING_TRADE_DATE'], '%Y-%m-%d')

        close_trades_logs = execute_process_group_using_metadata('CLOSE_TRADES_ENTRY_PROCESS_GROUP', min_close_trade_date.strftime('%Y-%m-%d'), None, close_trades_payloads, "true")
        return jsonify(close_trades_logs)
    except Exception as e:
        return jsonify({'message': repr(e), 'status': "Failed"})

@api.route('/api/process_realised_swing_stock_returns/', methods = ['GET'])
def process_realised_swing_stock_returns():
    try:
        start_date = request.args.get('start_date') or None
        end_date   = request.args.get('end_date') or None
        on_start   = request.args.get('on_start') or None

        create_realised_swing_stock_returns_table()
        create_agg_realised_swing_stock_returns_table()
        create_fin_realised_swing_stock_returns_table()
        
        if on_start == "true" or (start_date):
            realised_swing_stock_returns_process_group_logs = execute_process_group_using_metadata('REALISED_SWING_STOCK_RETURNS_DAILY_PROCESS_GROUP', start_date, end_date)
        elif not start_date and not end_date:
            realised_swing_stock_returns_process_group_logs = execute_process_group_using_metadata('REALISED_SWING_STOCK_RETURNS_HIST_PROCESS_GROUP')
        return jsonify(realised_swing_stock_returns_process_group_logs)
    except Exception as e:
        return jsonify({'message': repr(e), 'status': 'Failed'})

@api.route('/api/process_unrealised_stock_returns/', methods = ['GET'])
def process_unrealised_swing_stock_returns():
    try:
        start_date = request.args.get('start_date') or None
        end_date   = request.args.get('end_date') or None
        on_start   = request.args.get('on_start') or None

        create_unrealised_stock_returns_table()
        create_agg_unrealised_stock_returns_table()
        create_fin_unrealised_stock_returns_table()
        
        if on_start == "true" or (start_date):
            unrealised_stock_returns_process_group_logs = execute_process_group_using_metadata('UNREALISED_STOCK_RETURNS_DAILY_PROCESS_GROUP', start_date, end_date)
        elif not start_date and not end_date:
            unrealised_stock_returns_process_group_logs = execute_process_group_using_metadata('UNREALISED_STOCK_RETURNS_HIST_PROCESS_GROUP')
        return jsonify(unrealised_stock_returns_process_group_logs)
    except Exception as e:
        return jsonify({'message': repr(e), 'status': 'Failed'})

@api.route('/api/unrealised_stock_returns/', methods = ['GET'])
def unrealised_stock_returns_lookup():
    try:
        data = get_unrealised_swing_stock_returns()
        return jsonify({'data': data,'message': 'Successfully retrieved Unrealised Swing Stock Returns','status': 'Success'})
    except Exception as e:
        return jsonify({'message': repr(e), 'status': 'Failed'})

@api.route('/api/process_consolidated_returns/', methods = ['GET'])
def process_consolidated_returns():
    try:
        start_date = request.args.get('start_date') or None
        end_date   = request.args.get('end_date') or None
        on_start   = request.args.get('on_start') or None

        create_consolidated_returns_table()
        create_agg_consolidated_returns_table()
        create_fin_consolidated_returns_table()
        
        if on_start == "true" or (start_date):
            consolidated_returns_process_group_logs = execute_process_group_using_metadata('CONSOLIDATED_RETURNS_DAILY_PROCESS_GROUP', start_date, end_date)
        elif not start_date and not end_date:
            consolidated_returns_process_group_logs = execute_process_group_using_metadata('CONSOLIDATED_RETURNS_HIST_PROCESS_GROUP')
        return jsonify(consolidated_returns_process_group_logs)
    except Exception as e:
        return jsonify({'message': repr(e), 'status': 'Failed'})

@api.route('/api/consolidated_returns/', methods = ['GET'])
def consolidated_returns_lookup():
    try:
        data = get_consolidated_returns()
        return jsonify({'data': data,'message': 'Successfully retrieved from Consolidated Returns','status': 'Success'})
    except Exception as e:
        return jsonify({'message': repr(e), 'status': 'Failed'})

@api.route('/api/consolidated_returns/all/', methods = ['GET'])
def consolidated_returns_fetch_all():
    try:
        data = get_all_from_consolidated_returns_table()
        return jsonify({'data': data,'message': 'Successfully retrieved Consolidated Returns','status': 'Success'})
    except Exception as e:
        return jsonify({'message': repr(e), 'status': 'Failed'})

@api.route('/api/process_consolidated_allocation/', methods = ['GET'])
def process_consolidated_allocation():
    try:
        start_date = request.args.get('start_date') or None
        end_date   = request.args.get('end_date') or None
        on_start   = request.args.get('on_start') or None

        create_consolidated_allocation_table()
        create_agg_consolidated_allocation_table()
        create_fin_consolidated_allocation_table()
        
        if on_start == "true" or (start_date):
            consolidated_allocation_process_group_logs = execute_process_group_using_metadata('CONSOLIDATED_ALLOCATION_DAILY_PROCESS_GROUP', start_date, end_date)
        elif not start_date and not end_date:
            consolidated_allocation_process_group_logs = execute_process_group_using_metadata('CONSOLIDATED_ALLOCATION_HIST_PROCESS_GROUP')
        return jsonify(consolidated_allocation_process_group_logs)
    except Exception as e:
        return jsonify({'message': repr(e), 'status': 'Failed'})

@api.route('/api/consolidated_allocation/', methods = ['GET'])
def consolidated_allocation_portfolio_lookup():
    try:
        data = get_consolidated_allocation()
        return jsonify({'data': data,'message': 'Successfully retrieved Consolidated Allocation','status': 'Success'})
    except Exception as e:
        return jsonify({'message': repr(e), 'status': 'Failed'})

@api.route('/api/consolidated_allocation/all/', methods = ['GET'])
def consolidated_allocation_fetch_all():
    try:
        data = get_all_consolidated_allocation()
        return jsonify({'data': data,'message': 'Successfully retrieved Consolidated Allocation','status': 'Success'})
    except Exception as e:
        return jsonify({'message': repr(e), 'status': 'Failed'})

@api.route('/api/process_simulate_returns/', methods = ['GET'])
def process_simulated_returns():
    try:
        start_date = request.args.get('start_date') or None
        end_date = request.args.get('end_date') or None
        on_start = request.args.get('on_start') or None

        create_simulated_portfolio_table()
        create_agg_simulated_portfolio_table()
        create_fin_simulated_portfolio_table()

        if on_start == "true" or (start_date):
            simulated_returns_process_group_logs = execute_process_group_using_metadata('SIMULATED_RETURNS_DAILY_PROCESS_GROUP', start_date, end_date)
        elif not start_date and not end_date:
            simulated_returns_process_group_logs = execute_process_group_using_metadata('SIMULATED_RETURNS_HIST_PROCESS_GROUP')
        return jsonify(simulated_returns_process_group_logs)
    except Exception as e:
        return jsonify({'message': repr(e), 'status': 'Failed'})

@api.route('/api/simulated_returns/', methods = ['GET'])
def fetch_simulated_returns():
    try:
        data = get_simulated_returns_from_fin_simulated_returns_table()
        return jsonify({'data': data,'message': 'Successfully retrieved from FIN_SIMULATED_RETURNS Table','status': 'Success'})
    except Exception as e:
        return jsonify({'message': repr(e), 'status': 'Failed'})

@api.route('/api/table_and_view_info/', methods = ['GET'])
def get_table_and_view_info_from_db():
    try:
        table_info = get_component_info_from_db('table')
        view_info  = get_component_info_from_db('view')
        return jsonify({'table_info': table_info, 'view_info': view_info, 'status': 'Success'})
    except Exception as e:
        return jsonify({'message': repr(e), 'status': "Failed"})

@api.route('/api/process_entry/', methods = ['POST'])
def add_process_entry():
    try:
        process_entry_payloads       = loads(request.form.get('process_entry_values')) # json.loads
        process_group_payloads       = []
        process_payloads             = []
        process_keycolumns_payloads  = []
        for payload in process_entry_payloads:
            holiday_calendar_data = get_date_setup_from_holiday_calendar(date.today().strftime('%Y-%m-%d'))

            process_group_payload = {
                'PROCESS_GROUP'             : payload['PROCESS_GROUP']
                ,'OUT_PROCESS_NAME'         : payload['OUT_PROCESS_NAME']
                ,'CONSIDER_FOR_PROCESSING'  : payload['CONSIDER_FOR_PROCESSING']
                ,'EXECUTION_ORDER'          : payload['EXECUTION_ORDER']
                ,'PROCESSING_DATE'          : holiday_calendar_data['PROCESSING_DATE']
                ,'NEXT_PROCESSING_DATE'     : holiday_calendar_data['NEXT_PROCESSING_DATE']
                ,'PREVIOUS_PROCESSING_DATE' : holiday_calendar_data['PREVIOUS_PROCESSING_DATE']
            }
            process_group_payloads.append(process_group_payload)

            process_payload = {
                'OUT_PROCESS_NAME'            : payload['OUT_PROCESS_NAME']
                ,'PROCESS_TYPE'               : payload['PROCESS_TYPE']
                ,'PROC_TYP_CD_LIST'           : payload['PROC_TYP_CD_LIST']
                ,'INPUT_VIEW'                 : payload['INPUT_VIEW']
                ,'TARGET_TABLE'               : payload['TARGET_TABLE']
                ,'PROCESS_DESCRIPTION'        : payload['PROCESS_DESCRIPTION']
                ,'AUTO_TRIGGER_ON_LAUNCH'     : payload['AUTO_TRIGGER_ON_LAUNCH']
                ,'PROCESS_DECOMMISSIONED'     : payload['PROCESS_DECOMMISSIONED']
                ,'FREQUENCY'                  : payload['FREQUENCY']
                ,'DEFAULT_START_DATE_TYPE_CD' : payload['DEFAULT_START_DATE_TYPE_CD']
                ,'PROCESSING_DATE'            : holiday_calendar_data['PROCESSING_DATE']
                ,'NEXT_PROCESSING_DATE'       : holiday_calendar_data['NEXT_PROCESSING_DATE']
                ,'PREVIOUS_PROCESSING_DATE'   : holiday_calendar_data['PREVIOUS_PROCESSING_DATE']
            }
            process_payloads.append(process_payload)

            for keycolumn_name in payload['PROCESS_KEYCOLUMNS']:
                process_keycolumn_payload = {
                    'OUT_PROCESS_NAME'          : payload['OUT_PROCESS_NAME']
                    ,'KEYCOLUMN_NAME'           : keycolumn_name
                    ,'CONSIDER_FOR_PROCESSING'  : payload['CONSIDER_FOR_PROCESSING']
                    ,'PROCESSING_DATE'          : holiday_calendar_data['PROCESSING_DATE']
                    ,'NEXT_PROCESSING_DATE'     : holiday_calendar_data['NEXT_PROCESSING_DATE']
                    ,'PREVIOUS_PROCESSING_DATE' : holiday_calendar_data['PREVIOUS_PROCESSING_DATE']
                }
                process_keycolumns_payloads.append(process_keycolumn_payload)
            
        process_group_logs     = execute_process_group_using_metadata('PROCESS_GROUP_ENTRY_PROCESS_GROUP', None, None, process_group_payloads, "true")
        process_logs           = execute_process_group_using_metadata('PROCESS_ENTRY_PROCESS_GROUP', None, None, process_payloads, "true")
        process_keycolumn_logs = execute_process_group_using_metadata('PROCESS_KEYCOLUMN_ENTRY_PROCESS_GROUP', None, None, process_keycolumns_payloads, "true")
        status = "Success" if process_group_logs['status'] == "Success" and process_logs['status'] == "Success" and process_keycolumn_logs['status'] == "Success" else "Failed"
        return jsonify({'message': f"{process_group_logs['message']} and {process_logs['message']} and {process_keycolumn_logs['message']}", 'status': status})
    except Exception as e:
        return jsonify({'message': repr(e), 'status': "Failed"})

@api.route('/api/missing_prices/', methods = ['GET'])
def get_missing_prices():
    try:
        missing_price_data = get_missing_prices_from_price_table()
        return jsonify({'missing_price_data': missing_price_data, 'status': 'Success'})
    except Exception as e:
        return jsonify({'message': repr(e), 'status': "Failed"})

@api.route('/api/missing_prices/', methods = ['POST'])
def insert_missing_prices():
    try:
        missing_price_payloads = loads(request.form.get('missing_price_payload'))
        filtered_payloads = []
        for missing_price_payload in missing_price_payloads:
            if missing_price_payload['PRICE']:
                value_date = datetime.strptime(missing_price_payload['VALUE_DATE'],'%Y-%m-%d')
                value_date = value_date.strftime('%Y-%m-%d')

                holiday_calendar_data                             = get_date_setup_from_holiday_calendar(value_date)
                missing_price_payload['PROCESSING_DATE']          = holiday_calendar_data['PROCESSING_DATE']
                missing_price_payload['NEXT_PROCESSING_DATE']     = holiday_calendar_data['NEXT_PROCESSING_DATE']
                missing_price_payload['PREVIOUS_PROCESSING_DATE'] = holiday_calendar_data['PREVIOUS_PROCESSING_DATE']
                filtered_payloads.append(missing_price_payload)

        process_price_logs = execute_process_group_using_metadata('PRICE_DAILY_PROCESS_GROUP', None, None, filtered_payloads, "true")
        return jsonify(process_price_logs)
    except Exception as e:
        return jsonify({'message': repr(e), 'status': "Failed"})