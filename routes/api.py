from flask import Blueprint, jsonify, request
import yfinance as yf
from dateutil import parser
from datetime import datetime, timedelta
import os
from json import loads
from uuid import NAMESPACE_URL,uuid5

from utils.sql_utils.sql_utils import \
create_price_table,\
delete_alt_symbol_from_price_table,\
upsert_into_price_table,\
create_metadata_store_table,\
insert_metadata_store_entry,\
get_price_from_price_table,\
create_mf_order_table,\
insert_mf_order_entry,\
get_proc_date_from_processing_date_table,\
update_proc_date_in_processing_date_table,\
create_processing_date_table,\
get_max_value_date_for_alt_symbol,\
duplicate_check_on_price_table,\
create_mf_portfolio_views_in_db,\
create_holiday_date_table,\
insert_into_holiday_dates_table,\
get_holiday_date_from_holiday_dates_table,\
truncate_holiday_calendar_table,\
create_holiday_calendar_table,\
insert_into_holiday_calendar_table,\
create_working_date_table,\
insert_into_working_date_table,\
get_working_date_from_working_dates_table,\
get_first_purchase_date_from_mf_order_date_table,\
truncate_mf_hist_returns_table,\
create_mf_hist_returns_table,\
get_date_setup_from_holiday_calendar,\
get_metrics_from_fin_mutual_fund_portfolio_view,\
insert_into_mf_hist_returns,\
get_mf_hist_returns_from_mf_hist_returns_table,\
get_max_next_proc_date_from_mf_hist_returns_table,\
create_stock_order_table,\
insert_stock_order_entry,\
get_all_symbols_list_from_metadata_store,\
upsert_trade_entry_in_db,\
create_trade_table,\
create_fee_table,\
upsert_fee_entry_in_db,\
create_stock_portfolio_views_in_db,\
get_stock_hist_returns_from_realised_intraday_stock_and_swing_stock_hist_returns_table,\
truncate_realised_intraday_stock_hist_returns_table,\
create_realised_intraday_stock_hist_returns_table,\
insert_into_realised_intraday_stock_hist_returns,\
get_max_trade_date_from_realised_intraday_stock_hist_returns_table,\
get_open_trades_from_trades_table,\
create_close_trades_table,\
insert_into_close_trades_table,\
truncate_realised_swing_stock_hist_returns_table,\
create_realised_swing_stock_hist_returns_table,\
insert_into_realised_swing_stock_hist_returns,\
get_max_trade_close_date_from_realised_swing_stock_hist_returns_table,\
truncate_unrealised_swing_stock_hist_returns_table,\
create_unrealised_swing_stock_hist_returns_table,\
get_first_swing_trade_date_from_trades_table,\
get_metrics_from_fin_stock_swing_unrealised_portfolio_view,\
insert_into_unrealised_swing_stock_hist_returns,\
get_stock_hist_returns_from_unrealised_swing_stock_hist_returns_table,\
get_max_next_proc_date_from_unrealised_swing_stock_hist_returns_table,\
create_consolidated_portfolio_views_in_db,\
truncate_consolidated_hist_returns_table,\
create_consolidated_hist_returns_table,\
get_first_purchase_date_from_all_portfolios,\
get_metrics_from_fin_consolidated_portfolio_view,\
insert_into_consolidated_hist_returns,\
get_metrics_from_agg_consolidated_portfolio_view,\
truncate_agg_consolidated_hist_returns_table,\
create_agg_consolidated_hist_returns_table,\
insert_into_agg_consolidated_hist_returns,\
get_consolidated_hist_returns_from_consolidated_hist_returns_table,\
get_max_next_proc_date_from_consolidated_hist_returns_table,\
get_max_proc_date_from_all_hist_tables,\
get_all_from_consolidated_hist_returns_table,\
get_max_next_proc_date_from_consolidated_hist_allocation_table,\
truncate_consolidated_hist_allocation_table,\
create_consolidated_hist_allocation_table,\
truncate_agg_consolidated_hist_allocation_table,\
create_agg_consolidated_hist_allocation_table,\
truncate_consolidated_hist_allocation_portfolio_table,\
create_consolidated_hist_allocation_portfolio_table,\
get_metrics_from_agg_consolidated_allocation_view_and_insert_into_agg_consolidated_allocation_table,\
get_metrics_from_fin_consolidated_allocation_view_and_insert_into_fin_consolidated_allocation_table,\
get_metrics_from_fin_consolidated_allocation_portfolio_view_and_insert_into_fin_consolidated_allocation_portfolio_table,\
get_consolidated_hist_allocation_portfolio_from_consolidated_hist_allocation_portfolio_table,\
get_all_from_consolidated_hist_allocation_table

from utils.date_utils.date_utils import convert_weekday_from_int_to_char

# Folders

from utils.folder_utils.paths import upload_folder_path
from werkzeug.utils import secure_filename
from PyPDF2 import PdfReader

api = Blueprint('api', __name__)

app_namespace = uuid5(NAMESPACE_URL,"Consolidated_Portfolio_Tracker") # Change Later

@api.route('/api/price_table/max_value_date/', methods = ['GET'])
def get_max_value_date_from_price_table():
    try:
        process_flag              = request.args.get('process_flag') or None
        consider_for_hist_returns = request.args.get('consider_for_hist_returns') or None
        portfolio_type            = request.args.get('portfolio_type') or None
        max_value_date_data = get_max_value_date_for_alt_symbol(process_flag, consider_for_hist_returns, portfolio_type)
        max_value_date_data = max_value_date_data.get_json()
        return jsonify({'max_value_date_data':max_value_date_data, 'message': "Successfully retrieved Maximum Value Date data from PRICE_TABLE table", 'status': "Success"})
    except Exception as e:
        return jsonify({'message': repr(e), 'status': "Failed"})

@api.route('/api/price_table/close_price/<alt_symbol>/', methods = ['POST'])
def upsert_price_table_for_alt_symbol(alt_symbol):
    try:
        yahoo_symbol   = request.form.get('yahoo_symbol')
        portfolio_type = request.form.get('portfolio_type')
        start_date     = request.form.get('start_date')
        end_date       = request.form.get('end_date')
        start_from     = request.args.get('start_from') or None
        end_till       = request.args.get('end_till') or None

        create_price_table()
        if not start_from or not end_till:
            delete_alt_symbol_from_price_table(alt_symbol)
        if start_from and end_till:
            start_date = start_from
            end_date   = end_till

        ticker = yf.Ticker(yahoo_symbol)
        pandas_data = ticker.history(start = start_date, end = end_date)
        for index, value in pandas_data['Close'].items():
            value_date = str(index)[:10]
            if (parser.parse(value_date, fuzzy = 'fuzzy')):
                upsert_into_price_table(alt_symbol, portfolio_type, value_date, '15:30:00', round(value,4), 'CLOSE_PRICE', value_date, '9998-12-31', 0)
        return jsonify({'message': f"Successfully inserted Close Price data into PRICE_TABLE table for {alt_symbol} from {start_date} and {end_date}", 'status': "Success"})
    except Exception as e:
        return jsonify({'message': repr(e), 'status': "Failed"})

@api.route('/api/metadata_store/', methods = ['POST'])
def metadata_entry():
    try:
        exchange_symbol         = request.form.get('exchange_symbol')
        yahoo_symbol            = request.form.get('yahoo_symbol')
        alt_symbol              = request.form.get('alt_symbol')
        allocation_category     = request.form.get('allocation_category')
        portfolio_type          = request.form.get('portfolio_type')
        amc                     = request.form.get('amc') or None
        mf_type                 = request.form.get('type') or None
        fund_category           = request.form.get('fund_category') or None
        launched_on             = request.form.get('launched_on') or None
        exit_load               = request.form.get('exit_load') or None
        expense_ratio           = request.form.get('expense_ratio') or None
        fund_manager            = request.form.get('fund_manager') or None
        fund_manager_started_on = request.form.get('fund_manager_started_on') or None
        isin                    = request.form.get('isin') or None
        create_metadata_store_table()
        insert_metadata_store_entry(exchange_symbol, yahoo_symbol, alt_symbol, allocation_category, portfolio_type, amc, mf_type, fund_category, launched_on, exit_load, expense_ratio, fund_manager, fund_manager_started_on, isin)
        return jsonify({'message': "Successfully inserted metadata into METADATA_STORE table", 'status': "Success"})
    except Exception as e:
        return jsonify({'message': repr(e), 'status': "Failed"})
    
@api.route('/api/metadata_store/symbols/', methods = ['GET'])
def get_all_symbols_list():
    try:
        portfolio_type = request.args.get('portfolio_type') or None
        all_symbols_data = get_all_symbols_list_from_metadata_store(portfolio_type)
        all_symbols_data = all_symbols_data.get_json()
        return jsonify({'all_symbols_list': all_symbols_data, 'message': "Successfully retrieved All Symbols List from METADATA_STORE Table", 'status': "Success"})
    except Exception as e:
        return jsonify({'message': repr(e), 'status': "Failed"})
    
@api.route('/api/price_table/close_price/', methods = ['GET'])
def price_table_lookup():
    try:
        alt_symbol    = request.args.get('alt_symbol') or None
        purchase_date = request.args.get('purchase_date') or None
        price_data = get_price_from_price_table(alt_symbol, purchase_date)
        price_data = price_data.get_json()
        return jsonify({'price_data': price_data, 'message': "Successfully retrieved Price data from PRICE_TABLE", 'status': "Success"})
    except Exception as e:
        return jsonify({'message': repr(e), 'status': "Failed"})

@api.route('/api/mf_order/', methods = ['POST'])
def mf_order():
    try:
        exchange_symbol = request.form.get('exchange_symbol')
        purchase_date = request.form.get('purchase_date')
        invested_amount = request.form.get('invested_amount')
        stamp_fees_amount = request.form.get('stamp_fees_amount')
        amc_amount = request.form.get('amc_amount')
        price_during_purchase = request.form.get('price_during_purchase')
        units = request.form.get('units')
        units = round(float(units), 3)
        stamp_fees_amount = round(float(invested_amount) - float(amc_amount),2)
        create_mf_order_table()
        insert_mf_order_entry(exchange_symbol, purchase_date, invested_amount, stamp_fees_amount, amc_amount, price_during_purchase, units)
        return jsonify({'message': "Successfully inserted data into MF_ORDER table", 'status': "Success"})
    except Exception as e:
        return jsonify({'message': repr(e), 'status': "Failed"})
    
@api.route('/api/processing_date/', methods = ['GET'])
def proc_date_lookup():
    try:
        data = get_proc_date_from_processing_date_table()
        data = data.get_json()
        MF_PROC = data['MF_PROC']
        PPF_MF_PROC = data['PPF_MF_PROC']
        STOCK_PROC = data['STOCK_PROC']

        mf_proc_date = MF_PROC[1]
        mf_next_proc_date = MF_PROC[2]
        mf_prev_proc_date = MF_PROC[3]

        ppf_mf_proc_date = PPF_MF_PROC[1]
        ppf_mf_next_proc_date = PPF_MF_PROC[2]
        ppf_mf_prev_proc_date = PPF_MF_PROC[3]

        stock_proc_date = STOCK_PROC[1]
        stock_next_proc_date = STOCK_PROC[2]
        stock_prev_proc_date = STOCK_PROC[3]

        return jsonify({'mf_proc_date': mf_proc_date,'mf_next_proc_date' : mf_next_proc_date, 'mf_prev_proc_date': mf_prev_proc_date,
                        'ppf_mf_proc_date': ppf_mf_proc_date,'ppf_mf_next_proc_date' : ppf_mf_next_proc_date, 'ppf_mf_prev_proc_date': ppf_mf_prev_proc_date,
                        'stock_proc_date': stock_proc_date,'stock_next_proc_date' : stock_next_proc_date, 'stock_prev_proc_date': stock_prev_proc_date,
                          'message': "Successfully retrieved data from PROCESSING_DATE Table", 'status': "Success"})
    except Exception as e:
        return jsonify({'message': repr(e), 'status': 'Failed'})

@api.route('/api/processing_date/', methods = ['POST'])
def proc_date_update():
    try:
        create_processing_date_table()

        mf_proc_date          = request.form.get('mf_proc_date')
        mf_next_proc_date     = request.form.get('mf_next_proc_date')
        mf_prev_proc_date     = request.form.get('mf_prev_proc_date')

        ppf_mf_proc_date      = request.form.get('ppf_mf_proc_date')
        ppf_mf_next_proc_date = request.form.get('ppf_mf_next_proc_date')
        ppf_mf_prev_proc_date = request.form.get('ppf_mf_prev_proc_date')

        stock_proc_date       = request.form.get('stock_proc_date')
        stock_next_proc_date  = request.form.get('stock_next_proc_date')
        stock_prev_proc_date  = request.form.get('stock_prev_proc_date')

        update_proc_date_in_processing_date_table('MF_PROC', mf_proc_date, mf_next_proc_date, mf_prev_proc_date)
        update_proc_date_in_processing_date_table('PPF_MF_PROC', ppf_mf_proc_date, ppf_mf_next_proc_date, ppf_mf_prev_proc_date)
        update_proc_date_in_processing_date_table('STOCK_PROC', stock_proc_date, stock_next_proc_date, stock_prev_proc_date)
        return jsonify({'message': "Successfully updated PROCESSING_DATE Table", 'status': "Success"})
    except Exception as e:
        return jsonify({'message': repr(e), 'status': 'Failed'})

@api.route('/api/price_table/duplicate_check/', methods = ['GET'])
def duplicate_check_price_table():
    try:
        dup_check_response = duplicate_check_on_price_table()
        if dup_check_response:
            dup_check_data = dup_check_response.get_json()
            return jsonify({'dup_check_data': dup_check_data,'message': 'Duplicates Present in PRICE_TABLE','status': 'Duplicate Issue'})
        else:
            return ({'message': 'Successfully completed Duplicate Check On PRICE_TABLE for All Alt Symbols','status': 'Success'})
    except Exception as e:
        return jsonify({'message': repr(e), 'status': 'Failed'})
    
@api.route('/api/create_portfolio_views/', methods = ['GET'])
def create_portfolio_views():
    try:
        create_mf_portfolio_views_in_db()
        create_stock_portfolio_views_in_db()
        create_consolidated_portfolio_views_in_db()
        return jsonify({'message': 'Successfully replaced Portfolio Views in DB','status': 'Success'})
    except Exception as e:
        return jsonify({'message': repr(e), 'status': 'Failed'})

@api.route('/api/create_managed_tables/', methods = ['GET'])
def create_managed_tables():
    try:
        create_metadata_store_table()
        create_price_table()
        create_processing_date_table()
        create_mf_order_table()
        create_holiday_date_table()
        create_working_date_table()
        create_holiday_calendar_table()
        create_mf_hist_returns_table()
        create_stock_order_table()
        create_trade_table()
        create_fee_table()
        create_realised_intraday_stock_hist_returns_table()
        create_close_trades_table()
        create_realised_swing_stock_hist_returns_table()
        create_unrealised_swing_stock_hist_returns_table()
        create_consolidated_hist_returns_table()
        create_agg_consolidated_hist_returns_table()
        create_consolidated_hist_allocation_table()
        create_agg_consolidated_hist_allocation_table()
        create_consolidated_hist_allocation_portfolio_table()
        return jsonify({'message': 'Successfully created Managed Tables in DB','status': 'Success'})
    except Exception as e:
        return jsonify({'message': repr(e), 'status': 'Failed'}) 
    
@api.route('/api/holiday_date/', methods = ['POST'])
def holiday_date_entry():
    try:
        create_holiday_date_table()

        holiday_date = request.form.get('holiday_date')
        holiday_name = request.form.get('holiday_name')
        holiday_day = request.form.get('holiday_day')
        insert_into_holiday_dates_table(holiday_date, holiday_name, holiday_day)
        return jsonify({'message': 'Successfully inserted holiday into HOLIDAY_DATES Table','status': 'Success'})
    except Exception as e:
        return jsonify({'message': repr(e), 'status': 'Failed'})
    
@api.route('/api/holiday_date/', methods = ['GET'])
def holiday_date_lookup():
    try:
        current_year = request.args.get('current_year') or None
        data = get_holiday_date_from_holiday_dates_table(current_year)
        data = data.get_json()
        return jsonify({'data': data,'message': 'Successfully retrieved from HOLIDAY_DATES Table','status': 'Success'})
    except Exception as e:
        return jsonify({'message': repr(e), 'status': 'Failed'})
    
@api.route('/api/working_date/', methods = ['POST'])
def working_date_entry():
    try:
        create_working_date_table()

        working_date = request.form.get('working_date')
        working_day_name = request.form.get('working_day_name')
        working_day = request.form.get('working_day')
        insert_into_working_date_table(working_date, working_day_name, working_day)
        return jsonify({'message': 'Successfully inserted working date into WORKING_DATES Table','status': 'Success'})
    except Exception as e:
        return jsonify({'message': repr(e), 'status': 'Failed'})
    
@api.route('/api/working_date/', methods = ['GET'])
def working_date_lookup():
    try:
        data = get_working_date_from_working_dates_table()
        data = data.get_json()
        if data[0]['working_date']:
            return jsonify({'data': data,'message': 'Successfully retrieved from WORKING_DATES Table','status': 'Success'})
        else:
            return jsonify({'data': [],'message': 'No records present in WORKING_DATES Table','status': 'Success'})
    except Exception as e:
        return jsonify({'message': repr(e), 'status': 'Failed'})

@api.route('/api/holiday_calendar_setup/', methods = ['POST'])
def holiday_calendar_setup():
    try:
        holiday_calendar_start_date = request.form.get('holiday_calendar_start_date')
        holiday_calendar_end_date   = request.form.get('holiday_calendar_end_date')
        holiday_data                = request.form.get('holiday_data')
        working_day_data            = request.form.get('working_day_data')
        
        holiday_dates = holiday_data.replace("[","").replace("]","").replace('"','').split(",")
        working_dates = working_day_data.replace("[","").replace("]","").replace('"','').split(",")

        create_holiday_calendar_table()

        counter_date = datetime.strptime(holiday_calendar_start_date,'%Y-%m-%d')
        while(counter_date <= datetime.strptime(holiday_calendar_end_date,'%Y-%m-%d')):
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
                
                insert_into_holiday_calendar_table(counter_date.strftime('%Y-%m-%d'), counter_day, next_counter_date.strftime('%Y-%m-%d'), next_counter_day, prev_counter_date.strftime('%Y-%m-%d'), prev_counter_day)
            counter_date = counter_date + timedelta(days = 1)
        return jsonify({'message': 'Successfully inserted holiday calendar into HOLIDAY_CALENDAR Table','status': 'Success'})
    except Exception as e:
        return jsonify({'message': repr(e), 'status': 'Failed'})
    
@api.route('/api/process_mf_hist_returns/', methods = ['GET'])
def process_mf_hist_returns():
    try:
        truncate_mf_hist_returns_table()
        create_mf_hist_returns_table()
        
        first_purchase_data = get_first_purchase_date_from_mf_order_date_table()
        first_purchase_data = first_purchase_data.get_json()
        first_purchase_date = first_purchase_data['first_purchase_date']

        counter_date = datetime.strptime(first_purchase_date,'%Y-%m-%d')
        first_purchase_date = datetime.strptime(first_purchase_date,'%Y-%m-%d')

        while(counter_date <= datetime.today() + timedelta(days = -2)):

            holiday_calendar_data = get_date_setup_from_holiday_calendar(counter_date.strftime('%Y-%m-%d'))
            holiday_calendar_data = holiday_calendar_data.get_json()
            processing_date = holiday_calendar_data[0]['processing_date']
            next_processing_date = holiday_calendar_data[0]['next_processing_date']
            prev_processing_date = holiday_calendar_data[0]['prev_processing_date']

            update_proc_date_in_processing_date_table('MF_PROC', processing_date, next_processing_date, prev_processing_date)
            update_proc_date_in_processing_date_table('PPF_MF_PROC', processing_date, next_processing_date, prev_processing_date)

            hist_returns_data = get_metrics_from_fin_mutual_fund_portfolio_view()
            hist_returns_data = hist_returns_data.get_json()
            total_p_l = hist_returns_data[0]['total_p_l']
            amount_invested_as_on_processing_date = hist_returns_data[0]['amount_invested_as_on_processing_date']
            amount_as_on_processing_date = hist_returns_data[0]['amount_as_on_processing_date']
            amount_as_on_prev_processing_date = hist_returns_data[0]['amount_as_on_prev_processing_date']
            perc_total_p_l = hist_returns_data[0]['perc_total_p_l']
            day_p_l = hist_returns_data[0]['day_p_l']
            perc_day_p_l = hist_returns_data[0]['perc_day_p_l']


            insert_into_mf_hist_returns(processing_date, next_processing_date, prev_processing_date, total_p_l, amount_invested_as_on_processing_date, amount_as_on_processing_date, amount_as_on_prev_processing_date, perc_total_p_l, day_p_l, perc_day_p_l)

            counter_date = datetime.strptime(next_processing_date,'%Y-%m-%d')

        return jsonify({'message': 'Successfully inserted historic returns in to MF_HIST_RETURNS Table','status': 'Success'})
    except Exception as e:
        return jsonify({'message': repr(e), 'status': 'Failed'})
    
@api.route('/api/mf_hist_returns/', methods = ['GET'])
def mf_hist_returns_lookup():
    try:
        data = get_mf_hist_returns_from_mf_hist_returns_table()
        data = data.get_json()
        return jsonify({'data': data,'message': 'Successfully retrieved from MF_HIST_RETURNS Table','status': 'Success'})
    except Exception as e:
        return jsonify({'message': repr(e), 'status': 'Failed'})
    
@api.route('/api/mf_hist_returns/max_next_proc_date/', methods = ['GET'])
def mf_hist_returns_max_date():
    try:
        max_date = get_max_next_proc_date_from_mf_hist_returns_table()
        max_date = max_date.get_json()
        return jsonify({'data': max_date,'message': 'Successfully retrieved Max Date from MF_HIST_RETURNS Table','status': 'Success'})
    except Exception as e:
        return jsonify({'message': repr(e), 'status': 'Failed'})
    
@api.route('/api/mf_hist_returns/<start_date>/<end_date>/', methods = ['GET'])
def process_mf_hist_returns_from_start_to_end_date(start_date, end_date):
    try:
        start_date = datetime.strptime(start_date,'%Y-%m-%d')
        end_date = datetime.strptime(end_date,'%Y-%m-%d')
        counter_date = start_date
        log_date = []

        while(counter_date <= end_date):

            holiday_calendar_data = get_date_setup_from_holiday_calendar(counter_date.strftime('%Y-%m-%d'))
            holiday_calendar_data = holiday_calendar_data.get_json()
            processing_date = holiday_calendar_data[0]['processing_date']
            next_processing_date = holiday_calendar_data[0]['next_processing_date']
            prev_processing_date = holiday_calendar_data[0]['prev_processing_date']

            update_proc_date_in_processing_date_table('MF_PROC', processing_date, next_processing_date, prev_processing_date)
            update_proc_date_in_processing_date_table('PPF_MF_PROC', processing_date, next_processing_date, prev_processing_date)
            
            hist_returns_data = get_metrics_from_fin_mutual_fund_portfolio_view()
            hist_returns_data = hist_returns_data.get_json()
            total_p_l = hist_returns_data[0]['total_p_l']
            amount_invested_as_on_processing_date = hist_returns_data[0]['amount_invested_as_on_processing_date']
            amount_as_on_processing_date = hist_returns_data[0]['amount_as_on_processing_date']
            amount_as_on_prev_processing_date = hist_returns_data[0]['amount_as_on_prev_processing_date']
            perc_total_p_l = hist_returns_data[0]['perc_total_p_l']
            day_p_l = hist_returns_data[0]['day_p_l']
            perc_day_p_l = hist_returns_data[0]['perc_day_p_l']


            insert_into_mf_hist_returns(processing_date, next_processing_date, prev_processing_date, total_p_l, amount_invested_as_on_processing_date, amount_as_on_processing_date, amount_as_on_prev_processing_date, perc_total_p_l, day_p_l, perc_day_p_l)
            log_date.append(processing_date)
            counter_date = datetime.strptime(next_processing_date,'%Y-%m-%d')
        return jsonify({'message': f'Successfully inserted historic returns for {str(log_date)} in to MF_HIST_RETURNS Table','status': 'Success'})
    except Exception as e:
        return jsonify({'message': repr(e), 'status': 'Failed'})

@api.route('/api/stock_order/', methods = ['POST'])
def stock_order():
    try:
        alt_symbol = request.form.get('alt_symbol')
        trade_entry_date = request.form.get('trade_entry_date')
        trade_entry_time = request.form.get('trade_entry_time')
        trade_exit_date = request.form.get('trade_exit_date')
        trade_exit_time = request.form.get('trade_exit_time')
        stock_quantity = request.form.get('stock_quantity')
        trade_type = request.form.get('trade_type')
        leverage = request.form.get('leverage')
        trade_position = request.form.get('trade_position')
        stock_buy_price = request.form.get('stock_buy_price')
        stock_sell_price = request.form.get('stock_sell_price')
        brokerage = request.form.get('brokerage')
        exchange_transaction_fees = request.form.get('exchange_transaction_fees')
        igst = request.form.get('igst')
        securities_transaction_tax = request.form.get('securities_transaction_tax')
        sebi_turnover_fees = request.form.get('sebi_turnover_fees')
        auto_square_off_charges = request.form.get('auto_square_off_charges')
        depository_charges = request.form.get('depository_charges')
        holding_days, sell_minus_buy, actual_p_l_w_o_leverage, deployed_capital, trade_exit_time = None, None, None, None, None
        net_obligation, total_fees, net_receivable, actual_p_l_w_leverage = None, None, None, None
        if trade_exit_date:
            trade_exit_time = request.form.get('trade_exit_time')
            # Derived Fields
            holding_days = request.form.get('holding_days')
            sell_minus_buy = round(float(request.form.get('sell_minus_buy')), 4)
            actual_p_l_w_o_leverage = round(float(request.form.get('actual_p_l_w_o_leverage')),2)
            deployed_capital = round(float(request.form.get('deployed_capital')),4)
            net_obligation = round(float(request.form.get('net_obligation')),4)
            total_fees = round(float(request.form.get('total_fees')),4)
            net_receivable = round(float(request.form.get('net_receivable')),4)
            actual_p_l_w_leverage = round(float(request.form.get('actual_p_l_w_leverage')),2)

        create_stock_order_table()
        insert_stock_order_entry(alt_symbol, trade_entry_date, trade_entry_time, trade_exit_date, trade_exit_time, stock_quantity, trade_type, leverage, trade_position, stock_buy_price, stock_sell_price, brokerage, exchange_transaction_fees, igst, securities_transaction_tax, sebi_turnover_fees, holding_days, sell_minus_buy, actual_p_l_w_o_leverage, deployed_capital, net_obligation, total_fees, net_receivable, auto_square_off_charges, depository_charges, actual_p_l_w_leverage)
        return jsonify({'message': "Successfully inserted data into STOCK_ORDER table", 'status': "Success"})
    except Exception as e:
        return jsonify({'message': repr(e), 'status': "Failed"})
    
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

            # Create the Upload Directory if it doesn't exist
            os.makedirs(upload_folder_path, exist_ok=True)
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

        create_trade_table()
        create_fee_table()
        for trade in trades_array:
            unique_trade_id = uuid5(app_namespace, str(trade['trade_number']) + str(trade['trade_entry_date']))
            unique_fee_id = uuid5(app_namespace, str(trade['trade_entry_date']) + str(trade['trade_type']))
            unique_trade_set_id = uuid5(app_namespace, str(trade['trade_entry_date']) + str(trade['stock_symbol']) + str(trade['trade_set']))
            trade_date = trade['trade_entry_date']
            upsert_trade_entry_in_db(unique_trade_id, unique_fee_id, unique_trade_set_id, trade['stock_symbol'], trade['stock_isin'], trade['trade_entry_date'], trade['order_number'], trade['order_time'], trade['trade_number'], trade['trade_time'], trade['buy_or_sell'], trade['stock_quantity'], trade['brokerage_per_trade'], trade['net_trade_price_per_unit'], trade['net_total_before_levies'], trade['trade_set'], trade['trade_position'], trade['trade_entry_date'], trade['trade_entry_time'], trade['trade_exit_date'], trade['trade_exit_time'], trade['trade_type'], trade['leverage'])
        for trade_type in fee_data['trade_types']:
            if trade_type == "Intraday Trading":
                unique_fee_id = uuid5(app_namespace, str(trade['trade_entry_date']) + "Intraday Trading")
                upsert_fee_entry_in_db(unique_fee_id, trade_date, fee_data['intraday_net_obligation'], fee_data['intraday_brokerage'], fee_data['intraday_exc_trans_charges'], fee_data['intraday_igst'], fee_data['intraday_sec_trans_tax'], fee_data['intraday_sebi_turn_fees'], fee_data['intraday_auto_square_off_charges'], fee_data['intraday_depository_charges'] )
            elif trade_type == "Swing Trading":
                unique_fee_id = uuid5(app_namespace, str(trade['trade_entry_date']) + "Swing Trading")
                upsert_fee_entry_in_db(unique_fee_id, trade_date, fee_data['swing_net_obligation'], fee_data['swing_brokerage'], fee_data['swing_exc_trans_charges'], fee_data['swing_igst'], fee_data['swing_sec_trans_tax'], fee_data['swing_sebi_turn_fees'], fee_data['swing_auto_square_off_charges'], fee_data['swing_depository_charges'] )
        return jsonify({'message': 'Successfully Inserted Trade and Fee Entries into TRADES and FEE_COMPONENT Table','status': 'Success'})
    except Exception as e:
        return jsonify({'message': repr(e), 'status': 'Failed'})

@api.route('/api/realised_intraday_and_swing_stock_hist_returns/', methods = ['GET'])
def realised_stock_hist_returns_lookup():
    try:
        data = get_stock_hist_returns_from_realised_intraday_stock_and_swing_stock_hist_returns_table()
        data = data.get_json()
        return jsonify({'data': data,'message': 'Successfully retrieved from REALISED_INTRADAY_STOCK_HIST_RETURNS and REALISED_SWING_STOCK_HIST_RETURNS Tables','status': 'Success'})
    except Exception as e:
        return jsonify({'message': repr(e), 'status': 'Failed'})

@api.route('/api/process_realised_intraday_stock_hist_returns/', methods = ['GET'])
def process_realised_intraday_stock_hist_returns():
    try:
        truncate_realised_intraday_stock_hist_returns_table()
        create_realised_intraday_stock_hist_returns_table()
        
        insert_into_realised_intraday_stock_hist_returns()

        return jsonify({'message': 'Successfully inserted historic returns in to REALISED_INTRADAY_STOCK_HIST_RETURNS Table','status': 'Success'})
    except Exception as e:
        return jsonify({'message': repr(e), 'status': 'Failed'})

@api.route('/api/realised_intraday_stock_hist_returns/max_trade_date/', methods = ['GET'])
def get_max_trade_date_from_realised_stock_hist_returns():
    try:
        max_trade_date = get_max_trade_date_from_realised_intraday_stock_hist_returns_table()
        max_trade_date = max_trade_date.get_json()
        return jsonify({'data': max_trade_date,'message': 'Successfully retrieved Max Trade Date from REALISED_INTRADAY_STOCK_HIST_RETURNS Table','status': 'Success'})
    except Exception as e:
        return jsonify({'message': repr(e), 'status': 'Failed'})
    
@api.route('/api/realised_intraday_stock_hist_returns/<start_date>/<end_date>/', methods = ['GET'])
def insert_into_realised_stock_hist_returns_table(start_date, end_date):
    try:
        insert_into_realised_intraday_stock_hist_returns(start_date, end_date)
        return jsonify({'message': f'Successfully inserted historic returns in to REALISED_INTRADAY_STOCK_HIST_RETURNS Table','status': 'Success'})
    except Exception as e:
        return jsonify({'message': repr(e), 'status': 'Failed'})
    
@api.route('/api/trades/open/', methods = ['GET'])
def get_open_trades_list():
    try:
        open_trades_list = get_open_trades_from_trades_table()
        open_trades_list = open_trades_list.get_json()
        return jsonify({'data': open_trades_list,'message': 'Successfully retrieved Open Trades from TRADES Table','status': 'Success'})
    except Exception as e:
        return jsonify({'message': repr(e), 'status': 'Failed'})

@api.route('/api/close_trade/', methods = ['POST'])
def close_trade_entry():
    try:
        create_close_trades_table()

        close_trades_data_array = loads(request.form.get('close_trades_data_array')) # json.loads()
        for close_trade_data in close_trades_data_array:
            opening_trade_id = close_trade_data['opening_trade_id']
            opening_alt_symbol = close_trade_data['opening_alt_symbol']
            opening_trade_date = close_trade_data['opening_trade_date']
            opening_trade_stock_quantity = close_trade_data['opening_trade_stock_quantity']
            opening_trade_buy_or_sell = close_trade_data['opening_trade_buy_or_sell']
            closing_trade_id = close_trade_data['closing_trade_id']
            closing_alt_symbol = close_trade_data['closing_alt_symbol']
            closing_trade_date = close_trade_data['closing_trade_date']
            closing_trade_stock_quantity = close_trade_data['closing_trade_stock_quantity']
            closing_trade_buy_or_sell = close_trade_data['closing_trade_buy_or_sell']

            insert_into_close_trades_table(opening_trade_id, opening_alt_symbol, opening_trade_date, opening_trade_stock_quantity, opening_trade_buy_or_sell, closing_trade_id, closing_alt_symbol, closing_trade_date, closing_trade_stock_quantity, closing_trade_buy_or_sell)
        return jsonify({'message': 'Successfully inserted into CLOSE_TRADES Table','status': 'Success'})
    except Exception as e:
        return jsonify({'message': repr(e), 'status': 'Failed'})
    
@api.route('/api/process_realised_swing_stock_hist_returns/', methods = ['GET'])
def process_realised_swing_stock_hist_returns():
    try:
        truncate_realised_swing_stock_hist_returns_table()
        create_realised_swing_stock_hist_returns_table()
        
        insert_into_realised_swing_stock_hist_returns()
        return jsonify({'message': 'Successfully inserted historic Swing Stock returns into REALISED_SWING_STOCK_HIST_RETURNS Table','status': 'Success'})
    except Exception as e:
        return jsonify({'message': repr(e), 'status': 'Failed'})

@api.route('/api/realised_swing_stock_hist_returns/max_trade_close_date/', methods = ['GET'])
def get_max_trade_close_date_from_realised_swing_stock_hist_returns():
    try:
        max_trade_close_date = get_max_trade_close_date_from_realised_swing_stock_hist_returns_table()
        max_trade_close_date = max_trade_close_date.get_json()
        return jsonify({'data': max_trade_close_date,'message': 'Successfully retrieved Max Trade Close Date from REALISED_SWING_STOCK_HIST_RETURNS Table','status': 'Success'})
    except Exception as e:
        return jsonify({'message': repr(e), 'status': 'Failed'})
    
@api.route('/api/realised_swing_stock_hist_returns/<start_date>/<end_date>/', methods = ['GET'])
def insert_into_realised_swing_stock_hist_returns_table(start_date, end_date):
    try:
        insert_into_realised_swing_stock_hist_returns(start_date, end_date)
        return jsonify({'message': f'Successfully inserted historic returns in to REALISED_SWING_STOCK_HIST_RETURNS Table','status': 'Success'})
    except Exception as e:
        return jsonify({'message': repr(e), 'status': 'Failed'})

@api.route('/api/process_unrealised_stock_hist_returns/', methods = ['GET'])
def process_unrealised_swing_stock_hist_returns():
    try:
        truncate_unrealised_swing_stock_hist_returns_table()
        create_unrealised_swing_stock_hist_returns_table()
        
        first_swing_trade_data = get_first_swing_trade_date_from_trades_table()
        first_swing_trade_data = first_swing_trade_data.get_json()
        first_swing_trade_date = first_swing_trade_data['first_trade_date']

        counter_date = datetime.strptime(first_swing_trade_date,'%Y-%m-%d')
        first_swing_trade_date = datetime.strptime(first_swing_trade_date,'%Y-%m-%d')

        while(counter_date <= datetime.today() + timedelta(days = -2)):
            holiday_calendar_data = get_date_setup_from_holiday_calendar(counter_date.strftime('%Y-%m-%d'))
            holiday_calendar_data = holiday_calendar_data.get_json()
            processing_date = holiday_calendar_data[0]['processing_date']
            next_processing_date = holiday_calendar_data[0]['next_processing_date']
            prev_processing_date = holiday_calendar_data[0]['prev_processing_date']

            update_proc_date_in_processing_date_table('STOCK_PROC', processing_date, next_processing_date, prev_processing_date)

            hist_returns_data = get_metrics_from_fin_stock_swing_unrealised_portfolio_view()
            hist_returns_data = hist_returns_data.get_json()

            fin_invested_amount       = hist_returns_data[0]['fin_invested_amount']
            fin_total_fees            = hist_returns_data[0]['fin_total_fees']
            fin_total_invested_amount = hist_returns_data[0]['fin_total_invested_amount']
            fin_current_value         = hist_returns_data[0]['fin_current_value']
            fin_previous_value        = hist_returns_data[0]['fin_previous_value']
            fin_p_l                   = hist_returns_data[0]['fin_p_l']
            perc_fin_p_l              = hist_returns_data[0]['perc_fin_p_l']
            fin_net_p_l               = hist_returns_data[0]['fin_net_p_l']
            perc_fin_net_p_l          = hist_returns_data[0]['perc_fin_net_p_l']
            fin_day_p_l               = hist_returns_data[0]['fin_day_p_l']
            perc_fin_day_p_l          = hist_returns_data[0]['perc_fin_day_p_l']

            insert_into_unrealised_swing_stock_hist_returns(processing_date, next_processing_date, prev_processing_date, fin_invested_amount, fin_total_fees, fin_total_invested_amount, fin_current_value, fin_previous_value, fin_p_l, perc_fin_p_l, fin_net_p_l, perc_fin_net_p_l, fin_day_p_l, perc_fin_day_p_l)

            counter_date = datetime.strptime(next_processing_date,'%Y-%m-%d')

        return jsonify({'message': 'Successfully inserted historic returns in to UNREALISED_SWING_STOCK_HIST_RETURNS Table','status': 'Success'})
    except Exception as e:
        return jsonify({'message': repr(e), 'status': 'Failed'})
    
@api.route('/api/unrealised_stock_hist_returns/', methods = ['GET'])
def unrealised_stock_hist_returns_lookup():
    try:
        data = get_stock_hist_returns_from_unrealised_swing_stock_hist_returns_table()
        data = data.get_json()
        return jsonify({'data': data,'message': 'Successfully retrieved Historic Returns from UNREALISED_SWING_STOCK_HIST_RETURNS Table','status': 'Success'})
    except Exception as e:
        return jsonify({'message': repr(e), 'status': 'Failed'})

@api.route('/api/unrealised_swing_stock_hist_returns/max_next_proc_date/', methods = ['GET'])
def unrealised_hist_returns_max_date():
    try:
        max_date = get_max_next_proc_date_from_unrealised_swing_stock_hist_returns_table()
        max_date = max_date.get_json()
        return jsonify({'data': max_date,'message': 'Successfully retrieved Max Date from UNREALISED_SWING_STOCK_HIST_RETURNS Table','status': 'Success'})
    except Exception as e:
        return jsonify({'message': repr(e), 'status': 'Failed'})

@api.route('/api/unrealised_stock_hist_returns/<start_date>/<end_date>/', methods = ['GET'])
def process_unrealised_stock_hist_returns_from_start_to_end_date(start_date, end_date):
    try:
        start_date = datetime.strptime(start_date,'%Y-%m-%d')
        end_date = datetime.strptime(end_date,'%Y-%m-%d')
        counter_date = start_date
        log_date = []

        while(counter_date <= end_date):

            holiday_calendar_data = get_date_setup_from_holiday_calendar(counter_date.strftime('%Y-%m-%d'))
            holiday_calendar_data = holiday_calendar_data.get_json()
            processing_date = holiday_calendar_data[0]['processing_date']
            next_processing_date = holiday_calendar_data[0]['next_processing_date']
            prev_processing_date = holiday_calendar_data[0]['prev_processing_date']

            update_proc_date_in_processing_date_table('STOCK_PROC', processing_date, next_processing_date, prev_processing_date)

            hist_returns_data = get_metrics_from_fin_stock_swing_unrealised_portfolio_view()
            hist_returns_data = hist_returns_data.get_json()

            fin_invested_amount       = hist_returns_data[0]['fin_invested_amount']
            fin_total_fees            = hist_returns_data[0]['fin_total_fees']
            fin_total_invested_amount = hist_returns_data[0]['fin_total_invested_amount']
            fin_current_value         = hist_returns_data[0]['fin_current_value']
            fin_previous_value        = hist_returns_data[0]['fin_previous_value']
            fin_p_l                   = hist_returns_data[0]['fin_p_l']
            perc_fin_p_l              = hist_returns_data[0]['perc_fin_p_l']
            fin_net_p_l               = hist_returns_data[0]['fin_net_p_l']
            perc_fin_net_p_l          = hist_returns_data[0]['perc_fin_net_p_l']
            fin_day_p_l               = hist_returns_data[0]['fin_day_p_l']
            perc_fin_day_p_l          = hist_returns_data[0]['perc_fin_day_p_l']

            insert_into_unrealised_swing_stock_hist_returns(processing_date, next_processing_date, prev_processing_date, fin_invested_amount, fin_total_fees, fin_total_invested_amount, fin_current_value, fin_previous_value, fin_p_l, perc_fin_p_l, fin_net_p_l, perc_fin_net_p_l, fin_day_p_l, perc_fin_day_p_l)
            log_date.append(processing_date)
            counter_date = datetime.strptime(next_processing_date,'%Y-%m-%d')
        return jsonify({'message': f'Successfully inserted historic returns for {str(log_date)} in to UNREALISED_SWING_STOCK_HIST_RETURNS Table','status': 'Success'})
    except Exception as e:
        return jsonify({'message': repr(e), 'status': 'Failed'})

@api.route('/api/process_consolidated_hist_returns/', methods = ['GET'])
def process_consolidated_hist_returns():
    try:
        truncate_consolidated_hist_returns_table()
        create_consolidated_hist_returns_table()

        truncate_agg_consolidated_hist_returns_table()
        create_agg_consolidated_hist_returns_table()
        
        first_purchase_data_across_portfolio_type = get_first_purchase_date_from_all_portfolios()
        first_purchase_data_across_portfolio_type = first_purchase_data_across_portfolio_type.get_json()
        first_purchase_date = first_purchase_data_across_portfolio_type['first_purchase_date']

        counter_date = datetime.strptime(first_purchase_date,'%Y-%m-%d')
        first_purchase_date = datetime.strptime(first_purchase_date,'%Y-%m-%d')

        while(counter_date <= datetime.today() + timedelta(days = -2)):

            holiday_calendar_data = get_date_setup_from_holiday_calendar(counter_date.strftime('%Y-%m-%d'))
            holiday_calendar_data = holiday_calendar_data.get_json()
            processing_date = holiday_calendar_data[0]['processing_date']
            next_processing_date = holiday_calendar_data[0]['next_processing_date']
            prev_processing_date = holiday_calendar_data[0]['prev_processing_date']

            update_proc_date_in_processing_date_table('MF_PROC', processing_date, next_processing_date, prev_processing_date)
            update_proc_date_in_processing_date_table('STOCK_PROC', processing_date, next_processing_date, prev_processing_date)

            agg_hist_returns_data = get_metrics_from_agg_consolidated_portfolio_view()
            agg_hist_returns_data = agg_hist_returns_data.get_json()
            for agg_hist_returns_data_pf in agg_hist_returns_data:
                portfolio_type                = agg_hist_returns_data_pf['portfolio_type']
                agg_invested_amount           = agg_hist_returns_data_pf['agg_invested_amount']
                agg_current_value             = agg_hist_returns_data_pf['agg_current_value']
                agg_previous_value            = agg_hist_returns_data_pf['agg_previous_value']
                agg_total_p_l                 = agg_hist_returns_data_pf['agg_total_p_l']
                agg_day_p_l                   = agg_hist_returns_data_pf['agg_day_p_l']
                perc_agg_total_p_l            = agg_hist_returns_data_pf['perc_agg_total_p_l']
                perc_agg_day_p_l              = agg_hist_returns_data_pf['perc_agg_day_p_l']

                insert_into_agg_consolidated_hist_returns(portfolio_type, processing_date, prev_processing_date, next_processing_date, agg_invested_amount, agg_current_value, agg_previous_value, agg_total_p_l, agg_day_p_l, perc_agg_total_p_l, perc_agg_day_p_l)

            fin_hist_returns_data = get_metrics_from_fin_consolidated_portfolio_view()
            fin_hist_returns_data = fin_hist_returns_data.get_json()
            processing_date_from_fin      = fin_hist_returns_data[0]['processing_date']
            prev_processing_date_from_fin = fin_hist_returns_data[0]['prev_processing_date']
            next_processing_date_from_fin = fin_hist_returns_data[0]['next_processing_date']
            fin_invested_amount           = fin_hist_returns_data[0]['fin_invested_amount']
            fin_current_value             = fin_hist_returns_data[0]['fin_current_value']
            fin_previous_value            = fin_hist_returns_data[0]['fin_previous_value']
            fin_total_p_l                 = fin_hist_returns_data[0]['fin_total_p_l']
            fin_day_p_l                   = fin_hist_returns_data[0]['fin_day_p_l']
            perc_fin_total_p_l            = fin_hist_returns_data[0]['perc_fin_total_p_l']
            perc_fin_day_p_l              = fin_hist_returns_data[0]['perc_fin_day_p_l']

            insert_into_consolidated_hist_returns(processing_date_from_fin, prev_processing_date_from_fin, next_processing_date_from_fin, fin_invested_amount, fin_current_value, fin_previous_value, fin_total_p_l, fin_day_p_l, perc_fin_total_p_l, perc_fin_day_p_l)

            counter_date = datetime.strptime(next_processing_date,'%Y-%m-%d')

        return jsonify({'message': 'Successfully inserted historic returns in to AGG_CONSOLIDATED_HIST_RETURNS and CONSOLIDATED_HIST_RETURNS Table','status': 'Success'})
    except Exception as e:
        return jsonify({'message': repr(e), 'status': 'Failed'})

@api.route('/api/consolidated_hist_returns/', methods = ['GET'])
def consolidated_hist_returns_lookup():
    try:
        data = get_consolidated_hist_returns_from_consolidated_hist_returns_table()
        data = data.get_json()
        return jsonify({'data': data,'message': 'Successfully retrieved from CONSOLIDATED_HIST_RETURNS Table','status': 'Success'})
    except Exception as e:
        return jsonify({'message': repr(e), 'status': 'Failed'})

@api.route('/api/consolidated_hist_returns/max_next_proc_date/', methods = ['GET'])
def consolidated_hist_returns_max_proc_date():
    try:
        max_proc_date = get_max_next_proc_date_from_consolidated_hist_returns_table()
        max_proc_date = max_proc_date.get_json()
        return jsonify({'data': max_proc_date,'message': 'Successfully retrieved Max Processing Date from CONSOLIDATED_HIST_RETURNS Table','status': 'Success'})
    except Exception as e:
        return jsonify({'message': repr(e), 'status': 'Failed'})
    
@api.route('/api/hist_returns_tables/max_processing_date/', methods = ['GET'])
def get_max_processing_date_from_all_hist_returns_table():
    try:
        min_of_max_proc_date = get_max_proc_date_from_all_hist_tables()
        min_of_max_proc_date = min_of_max_proc_date.get_json()
        return jsonify({'max_proc_date_data':min_of_max_proc_date, 'message': "Successfully retrieved Maximum Processing Date data from all Historic Returns table", 'status': "Success"})
    except Exception as e:
        return jsonify({'message': repr(e), 'status': "Failed"})

@api.route('/api/consolidated_stock_hist_returns/<start_date>/<end_date>/', methods = ['GET'])
def process_consolidated_hist_returns_from_start_date_to_end_date(start_date, end_date):
    try:
        start_date = datetime.strptime(start_date,'%Y-%m-%d')
        end_date = datetime.strptime(end_date,'%Y-%m-%d')
        counter_date = start_date
        log_date = []

        while(counter_date <= end_date):

            holiday_calendar_data = get_date_setup_from_holiday_calendar(counter_date.strftime('%Y-%m-%d'))
            holiday_calendar_data = holiday_calendar_data.get_json()
            processing_date = holiday_calendar_data[0]['processing_date']
            next_processing_date = holiday_calendar_data[0]['next_processing_date']
            prev_processing_date = holiday_calendar_data[0]['prev_processing_date']

            update_proc_date_in_processing_date_table('MF_PROC', processing_date, next_processing_date, prev_processing_date)
            update_proc_date_in_processing_date_table('STOCK_PROC', processing_date, next_processing_date, prev_processing_date)

            agg_hist_returns_data = get_metrics_from_agg_consolidated_portfolio_view()
            agg_hist_returns_data = agg_hist_returns_data.get_json()
            for agg_hist_returns_data_pf in agg_hist_returns_data:
                portfolio_type                = agg_hist_returns_data_pf['portfolio_type']
                agg_invested_amount           = agg_hist_returns_data_pf['agg_invested_amount']
                agg_current_value             = agg_hist_returns_data_pf['agg_current_value']
                agg_previous_value            = agg_hist_returns_data_pf['agg_previous_value']
                agg_total_p_l                 = agg_hist_returns_data_pf['agg_total_p_l']
                agg_day_p_l                   = agg_hist_returns_data_pf['agg_day_p_l']
                perc_agg_total_p_l            = agg_hist_returns_data_pf['perc_agg_total_p_l']
                perc_agg_day_p_l              = agg_hist_returns_data_pf['perc_agg_day_p_l']

                insert_into_agg_consolidated_hist_returns(portfolio_type, processing_date, prev_processing_date, next_processing_date, agg_invested_amount, agg_current_value, agg_previous_value, agg_total_p_l, agg_day_p_l, perc_agg_total_p_l, perc_agg_day_p_l)

            fin_hist_returns_data = get_metrics_from_fin_consolidated_portfolio_view()
            fin_hist_returns_data = fin_hist_returns_data.get_json()
            processing_date_from_fin      = fin_hist_returns_data[0]['processing_date']
            prev_processing_date_from_fin = fin_hist_returns_data[0]['prev_processing_date']
            next_processing_date_from_fin = fin_hist_returns_data[0]['next_processing_date']
            fin_invested_amount           = fin_hist_returns_data[0]['fin_invested_amount']
            fin_current_value             = fin_hist_returns_data[0]['fin_current_value']
            fin_previous_value            = fin_hist_returns_data[0]['fin_previous_value']
            fin_total_p_l                 = fin_hist_returns_data[0]['fin_total_p_l']
            fin_day_p_l                   = fin_hist_returns_data[0]['fin_day_p_l']
            perc_fin_total_p_l            = fin_hist_returns_data[0]['perc_fin_total_p_l']
            perc_fin_day_p_l              = fin_hist_returns_data[0]['perc_fin_day_p_l']

            insert_into_consolidated_hist_returns(processing_date_from_fin, prev_processing_date_from_fin, next_processing_date_from_fin, fin_invested_amount, fin_current_value, fin_previous_value, fin_total_p_l, fin_day_p_l, perc_fin_total_p_l, perc_fin_day_p_l)
            log_date.append(processing_date)
            counter_date = datetime.strptime(next_processing_date,'%Y-%m-%d')

        return jsonify({'message': f'Successfully inserted historic returns for {str(log_date)} in to CONSOLIDATED_HIST_RETURNS Table','status': 'Success'})
    except Exception as e:
        return jsonify({'message': repr(e), 'status': 'Failed'})

@api.route('/api/consolidated_hist_returns/all/', methods = ['GET'])
def consolidated_hist_returns_fetch_all():
    try:
        data = get_all_from_consolidated_hist_returns_table()
        data = data.get_json()
        return jsonify({'data': data,'message': 'Successfully retrieved from CONSOLIDATED_HIST_RETURNS and AGG_CONSOLIDATED_RETURNS Table','status': 'Success'})
    except Exception as e:
        return jsonify({'message': repr(e), 'status': 'Failed'})
    
@api.route('/api/consolidated_hist_allocation/max_next_proc_date/', methods = ['GET'])
def consolidated_hist_allocation_max_next_proc_date():
    try:
        max_proc_date = get_max_next_proc_date_from_consolidated_hist_allocation_table()
        max_proc_date = max_proc_date.get_json()
        return jsonify({'data': max_proc_date,'message': 'Successfully retrieved Max Processing Date from CONSOLIDATED_ALLOCATION_RETURNS Table','status': 'Success'})
    except Exception as e:
        return jsonify({'message': repr(e), 'status': 'Failed'})

@api.route('/api/process_consolidated_hist_allocation/', methods = ['GET'])
def process_consolidated_hist_allocation():
    try:
        start_date = request.args.get('start_date') or None
        end_date = request.args.get('end_date') or None
        if not start_date and not end_date:
            truncate_consolidated_hist_allocation_table()
            truncate_consolidated_hist_allocation_portfolio_table()
            truncate_agg_consolidated_hist_allocation_table()

        create_consolidated_hist_allocation_table()
        create_consolidated_hist_allocation_portfolio_table()
        create_agg_consolidated_hist_allocation_table()

        if not start_date:
            first_purchase_data_across_portfolio_type = get_first_purchase_date_from_all_portfolios()
            first_purchase_data_across_portfolio_type = first_purchase_data_across_portfolio_type.get_json()
            start_date = first_purchase_data_across_portfolio_type['first_purchase_date']

        if end_date:
            end_date = datetime.strptime(end_date,'%Y-%m-%d')
        else:
            end_date = datetime.today() + timedelta(days = -5)

        counter_date = datetime.strptime(start_date,'%Y-%m-%d')

        while(counter_date <= end_date):

            holiday_calendar_data = get_date_setup_from_holiday_calendar(counter_date.strftime('%Y-%m-%d'))
            holiday_calendar_data = holiday_calendar_data.get_json()
            processing_date = holiday_calendar_data[0]['processing_date']
            next_processing_date = holiday_calendar_data[0]['next_processing_date']
            prev_processing_date = holiday_calendar_data[0]['prev_processing_date']

            update_proc_date_in_processing_date_table('MF_PROC', processing_date, next_processing_date, prev_processing_date)
            update_proc_date_in_processing_date_table('STOCK_PROC', processing_date, next_processing_date, prev_processing_date)

            get_metrics_from_agg_consolidated_allocation_view_and_insert_into_agg_consolidated_allocation_table()
            get_metrics_from_fin_consolidated_allocation_view_and_insert_into_fin_consolidated_allocation_table()
            get_metrics_from_fin_consolidated_allocation_portfolio_view_and_insert_into_fin_consolidated_allocation_portfolio_table()

            counter_date = datetime.strptime(next_processing_date,'%Y-%m-%d')

        return jsonify({'message': 'Successfully inserted historic Allocation in to AGG_CONSOLIDATED_HIST_ALLOCATION, CONSOLIDATED_HIST_ALLOCATION and CONSOLIDATED_HIST_ALLOCATION_PORTFOLIO Table','status': 'Success'})
    except Exception as e:
        return jsonify({'message': repr(e), 'status': 'Failed'})

@api.route('/api/consolidated_hist_allocation_portfolio/', methods = ['GET'])
def consolidated_hist_allocation_portfolio_lookup():
    try:
        data = get_consolidated_hist_allocation_portfolio_from_consolidated_hist_allocation_portfolio_table()
        data = data.get_json()
        return jsonify({'data': data,'message': 'Successfully retrieved Historic Allocation from CONSOLIDATED_HIST_ALLOCATION_PORTFOLIO Table','status': 'Success'})
    except Exception as e:
        return jsonify({'message': repr(e), 'status': 'Failed'})

@api.route('/api/consolidated_hist_allocation/all/', methods = ['GET'])
def consolidated_hist_allocation_fetch_all():
    try:
        data = get_all_from_consolidated_hist_allocation_table()
        data = data.get_json()
        return jsonify({'data': data,'message': 'Successfully retrieved from CONSOLIDATED_HIST_ALLOCATION, CONSOLIDATED_HIST_ALLOCATION_PORTFOLIO and AGG_CONSOLIDATED_ALLOCATION Table','status': 'Success'})
    except Exception as e:
        return jsonify({'message': repr(e), 'status': 'Failed'})