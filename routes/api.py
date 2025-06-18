from flask import Blueprint, jsonify, request
import yfinance as yf
from dateutil import parser
from datetime import datetime, timedelta
import os
from json import loads
from uuid import NAMESPACE_URL,uuid5

from utils.sql_utils.sql_utils import create_table, truncate_table, insert_into_nav_table, create_metadata_table, insert_metadata_entry, get_name_from_metadata, get_symbol_from_metadata, get_nav_from_hist_table, create_mf_order_table, insert_mf_order_entry, get_proc_date_from_processing_date_table, update_proc_date_in_processing_date_table, create_processing_date_table, get_tables_list, get_max_date_from_table, dup_check_on_nav_table, create_mf_portfolio_view_in_db, create_holiday_date_table, insert_into_holiday_date_table, get_holiday_date_from_holiday_date_table, truncate_holiday_calendar_table, create_holiday_calendar_table, insert_into_holiday_calendar_table, create_working_date_table, insert_into_working_date_table, get_working_date_from_holiday_date_table, get_first_purchase_date_from_mf_order_date_table, truncate_hist_returns_table , create_hist_returns_table, get_date_setup_from_holiday_calendar, get_metrics_from_fin_mutual_fund_portfolio_view, insert_into_mf_hist_returns, get_mf_hist_returns_from_mf_hist_returns_table, get_max_next_proc_date_from_mf_hist_returns_table, create_stock_order_table, insert_stock_order_entry, get_all_names_from_metadata, upsert_trade_entry_in_db, create_trade_table, create_fee_table, upsert_fee_entry_in_db, get_all_tables_list, create_stock_portfolio_view_in_db
from utils.date_utils.date_utils import convert_weekday_from_int_to_char

# Folders

from utils.folder_utils.paths import upload_folder_path
from werkzeug.utils import secure_filename
from PyPDF2 import PdfReader, PdfWriter

api = Blueprint('api', __name__)

app_namespace = uuid5(NAMESPACE_URL,"Consolidated_Portfolio_Tracker") # Change Later

@api.route('/api/price_tables/max_date/', methods = ['GET'])
def max_date_in_price_tables():
    table_list = get_tables_list()
    table_list = table_list.get_json()
    max_date_from_tables = {}
    for table in table_list:
            table = str(table).replace('[','').replace(']','').replace(" ","_").replace("'","")
            max_date = get_max_date_from_table(table)
            max_date = max_date.get_json()
            max_date_from_tables[table] = max_date['max_date']
    return jsonify({'max_date_from_tables':max_date_from_tables, 'message': "Successfully retrieved MAX Date data from NAV Tables", 'status': "Success"})

@api.route('/api/all_price_tables/max_date/', methods = ['GET'])
def max_date_in_all_price_tables():
    table_list = get_all_tables_list()
    table_list = table_list.get_json()
    max_date_from_tables = {}
    for table in table_list:
            table = str(table).replace('[','').replace(']','').replace(" ","_").replace("'","")
            max_date = get_max_date_from_table(table)
            max_date = max_date.get_json()
            max_date_from_tables[table] = max_date['max_date']
    return jsonify({'max_date_from_tables':max_date_from_tables, 'message': "Successfully retrieved MAX Date data from NAV Tables", 'status': "Success"})

@api.route('/api/hist_price/<symbol>/', methods = ['POST'])
def hist_price(symbol):
    alt_symbol = request.form.get('alt_symbol')
    alt_symbol = alt_symbol.replace(" ", "_")
    truncate_table(alt_symbol)
    create_table(alt_symbol)
    end_date = request.form.get('end_date')
    start_date = request.form.get('start_date')
    ticker = yf.Ticker(symbol)
    pandas_data = ticker.history(start=start_date, end=end_date)
    for index, value in pandas_data['Close'].items():
        date = str(index)[:10]
        if (parser.parse(date, fuzzy = 'fuzzy')):
            insert_into_nav_table(alt_symbol, date, round(value,4), date, '9998-12-31', 0)
    return jsonify({'message': "Successfully inserted data into " + alt_symbol + " table", 'status': "Success"})


@api.route('/api/metadata/', methods = ['POST'])
def metadata_entry():
    symbol = request.form.get('symbol')
    alt_symbol = request.form.get('alt_symbol')
    portfolio_type = request.form.get('portfolio_type')
    amc = request.form.get('amc')
    mf_type = request.form.get('type')
    fund_category = request.form.get('fund_category')
    launched_on = request.form.get('launched_on')
    exit_load = request.form.get('exit_load')
    expense_ratio = request.form.get('expense_ratio')
    fund_manager = request.form.get('fund_manager')
    fund_manager_started_on = request.form.get('fund_manager_started_on')
    isin = request.form.get('isin')
    try:
        create_metadata_table()
        insert_metadata_entry(symbol, alt_symbol, portfolio_type, amc, mf_type, fund_category, launched_on, exit_load, expense_ratio, fund_manager, fund_manager_started_on, isin)
        return jsonify({'message': "Successfully inserted data into Metadata table", 'status': "Success"})
    except Exception as e:
        return jsonify({'message': repr(e), 'status': "Failed"})
    
@api.route('/api/name_list/', methods = ['GET'])
def all_name_list():
    data = get_all_names_from_metadata()
    data = data.get_json()
    name_list = []
    for item in data:
        name_list.append(item['name'])
    return jsonify({'name_list': name_list, 'message': "Successfully retrieved data from Metadata Table", 'status': "Success"})

@api.route('/api/mf_name_list/', methods = ['GET'])
def mf_name_list():
    data = get_name_from_metadata('Mutual Fund')
    data = data.get_json()
    name_list = []
    for item in data:
        name_list.append(item['name'])
    return jsonify({'name_list': name_list, 'message': "Successfully retrieved data from Metadata Table", 'status': "Success"})

@api.route('/api/stock_name_list/', methods = ['GET'])
def stock_name_list():
    data = get_name_from_metadata('Stock')
    data = data.get_json()
    name_list = []
    for item in data:
        name_list.append(item['name'])
    return jsonify({'name_list': name_list, 'message': "Successfully retrieved data from Metadata Table", 'status': "Success"})


@api.route('/api/symbol/<alt_symbol>/', methods = ['GET'])
def symbol_lookup(alt_symbol):
    data = get_symbol_from_metadata(alt_symbol)
    data = data.get_json()
    symbol_list = []
    for item in data:
        symbol_list.append(item['symbol'])
    return jsonify({'symbol_list': symbol_list, 'message': "Successfully retrieved data from Metadata Table", 'status': "Success"})

@api.route('/api/nav_lookup/<table_name>/<purchase_date>', methods = ['GET'])
def nav_lookup(table_name, purchase_date):
    data = get_nav_from_hist_table(table_name, purchase_date)
    data = data.get_json()
    nav = data['nav']
    return jsonify({'nav': nav, 'message': "Successfully retrieved data from Metadata Table", 'status': "Success"})

@api.route('/api/mf_order/', methods = ['POST'])
def mf_order():
    alt_symbol = request.form.get('alt_symbol')
    purchase_date = request.form.get('purchase_date')
    invested_amount = request.form.get('invested_amount')
    stamp_fees_amount = request.form.get('stamp_fees_amount')
    amc_amount = request.form.get('amc_amount')
    nav_during_purchase = request.form.get('nav_during_purchase')
    units = request.form.get('units')
    units = round(float(units), 3)
    stamp_fees_amount = round(float(invested_amount) - float(amc_amount),2)
    try:
        create_mf_order_table()
        insert_mf_order_entry(alt_symbol, purchase_date, invested_amount, stamp_fees_amount, amc_amount, nav_during_purchase, units)
        return jsonify({'message': "Successfully inserted data into MF Order table", 'status': "Success"})
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
                          'message': "Successfully retrieved data from Processing Date Table", 'status': "Success"})
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
        return jsonify({'message': "Successfully updated Processing Date Table", 'status': "Success"})
    except Exception as e:
        return jsonify({'message': repr(e), 'status': 'Failed'})

@api.route('/api/hist_price/<symbol>/<start_date>/<end_date>/', methods = ['POST'])
def date_range_hist_price(symbol,start_date, end_date):
    alt_symbol = request.form.get('alt_symbol')
    alt_symbol = alt_symbol.replace(" ", "_")
    ticker = yf.Ticker(symbol)
    pandas_data = ticker.history(start=start_date, end=end_date)
    for index, value in pandas_data['Close'].items():
        date = str(index)[:10]
        if (parser.parse(date, fuzzy = 'fuzzy')):
            insert_into_nav_table(alt_symbol, date, round(value,4), date, '9998-12-31', 0)
    return jsonify({'message': "Successfully inserted data into " + alt_symbol + " table", 'status': "Success"})

@api.route('/api/nav_tables/dup_check/', methods = ['GET'])
def dup_check_on_all_nav_tables():
    table_list = get_tables_list()
    table_list = table_list.get_json()
    dup_tables = {}
    for table in table_list:
        table = str(table).replace("[","").replace("]","").replace(" ","_").replace("'","")
        dup_check_response = dup_check_on_nav_table(table)
        if dup_check_response:
            dup_check_response = dup_check_response.get_json()
            dup_tables[table] = dup_check_response['value_date']
    if dup_tables:
        return jsonify({'dup_tables': dup_tables,'message': 'Successfully completed Duplicate Check On All NAV table','status': 'Duplicate Issue'})
    else:
        return ({'message': 'Successfully completed Duplicate Check On All NAV tables','status': 'Success'})
    
@api.route('/api/create_portfolio_view/', methods = ['GET'])
def create_portfolio_view():
    try:
        create_mf_portfolio_view_in_db()
        create_stock_portfolio_view_in_db()
        return jsonify({'message': 'Successfully replaced Portfolio Views in DB','status': 'Success'})
    except Exception as e:
        return jsonify({'message': repr(e), 'status': 'Failed'})
    
@api.route('/api/holiday_date/', methods = ['POST'])
def holiday_date_entry():
    try:
        create_holiday_date_table()

        holiday_date = request.form.get('holiday_date')
        holiday_name = request.form.get('holiday_name')
        holiday_day = request.form.get('holiday_day')
        insert_into_holiday_date_table(holiday_date, holiday_name, holiday_day)
        return jsonify({'message': 'Successfully inserted holiday into Holiday Dates Table','status': 'Success'})
    except Exception as e:
        return jsonify({'message': repr(e), 'status': 'Failed'})
    
@api.route('/api/holiday_date/', methods = ['GET'])
def holiday_date_lookup_all():
    try:
        data = get_holiday_date_from_holiday_date_table()
        data = data.get_json()
        return jsonify({'data': data,'message': 'Successfully retrieved from Holiday Dates Table','status': 'Success'})
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
        return jsonify({'message': 'Successfully inserted working date into Working Dates Table','status': 'Success'})
    except Exception as e:
        return jsonify({'message': repr(e), 'status': 'Failed'})
    
@api.route('/api/working_date/', methods = ['GET'])
def working_date_lookup():
    try:
        data = get_working_date_from_holiday_date_table()
        data = data.get_json()
        if data[0]['working_date']:
            return jsonify({'data': data,'message': 'Successfully retrieved from Working Dates Table','status': 'Success'})
        else:
            return jsonify({'data': [],'message': 'No records present in Working Dates Table','status': 'Success'})
    except Exception as e:
        return jsonify({'message': repr(e), 'status': 'Failed'})

@api.route('/api/holiday_date/<current_year>/', methods = ['GET'])
def holiday_date_lookup(current_year):
    try:
        data = get_holiday_date_from_holiday_date_table(current_year)
        data = data.get_json()
        return jsonify({'data': data,'message': 'Successfully retrieved from Holiday Dates Table','status': 'Success'})
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

        truncate_holiday_calendar_table()
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
        return jsonify({'message': 'Successfully inserted holiday calendar into Holiday Calendar Table','status': 'Success'})
    except Exception as e:
        return jsonify({'message': repr(e), 'status': 'Failed'})
    
@api.route('/api/process_hist_returns/', methods = ['GET'])
def process_hist_returns():
    try:
        truncate_hist_returns_table()
        create_hist_returns_table()
        
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
        return jsonify({'data': data,'message': 'Successfully retrieved from MF Hist Returns Table','status': 'Success'})
    except Exception as e:
        return jsonify({'message': repr(e), 'status': 'Failed'})
    
@api.route('/api/mf_hist_returns/max_next_proc_date/', methods = ['GET'])
def mf_hist_returns_max_date():
    try:
        max_date = get_max_next_proc_date_from_mf_hist_returns_table()
        max_date = max_date.get_json()
        return jsonify({'data': max_date,'message': 'Successfully retrieved Max Date from MF Hist Returns Table','status': 'Success'})
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

    try:
        create_stock_order_table()
        insert_stock_order_entry(alt_symbol, trade_entry_date, trade_entry_time, trade_exit_date, trade_exit_time, stock_quantity, trade_type, leverage, trade_position, stock_buy_price, stock_sell_price, brokerage, exchange_transaction_fees, igst, securities_transaction_tax, sebi_turnover_fees, holding_days, sell_minus_buy, actual_p_l_w_o_leverage, deployed_capital, net_obligation, total_fees, net_receivable, auto_square_off_charges, depository_charges, actual_p_l_w_leverage)
        return jsonify({'message': "Successfully inserted data into Stock Order table", 'status': "Success"})
    except Exception as e:
        return jsonify({'message': repr(e), 'status': "Failed"})
    
@api.route('/api/stock_pdf/', methods = ['POST'])
def stock_order_entry_from_pdf():
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

    try:
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
        #os.remove(stock_text_file_name_path)
        return jsonify({'data': trade_list, 'fees': fees_info, 'message': 'Successfully uploaded the Stock PDF.','status': 'Success'})
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
        return jsonify({'message': 'Successfully Inserted Trade and Fee Entries into DB','status': 'Success'})
    except Exception as e:
        return jsonify({'message': repr(e), 'status': 'Failed'})
