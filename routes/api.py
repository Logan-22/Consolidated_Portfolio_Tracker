from flask import Blueprint, jsonify, request
import yfinance as yf
from dateutil import parser
from datetime import datetime, timedelta

from utils.sql_utils.sql_utils import create_table, truncate_table, insert_into_nav_table, create_metadata_table, insert_metadata_entry, get_name_from_metadata, get_symbol_from_metadata, get_nav_from_hist_table, create_mf_order_table, insert_mf_order_entry, get_proc_date_from_processing_date_table, update_proc_date_in_processing_date_table, create_processing_date_table, get_tables_list, get_max_date_from_table, dup_check_on_nav_table, create_mf_portfolio_view_in_db, create_holiday_date_table, insert_into_holiday_date_table, get_holiday_date_from_holiday_date_table, truncate_holiday_calendar_table, create_holiday_calendar_table, insert_into_holiday_calendar_table, create_working_date_table, insert_into_working_date_table, get_working_date_from_holiday_date_table, get_first_purchase_date_from_mf_order_date_table, truncate_hist_returns_table , create_hist_returns_table, get_date_setup_from_holiday_calendar, get_metrics_from_fin_mutual_fund_portfolio_view, insert_into_mf_hist_returns, get_mf_hist_returns_from_mf_hist_returns_table, get_max_next_proc_date_from_mf_hist_returns_table
from utils.date_utils.date_utils import convert_weekday_from_int_to_char

api = Blueprint('api', __name__)

@api.route('/api/nav_tables/max_date/', methods = ['GET'])
def max_date_in_nav_tables():
    table_list = get_tables_list()
    table_list = table_list.get_json()
    max_date_from_tables = {}
    for table in table_list:
            table = str(table).replace('[','').replace(']','')
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
    amc = request.form.get('amc')
    mf_type = request.form.get('type')
    fund_category = request.form.get('fund_category')
    launched_on = request.form.get('launched_on')
    exit_load = request.form.get('exit_load')
    expense_ratio = request.form.get('expense_ratio')
    fund_manager = request.form.get('fund_manager')
    fund_manager_started_on = request.form.get('fund_manager_started_on')
    try:
        create_metadata_table()
        insert_metadata_entry(symbol, alt_symbol, amc, mf_type, fund_category, launched_on, exit_load, expense_ratio, fund_manager, fund_manager_started_on)
        return jsonify({'message': "Successfully inserted data into Metadata table", 'status': "Success"})
    except Exception as e:
        return jsonify({'message': repr(e), 'status': "Failed"})

@api.route('/api/name_list/', methods = ['GET'])
def name_list():
    data = get_name_from_metadata()
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
        table = str(table).replace("[","").replace("]","").replace("'","")
        dup_check_response = dup_check_on_nav_table(table)
        if dup_check_response:
            dup_check_response = dup_check_response.get_json()
            dup_tables[table] = dup_check_response['value_date']
    if dup_tables:
        return jsonify({'dup_tables': dup_tables,'message': 'Successfully completed Duplicate Check On All NAV table','status': 'Duplicate Issue'})
    else:
        return ({'message': 'Successfully completed Duplicate Check On All NAV tables','status': 'Success'})
    
@api.route('/api/create_mf_portfolio_view/', methods = ['GET'])
def create_mf_portfolio_view():
    try:
        create_mf_portfolio_view_in_db()
        return jsonify({'message': 'Successfully replaced MF Portfolio View in DB','status': 'Success'})
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
