from flask import Blueprint, jsonify, request
import yfinance as yf
from dateutil import parser
from datetime import datetime

from utils.sql_utils.sql_utils import create_table, truncate_table, insert_into_nav_table, create_metadata_table, insert_metadata_entry, get_name_from_metadata, get_symbol_from_metadata, get_nav_from_hist_table, create_portfolio_order_table, insert_portfolio_order_entry, get_proc_date_from_processing_date_table, update_proc_date_in_processing_date_table, create_processing_date_table, get_tables_list, get_max_date_from_table, dup_check_on_nav_table, create_mf_portfolio_view_in_db

api = Blueprint('api', __name__)

@api.route('/api/max_date/', methods = ['GET'])
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

@api.route('/api/portfolio_order/', methods = ['POST'])
def portfolio_order():
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
        create_portfolio_order_table()
        insert_portfolio_order_entry(alt_symbol, purchase_date, invested_amount, stamp_fees_amount, amc_amount, nav_during_purchase, units)
        return jsonify({'message': "Successfully inserted data into Portfolio Order table", 'status': "Success"})
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

@api.route('/api/nav/dup_check/', methods = ['GET'])
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