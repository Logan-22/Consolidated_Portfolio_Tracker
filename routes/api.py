from flask import Blueprint, jsonify, request
from dateutil import parser
from datetime import datetime, timedelta, date
import os
from json import loads
from uuid import NAMESPACE_URL, uuid5

from utils.sql_utils.process.execute_process_group import execute_process_group_using_metadata
from utils.sql_utils.process.duplicate_check import duplicate_check_on_managed_tables

# from utils.sql_utils.tables.create_metadata_tables import\
# create_metadata_store_table,\
# create_processing_date_table,\
# create_processing_type_table,\
# create_metadata_process_group_table,\
# create_metadata_process_table,\
# create_metadata_key_columns_table,\
# create_execution_logs_table,\
# create_holiday_date_table,\
# create_working_date_table,\
# create_holiday_calendar_table,\
# create_duplicate_logs_table

# from utils.sql_utils.tables.create_metrics_tables import\
# create_price_table,\
# create_mutual_fund_returns_table,\
# create_agg_mutual_fund_returns_table,\
# create_fin_mutual_fund_returns_table,\
# create_realised_intraday_stock_returns_table,\
# create_agg_realised_intraday_stock_returns_table,\
# create_fin_realised_intraday_stock_returns_table,\
# create_realised_swing_stock_returns_table,\
# create_agg_realised_swing_stock_returns_table,\
# create_fin_realised_swing_stock_returns_table,\
# create_unrealised_stock_returns_table,\
# create_agg_unrealised_stock_returns_table,\
# create_fin_unrealised_stock_returns_table,\
# create_consolidated_returns_table,\
# create_agg_consolidated_returns_table,\
# create_fin_consolidated_returns_table,\
# create_consolidated_allocation_table,\
# create_agg_consolidated_allocation_table,\
# create_fin_consolidated_allocation_table,\
# create_simulated_portfolio_table,\
# create_agg_simulated_portfolio_table,\
# create_fin_simulated_portfolio_table

# from utils.sql_utils.tables.create_user_data_tables import\
# create_mf_order_table,\
# create_trade_table,\
# create_fee_table,\
# create_close_trades_table

from utils.sql_utils.views.create_views import\
create_mf_portfolio_views_in_db,\
create_stock_portfolio_views_in_db,\
create_consolidated_portfolio_views_in_db,\
create_simulated_portfolio_views_in_db

from utils.sql_utils.query_db.get_or_process_in_db import\
get_proc_date_from_processing_date_table,\
get_max_value_date_for_alt_symbol,\
get_holiday_dates,\
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
get_missing_prices_from_price_table,\
get_from_sqlite_component

from utils.sql_utils.query_db.delete_in_db import\
truncate_table

from utils.sql_utils.query_db.insert_in_db import\
insert_into_table

from utils.date_utils.date_utils import convert_weekday_from_int_to_char

# Folders

from utils.folder_utils.paths import upload_folder_path

from utils.sql_utils.tables.p1t_meta import\
create_metadata_schema,\
create_metadata_instruments_table,\
create_metadata_process_group_table,\
create_metadata_process_table,\
create_metadata_columns_table,\
create_holiday_date_table,\
create_working_date_table,\
create_holiday_calendar_table

from utils.sql_utils.tables.p1t_util import\
create_utility_schema,\
create_processing_date_table,\
create_processing_type_table,\
create_user_processing_date_table

from utils.sql_utils.tables.p1t_auth import\
create_authorization_schema,\
create_users_table,\
create_user_profile_table,\
create_user_security_table,\
create_user_sessions_table,\
create_password_resets_table

from utils.sql_utils.tables.p1t_log import\
create_log_schema,\
create_execution_logs_table,\
create_duplicate_logs_table,\
create_auth_audit_table

from utils.sql_utils.tables.p1t_usr_txn import\
create_user_transaction_schema,\
create_mf_transaction_table

from utils.sql_utils.tables.p1t_tier0_metrics import\
create_tier0_metrics_schema,\
create_daily_instrument_prices_table,\
create_mf_depository_holding_table

from utils.sql_utils.tables.p1t_tier1_metrics import\
create_tier1_metrics_schema,\
create_tier1_mf_pf_table

from utils.sql_utils.tables.p1t_tier2_metrics import\
create_tier2_metrics_schema,\
create_tier2_agg_mf_pf_table

from utils.sql_utils.tables.p1t_tier3_metrics import\
create_tier3_metrics_schema,\
create_tier3_fin_mf_pf_table

from utils.sql_utils.views.p1v_tier0_inp import\
create_tier0_inp_view_schema,\
create_tier0_depository_holding_view

from utils.sql_utils.views.p1v_tier1_inp import\
create_tier1_inp_view_schema,\
create_tier1_mf_portfolio_view

from utils.sql_utils.views.p1v_tier2_inp import\
create_tier2_inp_view_schema,\
create_tier2_agg_mf_portfolio_view

from utils.sql_utils.views.p1v_tier3_inp import\
create_tier3_inp_view_schema,\
create_tier3_fin_mf_portfolio_view

api = Blueprint('api', __name__)

env = os.getenv('ENVIRONMENT')

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
        #update_proc_date_in_processing_date_table('MF_PROC',         processing_date_payload['mf_proc_date'],        processing_date_payload['mf_next_proc_date'],        processing_date_payload['mf_prev_proc_date'])
        #update_proc_date_in_processing_date_table('PPF_MF_PROC',     processing_date_payload['ppf_mf_proc_date'],    processing_date_payload['ppf_mf_next_proc_date'],    processing_date_payload['ppf_mf_prev_proc_date'])
        #update_proc_date_in_processing_date_table('STOCK_PROC',      processing_date_payload['stock_proc_date'],     processing_date_payload['stock_next_proc_date'],     processing_date_payload['stock_prev_proc_date'])
        #update_proc_date_in_processing_date_table('SIM_MF_PROC',     processing_date_payload['sim_mf_proc_date'],    processing_date_payload['sim_mf_next_proc_date'],    processing_date_payload['sim_mf_prev_proc_date'])
        #update_proc_date_in_processing_date_table('SIM_STOCK_PROC',  processing_date_payload['sim_stock_proc_date'], processing_date_payload['sim_stock_next_proc_date'], processing_date_payload['sim_stock_prev_proc_date'])
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
        return jsonify({'message': 'Successfully created Managed Folders in Directory','status': 'Success'})
    except Exception as e:
        return jsonify({'message': repr(e), 'status': 'Failed'}) 

@api.route('/api/create_managed_tables/', methods = ['GET'])
def create_managed_tables():
    try:
        pass
        return jsonify({'message': 'Successfully created Managed Tables in DB','status': 'Success'})
    except Exception as e:
        return jsonify({'message': repr(e), 'status': 'Failed'})

@api.route('/api/process_mf_returns/', methods = ['GET'])
def process_mf_returns():
    try:
        start_date = request.args.get('start_date') or None
        end_date   = request.args.get('end_date') or None
        on_start   = request.args.get('on_start') or None
        
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
    
@api.route('/api/stock_pdf/', methods = ['PUT'])
def upsert_trade_entry():
    try:
        trades_array = loads(request.form.get('trade_data')) # json.loads()
        fee_data = loads(request.form.get('fee_data'))
        unique_fee_id = ""
        trade_date = ""

        trades_payloads = []
        fee_payloads    = []

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

@api.route('/api/component_info/', methods = ['GET'])
def get_component_info():
    try:
        component_type = request.args.get("component_type") or None
        component_info = get_component_info_from_db(component_type = component_type)
        return jsonify({'component_info': component_info, 'message': f'Successfully retrieved {component_type if component_type else "All Components"} Info', 'status': 'Success'})
    except Exception as e:
        return jsonify({'message': repr(e), 'status': "Failed"})

@api.route('/api/create_metadata_tables/', methods = ['GET'])
def create_metadata_tables():
    try:
        metadata_schema = request.args.get("metadata_schema") or f"{env}T_META"
        create_metadata_schema(metadata_schema)
        create_metadata_instruments_table(metadata_schema)
        create_metadata_process_group_table(metadata_schema)
        create_metadata_process_table(metadata_schema)
        create_metadata_columns_table(metadata_schema)
        create_execution_logs_table(metadata_schema)
        create_holiday_date_table(metadata_schema)
        create_working_date_table(metadata_schema)
        create_holiday_calendar_table(metadata_schema)
        create_duplicate_logs_table(metadata_schema)
        return jsonify({'message': f'Successfully Created {metadata_schema} Metadata Schema and Metadata Tables', 'status': 'Success'})
    except Exception as e:
        return jsonify({'message': repr(e), 'status': "Failed"})

@api.route('/api/create_utility_tables/', methods = ['GET'])
def create_utlity_tables():
    try:
        utility_schema = request.args.get("utility_schema") or f"{env}T_UTIL"
        create_utility_schema(utility_schema)
        create_processing_date_table(utility_schema)
        create_processing_type_table(utility_schema)
        create_user_processing_date_table(utility_schema)
        return jsonify({'message': f'Successfully Created {utility_schema} Utility Schema and Utility Tables', 'status': 'Success'})
    except Exception as e:
        return jsonify({'message': repr(e), 'status': "Failed"})

@api.route('/api/create_auth_tables/', methods = ['GET'])
def create_auth_tables():
    try:
        auth_schema = request.args.get("auth_schema") or f"{env}T_AUTH"
        create_authorization_schema(auth_schema)
        create_users_table(auth_schema)
        create_user_profile_table(auth_schema)
        create_user_security_table(auth_schema)
        create_user_sessions_table(auth_schema)
        create_password_resets_table(auth_schema)
        return jsonify({'message': f'Successfully Created {auth_schema} Authorization Schema and Authorization Tables', 'status': 'Success'})
    except Exception as e:
        return jsonify({'message': repr(e), 'status': "Failed"})

@api.route('/api/create_log_tables/', methods = ['GET'])
def create_log_tables():
    try:
        log_schema = request.args.get("log_schema") or f"{env}T_LOG"
        create_log_schema(log_schema)
        create_execution_logs_table(log_schema)
        create_duplicate_logs_table(log_schema)
        create_auth_audit_table(log_schema)
        return jsonify({'message': f'Successfully Created {log_schema} Logging Schema and Logging Tables', 'status': 'Success'})
    except Exception as e:
        return jsonify({'message': repr(e), 'status': "Failed"})

@api.route('/api/create_tier0_metrics_tables/', methods = ['GET'])
def create_tier0_metrics_tables():
    try:
        tier0_metrics_schema = request.args.get("tier0_metrics_schema") or f"{env}T_TIER0_METRICS"
        create_tier0_metrics_schema(tier0_metrics_schema)
        create_daily_instrument_prices_table(tier0_metrics_schema)
        create_mf_depository_holding_table(tier0_metrics_schema)
        return jsonify({'message': f'Successfully Created {tier0_metrics_schema} Tier0 Metrics Schema and Tables', 'status': 'Success'})
    except Exception as e:
        return jsonify({'message': repr(e), 'status': "Failed"})

@api.route('/api/create_tier1_metrics_tables/', methods = ['GET'])
def create_tier1_metrics_tables():
    try:
        tier1_metrics_schema = request.args.get("tier1_metrics_schema") or f"{env}T_TIER1_METRICS"
        create_tier1_metrics_schema(tier1_metrics_schema)
        create_tier1_mf_pf_table(tier1_metrics_schema)
        return jsonify({'message': f'Successfully Created {tier1_metrics_schema} Tier1 Metrics Schema and Tables', 'status': 'Success'})
    except Exception as e:
        return jsonify({'message': repr(e), 'status': "Failed"})

@api.route('/api/create_tier2_metrics_tables/', methods = ['GET'])
def create_tier2_metrics_tables():
    try:
        tier2_metrics_schema = request.args.get("tier2_metrics_schema") or f"{env}T_TIER2_METRICS"
        create_tier2_metrics_schema(tier2_metrics_schema)
        create_tier2_agg_mf_pf_table(tier2_metrics_schema)
        return jsonify({'message': f'Successfully Created {tier2_metrics_schema} Tier2 Metrics Schema and Tables', 'status': 'Success'})
    except Exception as e:
        return jsonify({'message': repr(e), 'status': "Failed"})

@api.route('/api/create_tier3_metrics_tables/', methods = ['GET'])
def create_tier3_metrics_tables():
    try:
        tier3_metrics_schema = request.args.get("tier3_metrics_schema") or f"{env}T_TIER3_METRICS"
        create_tier3_metrics_schema(tier3_metrics_schema)
        create_tier3_fin_mf_pf_table(tier3_metrics_schema)
        return jsonify({'message': f'Successfully Created {tier3_metrics_schema} Tier3 Metrics Schema and Tables', 'status': 'Success'})
    except Exception as e:
        return jsonify({'message': repr(e), 'status': "Failed"})

@api.route('/api/create_txn_tables/', methods = ['GET'])
def create_txn_tables():
    try:
        txn_schema = request.args.get("txn_schema") or f"{env}T_USR_TXN"
        create_user_transaction_schema(txn_schema)
        create_mf_transaction_table(txn_schema)
        return jsonify({'message': f'Successfully Created {txn_schema} User Transaction Schema and Tables', 'status': 'Success'})
    except Exception as e:
        return jsonify({'message': repr(e), 'status': "Failed"})

@api.route('/api/create_tier0_inp_view/', methods = ['GET'])
def create_tier0_inp_view():
    try:
        tier0_inp_view_schema = request.args.get("tier0_inp_view_schema") or f"{env}V_TIER0_INP"
        create_tier0_inp_view_schema(tier0_inp_view_schema)
        create_tier0_depository_holding_view(tier0_inp_view_schema)
        return jsonify({'message': f'Successfully Created {tier0_inp_view_schema} Tier0 Input Schema and Views', 'status': 'Success'})
    except Exception as e:
        return jsonify({'message': repr(e), 'status': "Failed"})

@api.route('/api/create_tier1_inp_view/', methods = ['GET'])
def create_tier1_inp_view():
    try:
        tier1_inp_view_schema = request.args.get("tier1_inp_view_schema") or f"{env}V_TIER1_INP"
        create_tier1_inp_view_schema(tier1_inp_view_schema)
        create_tier1_mf_portfolio_view(tier1_inp_view_schema)
        return jsonify({'message': f'Successfully Created {tier1_inp_view_schema} Tier1 Input Schema and Views', 'status': 'Success'})
    except Exception as e:
        return jsonify({'message': repr(e), 'status': "Failed"})

@api.route('/api/create_tier2_inp_view/', methods = ['GET'])
def create_tier2_inp_view():
    try:
        tier2_inp_view_schema = request.args.get("tier2_inp_view_schema") or f"{env}V_TIER2_INP"
        create_tier2_inp_view_schema(tier2_inp_view_schema)
        create_tier2_agg_mf_portfolio_view(tier2_inp_view_schema)
        return jsonify({'message': f'Successfully Created {tier2_inp_view_schema} Tier2 Input Schema and Views', 'status': 'Success'})
    except Exception as e:
        return jsonify({'message': repr(e), 'status': "Failed"})

@api.route('/api/create_tier3_inp_view/', methods = ['GET'])
def create_tier3_inp_view():
    try:
        tier3_inp_view_schema = request.args.get("tier3_inp_view_schema") or f"{env}V_TIER3_INP"
        create_tier3_inp_view_schema(tier3_inp_view_schema)
        create_tier3_fin_mf_portfolio_view(tier3_inp_view_schema)
        return jsonify({'message': f'Successfully Created {tier3_inp_view_schema} Tier3 Input Schema and Views', 'status': 'Success'})
    except Exception as e:
        return jsonify({'message': repr(e), 'status': "Failed"})

@api.route('/api/migrate_data_to_aws/', methods = ['GET'])
def migrate_data_from_sqlite3_to_aws():
    try:
        schema = request.args.get('schema') or None
        sqlite_table_name = request.args.get('sqlite_table_name') or None
        aws_table_name = request.args.get('aws_table_name') or None
        truncate_table(schema, aws_table_name)
        table_payload = get_from_sqlite_component(sqlite_table_name)
        inserted_count = insert_into_table(schema, aws_table_name, table_payload)
        return jsonify({'message': f'Successfully Migrated {inserted_count} records from SQLITE3 to AWS', 'status': 'Success'})
    except Exception as e:
        return jsonify({'message': repr(e), 'status': "Failed"})

