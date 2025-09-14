from flask import Blueprint, jsonify, request, g
from decimal import Decimal
from datetime import datetime
from json import loads
from utils.sql_utils.process.execute_process_group import execute_process_group_using_metadata
from utils.auth_utils.auth_utils import require_login
from utils.thread_utils.thread_executor import submit_threaded_task, queued_tasks
from utils.sql_utils.query_db.get_or_process_in_db import\
get_or_create_instrument_id,\
get_holding_data,\
get_consolidated_quantity_from_mf_txn,\
get_prev_proc_date_from_holiday_calendar_table

user_transactions_bp = Blueprint('user_transactions', __name__)

@user_transactions_bp.route('/mf_txn/', methods = ['POST'])
@require_login
def mf_transaction_entry():
    try:
        mf_txn_payloads = loads(request.form.get('mf_txn_payload'))
        if g.user_id:
            user_id = g.user_id
        else:
            return jsonify({'message': 'Transaction Declined: Invalid User Request', 'status' : 'Failed'}), 403
        min_txn_date = datetime.strptime('9998-12-31','%Y-%m-%d')
        for payload in mf_txn_payloads:
            payload['USER_ID'] = user_id
            payload['INSTRUMENT_ID'] = get_or_create_instrument_id(payload['EXCHANGE_SYMBOL'], 'get')
            payload['UNITS'] = round(Decimal(payload['AMC_AMOUNT']) / Decimal(payload['NAV_DURING_PURCHASE']), 4)

            payload['STAMP_FEES_AMOUNT'] = abs(round(Decimal(payload['TXN_AMOUNT']) - Decimal(payload['AMC_AMOUNT']), 4))
            if Decimal(payload['STAMP_FEES_AMOUNT']) < 0 or Decimal(payload['AMC_AMOUNT']) < 0 or Decimal(payload['UNITS']) < 0:
                return jsonify({'message': 'Transaction Declined: Invalid AMC Amount or Units', 'status' : 'Failed'}), 422

            if payload['TXN_TYPE'] == 'Sell':
                holding_as_on_purchase_date = get_holding_data(payload['INSTRUMENT_ID'], payload['USER_ID'], payload['TXN_DATE'])
                if holding_as_on_purchase_date and holding_as_on_purchase_date.get('TOTAL_QUANTITY') and Decimal(holding_as_on_purchase_date['TOTAL_QUANTITY']) >= Decimal(payload['UNITS']):
                    get_consolidated_quantity = get_consolidated_quantity_from_mf_txn(payload['INSTRUMENT_ID'], payload['USER_ID'])
                    if not(get_consolidated_quantity and get_consolidated_quantity.get('CONSOLIDATED_QUANTITY') and Decimal(get_consolidated_quantity['CONSOLIDATED_QUANTITY']) >= Decimal(payload['UNITS'])):
                        return jsonify({'message': 'Transaction Declined: Inconsistent Sell Order due Insufficient Units Held', 'status' : 'Failed'}), 422
                else:
                    return jsonify({'message': 'Transaction Declined: No units available for this sell request', 'status' : 'Failed'}), 422
            if datetime.strptime(payload['TXN_DATE'],'%Y-%m-%d') < min_txn_date:
                min_txn_date = datetime.strptime(payload['TXN_DATE'],'%Y-%m-%d')
        start_date = get_prev_proc_date_from_holiday_calendar_table(datetime.strftime(min_txn_date, '%Y-%m-%d'))
        start_date = datetime.strftime(start_date, '%Y-%m-%d')

        mf_txn_final_payload = {
            'PR_MF_TRASACTION_LOAD' : mf_txn_payloads
            ,'PR_MF_DEP_HOLD_LOAD'  : None
        }
        task_id = submit_threaded_task(execute_process_group_using_metadata, 'PG_MF_TRANSACTION_LOAD', start_date = start_date, payloads = mf_txn_final_payload, process_frequency = 'Ad hoc', user_id = user_id)
        return jsonify({'message': f'Mutual Fund transaction entry has been successfully added. Background process started with Task ID: {task_id}', 'status' : 'Success'})
    except Exception as e:
        return jsonify({'message': repr(e), 'status': "Failed"}), 500