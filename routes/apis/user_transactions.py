from flask import Blueprint, jsonify, request, g
from decimal import Decimal
from os import getenv
from json import loads
from utils.sql_utils.process.execute_process_group import execute_process_group_using_metadata
from utils.auth_utils.auth_utils import require_login
from utils.sql_utils.query_db.get_or_process_in_db import\
get_or_create_instrument_id,\
get_instrument_price,\
get_holding_data,\
get_consolidated_quantity_from_mf_txn

env = getenv('ENVIRONMENT')

user_transactions_bp = Blueprint('user_transactions', __name__)

@user_transactions_bp.route('/mf_txn/', methods = ['POST'])
@require_login
def mf_transaction_entry():
    try:
        mf_txn_payload = loads(request.form.get('mf_txn_payload'))
        if g.user_id:
            mf_txn_payload['USER_ID'] = g.user_id
        else:
            return jsonify({'message': 'Transaction Declined: Invalid User Request', 'status' : 'Failed'}), 403

        mf_txn_payload['INSTRUMENT_ID'] = get_or_create_instrument_id(mf_txn_payload['EXCHANGE_SYMBOL'], 'get')
        instrument_price_data           = get_instrument_price(mf_txn_payload['INSTRUMENT_ID'], mf_txn_payload['TXN_DATE'])

        if instrument_price_data and instrument_price_data.get('PRICE'):
            mf_txn_payload['NAV_DURING_PURCHASE'] = instrument_price_data['PRICE']
        else:
            return jsonify({'message': 'Transaction Declined: Invalid Transaction Date', 'status' : 'Failed'}), 422

        if mf_txn_payload['AMC_AMOUNT']:
            mf_txn_payload['UNITS'] = round(Decimal(mf_txn_payload['AMC_AMOUNT']) / Decimal(mf_txn_payload['NAV_DURING_PURCHASE']), 4)
        elif mf_txn_payload['UNITS']:
            mf_txn_payload['AMC_AMOUNT'] = round(Decimal(mf_txn_payload['UNITS']) * Decimal(mf_txn_payload['NAV_DURING_PURCHASE']), 4)
        else:
            return jsonify({'message': 'Transaction Declined: Please enter AMC Amount or Units', 'status' : 'Failed'}), 422

        mf_txn_payload['STAMP_FEES_AMOUNT'] = round(Decimal(mf_txn_payload['TXN_AMOUNT']) - Decimal(mf_txn_payload['AMC_AMOUNT']), 4)
        if Decimal(mf_txn_payload['STAMP_FEES_AMOUNT']) < 0 or Decimal(mf_txn_payload['AMC_AMOUNT']) < 0 or Decimal(mf_txn_payload['UNITS']) < 0:
            return jsonify({'message': 'Transaction Declined: Invalid AMC Amount or Units', 'status' : 'Failed'}), 422

        if mf_txn_payload['TXN_TYPE'] == 'Sell':
            holding_as_on_purchase_date       = get_holding_data(mf_txn_payload['INSTRUMENT_ID'], mf_txn_payload['USER_ID'], mf_txn_payload['TXN_DATE'])   
            if holding_as_on_purchase_date and holding_as_on_purchase_date.get('TOTAL_QUANTITY') and Decimal(holding_as_on_purchase_date['TOTAL_QUANTITY']) >= Decimal(mf_txn_payload['UNITS']):
                get_consolidated_quantity     = get_consolidated_quantity_from_mf_txn(mf_txn_payload['INSTRUMENT_ID'], mf_txn_payload['USER_ID'])
                if get_consolidated_quantity and get_consolidated_quantity.get('CONSOLIDATED_QUANTITY') and Decimal(get_consolidated_quantity['CONSOLIDATED_QUANTITY']) >= Decimal(mf_txn_payload['UNITS']):
                    pass
                else:
                    return jsonify({'message': 'Transaction Declined: Inconsistent Sell Order due Insufficient Units Held', 'status' : 'Failed'}), 422
            else:
                return jsonify({'message': 'Transaction Declined: No units available for this sell request', 'status' : 'Failed'}), 422

        mf_txn_final_payload = {
            'PR_MF_TRASACTION_LOAD' : mf_txn_payload
            ,'PR_MF_DEP_HOLD_LOAD'  : None
        }
        mf_txn_entry_logs = execute_process_group_using_metadata('PG_MF_TRANSACTION_LOAD', start_date = mf_txn_payload['TXN_DATE'], payloads = mf_txn_final_payload, process_frequency = 'Ad hoc', user_id = mf_txn_payload['USER_ID'], instrument_id = mf_txn_payload['INSTRUMENT_ID'])
        return jsonify(mf_txn_entry_logs)
    except Exception as e:
        return jsonify({'message': repr(e), 'status': "Failed"}), 500