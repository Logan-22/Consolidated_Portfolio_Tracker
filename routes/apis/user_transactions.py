from flask import Blueprint, jsonify, request, g
from decimal import Decimal
from os import getenv
from json import loads
from utils.sql_utils.process.execute_process_group import execute_process_group_using_metadata
from utils.auth_utils.auth_utils import require_login
from utils.sql_utils.query_db.get_or_process_in_db import\
get_or_create_instrument_id,\
get_instrument_price

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
            return jsonify({'message': 'Invalid User Request', 'status' : 'Failed'})
        mf_txn_payload['INSTRUMENT_ID']       = get_or_create_instrument_id(mf_txn_payload['EXCHANGE_SYMBOL'], 'get')
        mf_txn_payload['STAMP_FEES_AMOUNT']   = round(float(mf_txn_payload['TXN_AMOUNT']) - float(mf_txn_payload['AMC_AMOUNT']),4)
        instrument_price_data = get_instrument_price(mf_txn_payload['INSTRUMENT_ID'], mf_txn_payload['TXN_DATE'])
        mf_txn_payload['NAV_DURING_PURCHASE'] = instrument_price_data['PRICE']
        mf_txn_payload['UNITS']               = round(Decimal(mf_txn_payload['AMC_AMOUNT']) / Decimal(mf_txn_payload['NAV_DURING_PURCHASE']), 4)

        mf_txn_final_payload = {
            'PR_MF_TRASACTION_LOAD' : mf_txn_payload
        }

        mf_txn_entry_logs = execute_process_group_using_metadata('PG_MF_TRANSACTION_LOAD', payloads = mf_txn_final_payload)
        return jsonify(mf_txn_entry_logs)
    except Exception as e:
        return jsonify({'message': repr(e), 'status': "Failed"})