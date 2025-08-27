from flask import Blueprint, jsonify, request
from os import getenv
from json import loads
from utils.sql_utils.process.execute_process_group import execute_process_group_using_metadata

env = getenv('ENVIRONMENT')

user_transactions_bp = Blueprint('user_transactions', __name__)

@user_transactions_bp.route('/mf_txn/', methods = ['POST'])
def mf_transaction_entry():
    try:
        mf_txn_payload = loads(request.form.get('mf_order_payload'))
        mf_txn_payload['STAMP_FEES_AMOUNT'] = round(float(mf_txn_payload['INVESTED_AMOUNT']) - float(mf_txn_payload['AMC_AMOUNT']),2)

        mf_order_entry_logs = execute_process_group_using_metadata('PG_MF_TXN', payloads = mf_txn_payload)
        return jsonify(mf_order_entry_logs)
    except Exception as e:
        return jsonify({'message': repr(e), 'status': "Failed"})