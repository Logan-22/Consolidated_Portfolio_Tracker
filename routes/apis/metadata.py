from flask import Blueprint, request, jsonify
from json import loads
from utils.sql_utils.process.execute_process_group import execute_process_group_using_metadata
from utils.sql_utils.query_db.get_or_process_in_db import get_instrument_id_for_instrument

metadata_bp = Blueprint('metadata', __name__)

@metadata_bp.route('/instruments/', methods = ['POST'])
def metadata_instruments_entry():
    try:
        metadata_instruments_payload_unprocessed = loads(request.form.get('metadata_instruments_payload'))
        metadata_instruments_payload = {key : (None if value == "" else value) for key, value in metadata_instruments_payload_unprocessed.items()} # Convert "" to None so that it can be inserted as NULL in Mysql DB
        metadata_instruments_payload['INSTRUMENT_ID'] = get_instrument_id_for_instrument(metadata_instruments_payload['EXCHANGE_SYMBOL'])

        metadata_entry_final_payload = {
            'PR_METADATA_INSTRUMENTS_LOAD' : metadata_instruments_payload
        }
        metadata_entry_logs = execute_process_group_using_metadata('PG_METADATA_INSTRUMENTS_LOAD', payloads = metadata_entry_final_payload)
        return jsonify(metadata_entry_logs)
    except Exception as e:
        return jsonify({'message': repr(e), 'status': "Failed"})