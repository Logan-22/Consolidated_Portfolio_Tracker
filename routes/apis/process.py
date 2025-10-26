from flask import Blueprint, request, jsonify
from json import loads
from utils.thread_utils.thread_executor import submit_threaded_task
from utils.sql_utils.process.execute_process_group import execute_process_group_using_metadata
from utils.sql_utils.query_db.get_or_process_in_db import\
get_user,\
get_instrument

process_bp = Blueprint('process', __name__)

@process_bp.route('/', methods = ['POST'])
def adhoc_processing():
    try:
        process_payload = loads(request.form.get('process_payload'))
        user_id         = process_payload.get('user_id') or None
        instrument_id   = process_payload.get('instrument_id') or None
        process_type    = process_payload.get('process_type') or None
        start_date      = process_payload.get('start_date') or None
        end_date        = process_payload.get('end_date') or None

        if user_id:
            user_data = get_user(user_id)
            if not user_data:
                return jsonify({'message': 'Invalid User ID', 'status': 'Failed'})
        else:
            user_id = None

        if instrument_id:
            instrument_data = get_instrument(instrument_id)
            if not instrument_data:
                return jsonify({'message': 'Invalid Instrument ID', 'status': 'Failed'})
        else:
            instrument_id = None
        
        if process_type == 'Mutual Fund Process':
            task_id = submit_threaded_task(execute_process_group_using_metadata, 'PG_MF_TRANSACTION_LOAD', start_date = start_date, end_date = end_date, payloads = {}, process_frequency = 'Ad hoc', user_id = user_id, instrument_ids = instrument_id)

        return jsonify({'message': f'Ad Hoc Process has started. Background process started with Task ID: {task_id}', 'status': 'Success'})
    except Exception as e:
        return jsonify({'message': repr(e), 'status': 'Failed'})
