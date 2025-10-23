from flask import Blueprint, request, jsonify
from utils.sql_utils.query_db.get_or_process_in_db import\
get_user,\
get_instrument

process_bp = Blueprint('process', __name__)

@process_bp.route('/', methods = ['POST'])
def adhoc_or_scheduled_processing():
    print('Helllo')
    try:
        print("Hello")
        user_id       = request.form.get('user_id') or None
        instrument_id = request.form.get('instrument_id') or None
        process_type  = request.form.get('process_type') or None
        start_date    = request.form.get('start_date') or None
        end_date      = request.form.get('end_date') or None
        print(user_id)
        if user_id:
            user_data = get_user(user_id)
            print(user_data)
            if not user_data.get('USER_ID'):
                return jsonify({'message': 'Invalid User ID', 'status': 'Failed'})
        else:
            user_id = None

        if instrument_id:
            instrument_data = get_instrument(instrument_id)
            if not instrument_data.get('INSTRUMENT_ID'):
                return jsonify({'message': 'Invalid Instrument ID', 'status': 'Failed'})
        else:
            instrument_id = None

        return jsonify({'message': 'Started the Ad Hoc Process with Background Task ID: 1', 'status': 'Success'})
    except Exception as e:
        return jsonify({'message': repr(e), 'status': 'Failed'})
