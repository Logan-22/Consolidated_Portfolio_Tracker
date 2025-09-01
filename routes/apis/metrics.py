from flask import Blueprint, request, jsonify
import yfinance as yf
from dateutil import parser
from datetime import datetime, date
from json import loads
from utils.sql_utils.process.execute_process_group import execute_process_group_using_metadata
from utils.sql_utils.query_db.get_or_process_in_db import\
get_date_setup_from_holiday_calendar,\
get_or_create_instrument_id,\
get_metadata_instruments

metrics_bp = Blueprint('metrics', __name__)

@metrics_bp.route('/instrument_prices/close_price/', methods = ['GET'])
def upsert_price_table_for_alt_symbol():
    try:
        exchange_symbol = request.args.get('exchange_symbol') or None
        start_date      = request.args.get('start_date') or None
        end_date        = request.args.get('end_date') or None
        instrument_ids  = []
        price_payloads  = []
        if exchange_symbol:
            instrument_id = get_or_create_instrument_id(exchange_symbol, 'get')
            instrument_ids.append(instrument_id)
        else:
            instrument_ids = get_or_create_instrument_id(exchange_symbol = None, process_type = 'get', process_flag = 1)
        for instrument in instrument_ids:
            metadata_instrument = get_metadata_instruments(instrument)
            ticker = yf.Ticker(metadata_instrument['YAHOO_SYMBOL'])
            start_date_for_instrument = start_date if start_date else metadata_instrument['START_DATE']
            end_date_for_instrument = end_date if end_date else date.today().strftime('%Y-%m-%d')
            pandas_data = ticker.history(start = start_date_for_instrument, end = end_date_for_instrument)

            for index, value in pandas_data['Close'].items():
                value_date = str(index)[:10]
                if (parser.parse(value_date, fuzzy = 'fuzzy')):
                    value_date = datetime.strptime(value_date,'%Y-%m-%d')
                    value_date = value_date.strftime('%Y-%m-%d')

                    holiday_calendar_data = get_date_setup_from_holiday_calendar(value_date)

                    price_payload_from_yahoo_finance = {
                        'INSTRUMENT_ID'            : instrument
                        ,'VALUE_DATE'              : value_date
                        ,'PRICE'                   : round(value,4)
                        ,'PROCESSING_DATE'         : holiday_calendar_data['PROCESSING_DATE']
                        ,'PREVIOUS_PROCESSING_DATE': holiday_calendar_data['PREVIOUS_PROCESSING_DATE']
                        ,'NEXT_PROCESSING_DATE'    : holiday_calendar_data['NEXT_PROCESSING_DATE']
                    }
                    price_payloads.append(price_payload_from_yahoo_finance)
                else:
                    return jsonify({'message': f'Invalid Date from Yahoo Finance for {exchange_symbol}', 'status': "Failed"})
        process_price_final_payload = {
            'PR_DAILY_INSTRUMENTS_PRICE_LOAD' : price_payloads
        }
        process_price_logs = execute_process_group_using_metadata('PG_DAILY_PRICE_LOAD', start_date, end_date, process_price_final_payload)
        return jsonify(process_price_logs)
    except Exception as e:
        return jsonify({'message': repr(e), 'status': 'Failed'})
