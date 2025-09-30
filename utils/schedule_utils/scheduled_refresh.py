from yfinance import Ticker
from datetime import date, datetime
from dateutil import parser
from utils.sql_utils.process.execute_process_group import execute_process_group_using_metadata
from utils.thread_utils.thread_executor import submit_threaded_task
from utils.sql_utils.query_db.get_or_process_in_db import\
get_or_create_instrument_id,\
get_metadata_instruments,\
get_date_setup_from_holiday_calendar

def scheduled_refresh(app):
    try:
        with app.app_context():
            print(f'Scheduled Refresh started at {datetime.now()}')
            price_payloads  = []
            instrument_ids = get_or_create_instrument_id(exchange_symbol = None, process_type = 'get', process_flag = 1)
            end_date_for_instrument = date.today().strftime('%Y-%m-%d')
            for instrument in instrument_ids:
                metadata_instrument = get_metadata_instruments(instrument)
                ticker = Ticker(metadata_instrument['YAHOO_SYMBOL'])
                start_date_for_instrument = metadata_instrument['START_DATE']
                if datetime.strptime(start_date_for_instrument, '%Y-%m-%d') <= datetime.strptime(end_date_for_instrument, '%Y-%m-%d'):
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
                            print(f'Invalid Date from Yahoo Finance for {instrument}')
            scheduled_refresh_final_payload = {
                'PR_DAILY_INSTRUMENTS_PRICE_LOAD' : price_payloads
            }

            task_id = submit_threaded_task(execute_process_group_using_metadata, 'PG_SCHEDULED_REFRESH', start_date = None, end_date = end_date_for_instrument, payloads = scheduled_refresh_final_payload, process_frequency = 'On Start')
            print(f'Scheduled Refresh has started. Background process started with Task ID: {task_id}')
    except Exception as e:
        print(repr(e))