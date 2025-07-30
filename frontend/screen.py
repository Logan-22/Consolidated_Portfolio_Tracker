from flask import Blueprint, render_template

frontend = Blueprint('frontend', __name__)

@frontend.route('/')
def index():
    return render_template('index.html')

@frontend.route('/simulate_returns/')
def simulate_returns():
    return render_template('simulate_returns.html')

@frontend.route('/process_price/')
def process_price_form():
    return render_template('process_price_form.html')

@frontend.route('/metadata/')
def metadata_entry():
    return render_template('metadata_entry.html')

@frontend.route('/mf_order/')
def mf_order_entry():
    return render_template('mf_order_entry.html')

@frontend.route('/stock_order_pdf/')
def stock_order_pdf():
    return render_template('stock_order_pdf.html')

@frontend.route('/processing_date/')
def processing_date():
    return render_template('processing_date.html')

@frontend.route('/process_mf_returns/')
def process_mf_returns():
    return render_template('process_mf_returns.html')

@frontend.route('/process_realised_stock_returns/')
def process_realised_stock_returns():
    return render_template('process_realised_stock_returns.html')

@frontend.route('/holiday_calendar_setup/')
def holiday_calendar_setup():
    return render_template('holiday_calendar_setup.html')

@frontend.route('/additional_links/')
def additional_links():
    return render_template('additional_links.html')

@frontend.route('/close_trades/')
def close_trades():
    return render_template('close_trades.html')

@frontend.route('/process_unrealised_stock_returns/')
def process_unrealised_stock__returns():
    return render_template('process_unrelalised_stock_returns.html')

@frontend.route('/process_consolidated_returns')
def process_consolidated_returns():
    return render_template('process_consolidated_returns.html')

@frontend.route('/process_consolidated_allocation')
def process_consolidated_allocation():
    return render_template('process_consolidated_allocation.html')

@frontend.route('/process_entry')
def process_entry():
    return render_template('process_entry.html')

@frontend.route('/missing_prices_entry')
def missing_prices_entry():
    return render_template('missing_prices_entry.html')