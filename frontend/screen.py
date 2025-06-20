from flask import Blueprint, render_template

frontend = Blueprint('frontend', __name__)

@frontend.route('/')
def index():
    return render_template('index.html')

@frontend.route('/hist_price/')
def hist_price_form():
    return render_template('hist_price_form.html')

@frontend.route('/metadata/')
def metadata_entry():
    return render_template('metadata_entry.html')

@frontend.route('/mf_order/')
def mf_order_entry():
    return render_template('mf_order_entry.html')

@frontend.route('/stock_order/')
def stock_order_entry():
    return render_template('stock_order_entry.html')

@frontend.route('/stock_order_pdf/')
def stock_order_pdf():
    return render_template('stock_order_pdf.html')

@frontend.route('/processing_date/')
def processing_date():
    return render_template('processing_date.html')

@frontend.route('/process_mf_hist_returns/')
def process_mf_hist_returns():
    return render_template('process_mf_hist_returns.html')

@frontend.route('/process_realised_stock_hist_returns/')
def process_realised_stock_hist_returns():
    return render_template('process_realised_stock_hist_returns.html')

@frontend.route('/holiday_calendar_setup/')
def holiday_calendar_setup():
    return render_template('holiday_calendar_setup.html')

@frontend.route('/additional_links/')
def additional_links():
    return render_template('additional_links.html')