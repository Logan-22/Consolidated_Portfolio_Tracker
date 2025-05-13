from flask import Flask, request, render_template, jsonify
import yfinance as yf
import sqlite3
from datetime import datetime, timedelta
import os

# Create and use the path for database
db_path = os.path.join(os.getcwd(), "databases", "consolidated_portfolio.db")

if not os.path.exists(os.path.dirname(db_path)):
    os.makedirs(os.path.dirname(db_path))

app = Flask(__name__)

def fetch_nav(symbol):
    end_date = datetime.today()
    start_date = end_date - timedelta(days=5)
    ticker = yf.Ticker(symbol)
    data = ticker.history(start=start_date.strftime('%Y-%m-%d'), end=end_date.strftime('%Y-%m-%d'))
    if data.empty:
        return None
    return round(data['Close'].iloc[-1], 4)

def store_to_db(alt_symbol, price):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS ''' + alt_symbol + ''' (
            ID INTEGER PRIMARY KEY AUTOINCREMENT,
            DATE TEXT,
            PRICE REAL
        )
    ''')
    today = datetime.now().strftime("%Y-%m-%d")
    cursor.execute("INSERT INTO " + alt_symbol + "(DATE, PRICE) VALUES (?, ?)",
                   (today, price))
    conn.commit()
    conn.close()

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/get_nav', methods=['POST'])
def get_nav():
    price = fetch_nav('0P0000XVU2.BO')
    if price is None:
        return jsonify({'status': 'error', 'message': "Failed to fetch NAV for UTI Nifty 50 Direct Growth."})
    store_to_db(price)
    return jsonify({'status': 'success', 'name': "UTI Nifty 50 Direct Growth", 'nav': price})

@app.route('/history', methods=['GET'])
def history():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute("SELECT date, nav FROM mutual_fund_nav ORDER BY date DESC")
    rows = cursor.fetchall()
    conn.close()

    history_data = [{'date': row[0], 'nav': row[1]} for row in rows]
    return jsonify(history_data)

if __name__ == '__main__':
    app.run(debug=True)