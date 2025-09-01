import { create_notification } from './create_notification.js'

// Process Prices

// Method : POST
// URL    : /api/price_table/close_price/${alt_symbol}/

document.addEventListener('DOMContentLoaded', () => {
  if (document.getElementById('exchange_symbol')) {
    init_fund_names_dropdown();
  }
});

let all_symbols_data = {} // Global to be accessed by other eventlisteners

// Initialise Drop Down for Exchange Symbol

async function init_fund_names_dropdown(){
const all_symbols_response = await fetch ('/api/metadata/instruments/symbols/', {
  method: 'GET'
})

all_symbols_data = await all_symbols_response.json();

const exchange_symbol = document.getElementById('exchange_symbol')

all_symbols_data.all_symbols_list.forEach((element,index) => {
exchange_symbol.innerHTML += `<option id = "options">${element['EXCHANGE_SYMBOL']}</option>`
});
}

/////////////////////////////////////////////////////////////////////////////////////////

document.getElementById('procss_price_form').addEventListener('submit', async function (e) {
e.preventDefault();
const exchange_symbol = document.getElementById('exchange_symbol').value
const start_date      = document.getElementById('start_date').value;
const end_date        = document.getElementById('end_date').value;

const exchange_symbol_url = exchange_symbol.replaceAll(' ', '%20')

const price_table_refresh_response = await fetch(`/api/metrics/instrument_prices/close_price?exchange_symbol=${exchange_symbol_url}&start_date=${start_date}&end_date=${end_date}`, {
method: 'GET'
})

const price_table_refresh_data = await price_table_refresh_response.json();

create_notification(price_table_refresh_data.message, price_table_refresh_data.status)
}
)