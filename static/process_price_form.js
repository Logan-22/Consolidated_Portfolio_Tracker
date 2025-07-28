import { create_notification } from './create_notification.js'

// Process Prices

// Method : POST
// URL    : /api/price_table/close_price/${alt_symbol}/

document.addEventListener('DOMContentLoaded', () => {
  if (document.getElementById('exchange_symbol')) {
    init_alt_symbol_dropdown();
  }
});

let all_symbols_data = {} // Global to be accessed by other eventlisteners

// Initialise Drop Down for Exchange Symbol

async function init_alt_symbol_dropdown(){
const all_symbols_response = await fetch ('/api/metadata_store/symbols/', {
  method: 'GET'
})

all_symbols_data = await all_symbols_response.json();

const exchange_symbol = document.getElementById('exchange_symbol')
const yahoo_symbol    = document.getElementById('yahoo_symbol')
const alt_symbol      = document.getElementById('alt_symbol')
const portfolio_type  = document.getElementById('portfolio_type')

all_symbols_data.all_symbols_list.forEach((element,index) => {
exchange_symbol.innerHTML += `<option id = "options">${element.exchange_symbol}</option>`
if(index == 0){
yahoo_symbol.value   = element.yahoo_symbol
alt_symbol.value     = element.alt_symbol
portfolio_type.value = element.portfolio_type
}
});
}

// Change Yahoo Symbol and Alt Symbol Values when Exchange Symbol Changes

document.getElementById('exchange_symbol').addEventListener('input', function (e){
e.preventDefault();
let exchange_symbol   = document.getElementById('exchange_symbol').value;

all_symbols_data.all_symbols_list.forEach(element => {
if(exchange_symbol == element.exchange_symbol){
yahoo_symbol.value   = element.yahoo_symbol
alt_symbol.value     = element.alt_symbol
portfolio_type.value = element.portfolio_type
}
});
})

/////////////////////////////////////////////////////////////////////////////////////////

document.getElementById('procss_price_form').addEventListener('submit', async function (e) {
e.preventDefault();
const yahoo_symbol    = document.getElementById('yahoo_symbol').value
const portfolio_type  = document.getElementById('portfolio_type').value
const alt_symbol      = document.getElementById('alt_symbol').value
const start_date      = document.getElementById('start_date').value;
const end_date        = document.getElementById('end_date').value;

const formData = new FormData();
formData.append('yahoo_symbol', yahoo_symbol);
formData.append('portfolio_type', portfolio_type);
formData.append('start_date', start_date);
formData.append('end_date', end_date);

const price_table_refresh_response = await fetch(`/api/price_table/close_price/${alt_symbol}/`, {
method: 'POST',
body: formData
})

const price_table_refresh_data = await price_table_refresh_response.json();

create_notification(price_table_refresh_data.message, price_table_refresh_data.status)
}
)