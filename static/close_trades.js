// Historical data fetch

// Method : POST
// URL    : /get_hist_price/${symbol}

document.addEventListener('DOMContentLoaded', () => {
  if (document.getElementById('opening_trade_id')) {
    init_opening_trade_id_dropdown();
  }
});

// Global Variable
let open_trades_data;

async function init_opening_trade_id_dropdown(){

const opening_trade_id = document.getElementById('opening_trade_id')
const alt_symbol                   = document.getElementById('alt_symbol')
const opening_trade_date           = document.getElementById('opening_trade_date')
const opening_trade_stock_quantity = document.getElementById('opening_trade_stock_quantity')
const opening_trade_buy_or_sell    = document.getElementById('opening_trade_buy_or_sell')

const open_trades_response = await fetch ('/api/trades/open/', {
  method: 'GET'
})

open_trades_data = await open_trades_response.json();

open_trades_data.data.forEach(element => {
  opening_trade_id.innerHTML        += `<option id = "options">${element.trade_id}</option>`
  alt_symbol.value                   = element.stock_name
  opening_trade_date.value           = element.trade_date
  opening_trade_stock_quantity.value = element.stock_quantity
  opening_trade_buy_or_sell.value    = element.buy_or_sell
});
}

/////////////////////////////////////////////////////////////////////////////////////////

document.getElementById('opening_trade_id').addEventListener('change', async function (e) {
e.preventDefault();

const opening_trade_id             = document.getElementById('opening_trade_id').value
const alt_symbol                   = document.getElementById('alt_symbol')
const opening_trade_date           = document.getElementById('opening_trade_date')
const opening_trade_stock_quantity = document.getElementById('opening_trade_stock_quantity')
const opening_trade_buy_or_sell    = document.getElementById('opening_trade_buy_or_sell')

console.log(opening_trade_id)

open_trades_data.data.forEach(element => {
if (opening_trade_id == element.trade_id){
  alt_symbol.value                   = element.stock_name
  opening_trade_date.value           = element.trade_date
  opening_trade_stock_quantity.value = element.stock_quantity
  opening_trade_buy_or_sell.value    = element.buy_or_sell
}
})
})

/////////////////////////////////////////////////////////////////////////////////////////

document.getElementById('close_trades_form').addEventListener('submit', async function (e) {
e.preventDefault();
// const alt_symbol = document.getElementById('alt_symbol').value;
// const symbol_response = await fetch(`/api/symbol/${alt_symbol}/`, {
// method: 'GET'
// })

// const symbol_data = await symbol_response.json();
// const symbol = symbol_data.symbol_list[0]

// const start_date = document.getElementById('start_date').value;
// const end_date = document.getElementById('end_date').value;
// const formData = new FormData();
// formData.append('alt_symbol', alt_symbol);
// formData.append('start_date', start_date);
// formData.append('end_date', end_date);

// const response = await fetch(`/api/hist_price/${symbol}/`, {
// method: 'POST',
// body: formData
// })

// const data = await response.json();
// const resultDiv = document.getElementById('result')

// if(data.status === "Success"){
//     resultDiv.innerHTML = `<strong>${data.message}</strong>`
//     document.getElementById("hist_price_form").reset();
// }
// else{
//     resultDiv.innerHTML = `<strong>${data.message}</strong>`
// }
})

/////////////////////////////////////////////////////////////////////////////////////////

// Mode Switch

const toggle = document.getElementById('themeToggle');

  // Load theme from localStorage
const savedTheme = localStorage.getItem('theme');
if (savedTheme === 'dark') {
document.body.classList.add('dark-mode');
}

  if (toggle) {
    toggle.addEventListener('click', () => {
      document.body.classList.toggle('dark-mode');

      // Save theme choice
      if (document.body.classList.contains('dark-mode')) {
        localStorage.setItem('theme', 'dark');
      } else {
        localStorage.setItem('theme', 'light');
      }
    });
  }
