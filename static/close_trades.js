// Historical data fetch

// Method : POST
// URL    : /get_hist_price/${symbol}

document.addEventListener('DOMContentLoaded', () => {
  if (document.getElementById('opening_trade_id')) {
    init_opening_trade_id_dropdown();
    const closed_trade_entry_table     = document.getElementById('closed_trade_entry_table')
    closed_trade_entry_table.classList = 'hidden' // Hide the Close Trades Entry Table when loaded
  }
});

// Global Variables
let open_trades_data;
let closed_trades_entry_count = 0;

async function init_opening_trade_id_dropdown(){

const opening_trade_id             = document.getElementById('opening_trade_id')
const opening_alt_symbol           = document.getElementById('opening_alt_symbol')
const opening_trade_date           = document.getElementById('opening_trade_date')
const opening_trade_stock_quantity = document.getElementById('opening_trade_stock_quantity')
const opening_trade_buy_or_sell    = document.getElementById('opening_trade_buy_or_sell')

const closing_trade_id             = document.getElementById('closing_trade_id')
const closing_alt_symbol           = document.getElementById('closing_alt_symbol')
const closing_trade_date           = document.getElementById('closing_trade_date')
const closing_trade_stock_quantity = document.getElementById('closing_trade_stock_quantity')
const closing_trade_buy_or_sell    = document.getElementById('closing_trade_buy_or_sell')

const open_trades_response = await fetch ('/api/trades/open/', {
  method: 'GET'
})

open_trades_data = await open_trades_response.json();

open_trades_data.data.forEach((element,index) => {
  opening_trade_id.innerHTML        += `<option id = "options">${element.trade_id}</option>`
  closing_trade_id.innerHTML        += `<option id = "options">${element.trade_id}</option>`
  
  if(index == 0){
  opening_alt_symbol.value           = element.stock_name
  opening_trade_date.value           = element.trade_date
  opening_trade_stock_quantity.value = element.stock_quantity
  opening_trade_buy_or_sell.value    = element.buy_or_sell

  closing_alt_symbol.value           = element.stock_name
  closing_trade_date.value           = element.trade_date
  closing_trade_stock_quantity.value = element.stock_quantity
  closing_trade_buy_or_sell.value    = element.buy_or_sell
  }
});
}

/////////////////////////////////////////////////////////////////////////////////////////

document.getElementById('opening_trade_id').addEventListener('change', async function (e) {
e.preventDefault();

const opening_trade_id             = document.getElementById('opening_trade_id').value
const opening_alt_symbol           = document.getElementById('opening_alt_symbol')
const opening_trade_date           = document.getElementById('opening_trade_date')
const opening_trade_stock_quantity = document.getElementById('opening_trade_stock_quantity')
const opening_trade_buy_or_sell    = document.getElementById('opening_trade_buy_or_sell')

open_trades_data.data.forEach(element => {
if (opening_trade_id == element.trade_id){
  opening_alt_symbol.value           = element.stock_name
  opening_trade_date.value           = element.trade_date
  opening_trade_stock_quantity.value = element.stock_quantity
  opening_trade_buy_or_sell.value    = element.buy_or_sell
}
})
})

/////////////////////////////////////////////////////////////////////////////////////////

document.getElementById('closing_trade_id').addEventListener('change', async function (e) {
e.preventDefault();

const closing_trade_id             = document.getElementById('closing_trade_id').value
const closing_alt_symbol           = document.getElementById('closing_alt_symbol')
const closing_trade_date           = document.getElementById('closing_trade_date')
const closing_trade_stock_quantity = document.getElementById('closing_trade_stock_quantity')
const closing_trade_buy_or_sell    = document.getElementById('closing_trade_buy_or_sell')

open_trades_data.data.forEach(element => {
if (closing_trade_id == element.trade_id){
  closing_alt_symbol.value           = element.stock_name
  closing_trade_date.value           = element.trade_date
  closing_trade_stock_quantity.value = element.stock_quantity
  closing_trade_buy_or_sell.value    = element.buy_or_sell
}
})
})

/////////////////////////////////////////////////////////////////////////////////////////

document.getElementById('close_trades_form').addEventListener('submit', async function (e) {
e.preventDefault();
closed_trades_entry_count += 1

const opening_trade_id             = document.getElementById('opening_trade_id').value
const opening_alt_symbol           = document.getElementById('opening_alt_symbol').value
const opening_trade_date           = document.getElementById('opening_trade_date').value
const opening_trade_stock_quantity = document.getElementById('opening_trade_stock_quantity').value
const opening_trade_buy_or_sell    = document.getElementById('opening_trade_buy_or_sell').value

const closing_trade_id             = document.getElementById('closing_trade_id').value
const closing_alt_symbol           = document.getElementById('closing_alt_symbol').value
const closing_trade_date           = document.getElementById('closing_trade_date').value
const closing_trade_stock_quantity = document.getElementById('closing_trade_stock_quantity').value
const closing_trade_buy_or_sell    = document.getElementById('closing_trade_buy_or_sell').value

const closed_trade_entry_table     = document.getElementById('closed_trade_entry_table')

closed_trade_entry_table.classList = ""

closed_trade_entry_table.innerHTML += `
<tr id = "closed_trade_entry_${closed_trades_entry_count}">
    <td>${opening_trade_id}</td>
    <td>${opening_alt_symbol}</td>
    <td>${opening_trade_date}</td>
    <td>${opening_trade_stock_quantity}</td>
    <td>${opening_trade_buy_or_sell}</td>
    <td>${closing_trade_id}</td>
    <td>${closing_alt_symbol}</td>
    <td>${closing_trade_date}</td>
    <td>${closing_trade_stock_quantity}</td>
    <td>${closing_trade_buy_or_sell}</td>
</tr>
`

})

/////////////////////////////////////////////////////////////////////////////////////////

document.getElementById('close_trades_submit').addEventListener('click', async function (e) {
e.preventDefault();
let errors_in_close_trade_entries = 0
const close_trades_data_array = []
const resultDiv = document.getElementById('result')

const closed_trade_entry_table       = document.getElementById('closed_trade_entry_table')
const closed_trade_entry_table_child = closed_trade_entry_table.children

for(i = 1; i <= closed_trades_entry_count; i++ ){

const close_trades_data = {}

const closed_trade_entry_table_child_tbody       = closed_trade_entry_table_child[i]
const closed_trade_entry_table_child_tbody_child = closed_trade_entry_table_child_tbody.children
const closed_trade_entry_tr                      = closed_trade_entry_table_child_tbody_child[0] // The closed trade entry TR element
const closed_trade_entry_td                      = closed_trade_entry_tr.children
  
const opening_trade_id             = closed_trade_entry_td[0].textContent
const opening_alt_symbol           = closed_trade_entry_td[1].textContent
const opening_trade_date           = closed_trade_entry_td[2].textContent
const opening_trade_stock_quantity = closed_trade_entry_td[3].textContent
const opening_trade_buy_or_sell    = closed_trade_entry_td[4].textContent

const closing_trade_id             = closed_trade_entry_td[5].textContent
const closing_alt_symbol           = closed_trade_entry_td[6].textContent
const closing_trade_date           = closed_trade_entry_td[7].textContent
const closing_trade_stock_quantity = closed_trade_entry_td[8].textContent
const closing_trade_buy_or_sell    = closed_trade_entry_td[9].textContent

const opening_trade_date_object    = new Date(opening_trade_date)
const closing_trade_date_object    = new Date(closing_trade_date)

//Validations
if(opening_trade_id == closing_trade_id){
errors_in_close_trade_entries += 1
closed_trade_entry_tr.classList = "error"
resultDiv.innerHTML += `<strong>Opening Trade Id and Closing Trade ID cannot be same</strong><br/>`
}

if(opening_alt_symbol != closing_alt_symbol){
errors_in_close_trade_entries += 1
closed_trade_entry_tr.classList = "error"
resultDiv.innerHTML += `<strong>Opening Stock Symbol and Closing Stock Symbol should be same</strong><br/>`
}

if(opening_trade_date_object > closing_trade_date_object){
errors_in_close_trade_entries += 1
closed_trade_entry_tr.classList = "error"
resultDiv.innerHTML += `<strong>Opening Trade Date cannot be greater than Closing Trade Date</strong><br/>`
}

if(opening_trade_buy_or_sell ==  closing_trade_buy_or_sell){
errors_in_close_trade_entries += 1
closed_trade_entry_tr.classList = "error"
resultDiv.innerHTML += `<strong>Opening Trade and Closing Trade cannot be of the same Buy Or Sell Type</strong><br/>`
}

if(errors_in_close_trade_entries == 0){

close_trades_data.opening_trade_id = opening_trade_id
close_trades_data.opening_alt_symbol = opening_alt_symbol
close_trades_data.opening_trade_date = opening_trade_date
close_trades_data.opening_trade_stock_quantity = opening_trade_stock_quantity
close_trades_data.opening_trade_buy_or_sell = opening_trade_buy_or_sell

close_trades_data.closing_trade_id = closing_trade_id
close_trades_data.closing_alt_symbol = closing_alt_symbol
close_trades_data.closing_trade_date = closing_trade_date
close_trades_data.closing_trade_stock_quantity = closing_trade_stock_quantity
close_trades_data.closing_trade_buy_or_sell = closing_trade_buy_or_sell

close_trades_data_array.push(close_trades_data)
}
}

if(errors_in_close_trade_entries == 0){

const formData = new FormData();
formData.append('close_trades_data_array', JSON.stringify(close_trades_data_array));

const response = await fetch(`/api/close_trade/`, {
method: 'POST',
body: formData
})

const data = await response.json();
if(data.status === "Success"){
    resultDiv.innerHTML = `<strong>${data.message}</strong>`
    document.getElementById("close_trades_form").reset();
}
else{
    resultDiv.innerHTML = `<strong>${data.message}</strong>`
}
}

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
