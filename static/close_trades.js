import { create_notification } from './create_notification.js'

// Method : POST
// URL    : /api/close_trade/

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

create_notification(open_trades_data.message, open_trades_data.status)

open_trades_data.data.forEach((element,index) => {
  opening_trade_id.innerHTML        += `<option id = "options">${element['TRADE_ID']}</option>`
  closing_trade_id.innerHTML        += `<option id = "options">${element['TRADE_ID']}</option>`
  
  if(index == 0){
  opening_alt_symbol.value           = element['STOCK_NAME']
  opening_trade_date.value           = element['TRADE_DATE']
  opening_trade_stock_quantity.value = element['STOCK_QUANTITY']
  opening_trade_buy_or_sell.value    = element['BUY_OR_SELL']

  closing_alt_symbol.value           = element['STOCK_NAME']
  closing_trade_date.value           = element['TRADE_DATE']
  closing_trade_stock_quantity.value = element['STOCK_QUANTITY']
  closing_trade_buy_or_sell.value    = element['BUY_OR_SELL']
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
if (opening_trade_id == element['TRADE_ID']){
  opening_alt_symbol.value           = element['STOCK_NAME']
  opening_trade_date.value           = element['TRADE_DATE']
  opening_trade_stock_quantity.value = element['STOCK_QUANTITY']
  opening_trade_buy_or_sell.value    = element['BUY_OR_SELL']
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
if (closing_trade_id == element['TRADE_ID']){
  closing_alt_symbol.value           = element['STOCK_NAME']
  closing_trade_date.value           = element['TRADE_DATE']
  closing_trade_stock_quantity.value = element['STOCK_QUANTITY']
  closing_trade_buy_or_sell.value    = element['BUY_OR_SELL']
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
const close_trades_payloads = []

const closed_trade_entry_table       = document.getElementById('closed_trade_entry_table')
const closed_trade_entry_table_child = closed_trade_entry_table.children

for(let i = 1; i <= closed_trades_entry_count; i++ ){

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
create_notification('Opening Trade Id and Closing Trade ID cannot be same', 'warning')
}

if(opening_alt_symbol != closing_alt_symbol){
errors_in_close_trade_entries += 1
closed_trade_entry_tr.classList = "error"
create_notification('Opening Stock Symbol and Closing Stock Symbol should be same', 'warning')
}

if(opening_trade_date_object > closing_trade_date_object){
errors_in_close_trade_entries += 1
closed_trade_entry_tr.classList = "error"
create_notification('Opening Trade Date cannot be greater than Closing Trade Date', 'warning')
}

// if(opening_trade_buy_or_sell ==  closing_trade_buy_or_sell){
// errors_in_close_trade_entries += 1
// closed_trade_entry_tr.classList = "error"
// create_notification('Opening Trade and Closing Trade cannot be of the same Buy Or Sell Type', 'warning')
// }

if(errors_in_close_trade_entries == 0){

close_trades_data['STOCK_SYMBOL']                 = opening_alt_symbol

close_trades_data['OPENING_TRADE_ID']             = opening_trade_id
close_trades_data['OPENING_TRADE_DATE']           = opening_trade_date
close_trades_data['OPENING_TRADE_STOCK_QUANTITY'] = opening_trade_stock_quantity
close_trades_data['OPENING_TRADE_BUY_OR_SELL']    = opening_trade_buy_or_sell

close_trades_data['CLOSING_TRADE_ID']             = closing_trade_id
close_trades_data['CLOSING_TRADE_DATE']           = closing_trade_date
close_trades_data['CLOSING_TRADE_STOCK_QUANTITY'] = closing_trade_stock_quantity
close_trades_data['CLOSING_TRADE_BUY_OR_SELL']    = closing_trade_buy_or_sell

close_trades_payloads.push(close_trades_data)
}
}

if(errors_in_close_trade_entries == 0){

const formData = new FormData();
formData.append('close_trades_payloads', JSON.stringify(close_trades_payloads));

const post_close_trade_response = await fetch(`/api/close_trades/`, {
method: 'POST',
body: formData
})

const post_close_trade_data = await post_close_trade_response.json();

create_notification(post_close_trade_data.message, post_close_trade_data.status)
}
})