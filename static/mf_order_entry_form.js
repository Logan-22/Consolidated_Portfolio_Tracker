import { create_notification } from './create_notification.js'

// MF Order Entry into MF Order Table

// Method : POST
// URL    : /api/mf_order/

document.addEventListener('DOMContentLoaded', () => {
  if (document.getElementById('exchange_symbol')) {
    init_alt_symbol_dropdown();
  }
});

let mutual_fund_symbol_data = {}

async function init_alt_symbol_dropdown(){
const mutual_fund_symbol_response = await fetch ('/api/metadata_store/symbols?portfolio_type=Mutual+Fund', {
  method: 'GET'
})

mutual_fund_symbol_data = await mutual_fund_symbol_response.json();

const exchange_symbol = document.getElementById('exchange_symbol')
const alt_symbol      = document.getElementById('alt_symbol')
mutual_fund_symbol_data.all_symbols_list.forEach((element, index) => {
exchange_symbol.innerHTML += `<option id = "options">${element['EXCHANGE_SYMBOL']}</option>`
if(index == 0){
alt_symbol.value = element['ALT_SYMBOL']
  }
});
}

document.getElementById('exchange_symbol').addEventListener('change', e => {
const exchange_symbol = document.getElementById('exchange_symbol').value
const alt_symbol      = document.getElementById('alt_symbol')
mutual_fund_symbol_data.all_symbols_list.forEach(element => {
if(exchange_symbol == element['EXCHANGE_SYMBOL']){
alt_symbol.value = element['ALT_SYMBOL']
}
});
})

/////////////////////////////////////////////////////////////////////////////////////////

document.getElementById('mf_order_entry_form').addEventListener('submit', async function (e) {
e.preventDefault();

const exchange_symbol = document.getElementById('exchange_symbol').value;
const alt_symbol      = document.getElementById('alt_symbol').value;
const purchase_date   = document.getElementById('purchase_date').value;
const invested_amount = document.getElementById('invested_amount').value;
const amc_amount      = document.getElementById('amc_amount').value;

const price_lookup_response = await fetch(`/api/price_table/close_price/?alt_symbol=${alt_symbol}&purchase_date=${purchase_date}`, {
method: 'GET'
})

const price_lookup_data = await price_lookup_response.json();
if (price_lookup_data.price_data.length > 1){
create_notification(`Duplicate Price Entries present for ${alt_symbol} on ${purchase_date} in PRICE_TABLE`, 'duplicate_issue')
}else if (! price_lookup_data.price_data['PRICE']){
create_notification(`No Price Entries present for ${alt_symbol} on ${purchase_date} in PRICE_TABLE`, 'error')
}
else{
const price_during_purchase = price_lookup_data.price_data['PRICE']

const units = amc_amount/price_during_purchase

const mf_order_payload = {
'NAME'                 : exchange_symbol
,'PURCHASED_ON'        : purchase_date
,'INVESTED_AMOUNT'     : invested_amount
,'AMC_AMOUNT'          : amc_amount
,'NAV_DURING_PURCHASE' : price_during_purchase
,'UNITS'               : units
}

const formData = new FormData();
formData.append('mf_order_payload', JSON.stringify(mf_order_payload));

const mf_order_response = await fetch(`/api/mf_order/`, {
method: 'POST',
body: formData
})

const mf_order_data = await mf_order_response.json();

create_notification(mf_order_data.message, mf_order_data.status)

// Reprocess the MF Returns, Consolidated Returns and Simulated Returns from Fund Purchase Date

// const mf_returns_response = await fetch (`/api/process_mf_returns/?start_date=${purchase_date}`, {
//   method: 'GET'
// })

// const mf_returns_data = await mf_returns_response.json();

// create_notification(mf_returns_data.message, mf_returns_data.status)

// const process_simulate_returns_response = await fetch(`/api/process_simulate_returns/?start_date=${purchase_date}`, {
// method: 'GET'
// })

// const process_simulate_returns_data = await process_simulate_returns_response.json();

// create_notification(process_simulate_returns_data.message, process_simulate_returns_data.status)
}
})