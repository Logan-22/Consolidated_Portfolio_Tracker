import { create_notification } from './create_notification.js'

// MF Order Entry into MF Order Table

// Method : POST
// URL    : /api/mf_order/

document.addEventListener('DOMContentLoaded', () => {
  if (document.getElementById('exchange_symbol')) {
    init_mutual_fund_names_dropdown();
  }
});

let mutual_fund_symbol_data = {}

async function init_mutual_fund_names_dropdown(){
const mutual_fund_symbol_response = await fetch ('/api/metadata/instruments/symbols?portfolio_type=Mutual%20Fund', {
  method: 'GET'
})

mutual_fund_symbol_data = await mutual_fund_symbol_response.json();

const exchange_symbol = document.getElementById('exchange_symbol')
mutual_fund_symbol_data.all_symbols_list.forEach(element => {
exchange_symbol.innerHTML += `<option id = "options">${element['EXCHANGE_SYMBOL']}</option>`
});
}

/////////////////////////////////////////////////////////////////////////////////////////

document.getElementById('mf_txn_form').addEventListener('submit', async function (e) {
e.preventDefault();

const exchange_symbol = document.getElementById('exchange_symbol').value;
const txn_date        = document.getElementById('txn_date').value;
const txn_amount      = document.getElementById('txn_amount').value;
const txn_type        = document.getElementById('txn_type').value;
const amc_amount      = document.getElementById('amc_amount').value;

const mf_txn_payload = {
'EXCHANGE_SYMBOL'      : exchange_symbol
,'TXN_DATE'            : txn_date
,'TXN_AMOUNT'          : txn_amount
,'TXN_TYPE'            : txn_type
,'AMC_AMOUNT'          : amc_amount
}

const formData = new FormData();
formData.append('mf_txn_payload', JSON.stringify(mf_txn_payload));

const mf_txn_response = await fetch(`/user_txn/mf_txn/`, {
method: 'POST',
body: formData
})

const mf_txn_data = await mf_txn_response.json();

create_notification(mf_txn_data.message, mf_txn_data.status)
})