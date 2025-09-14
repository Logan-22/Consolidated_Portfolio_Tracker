import { create_notification } from './create_notification.js'
import FormToTable from './form_to_table.js';

// MF Order Entry into MF Order Table

// Method : POST
// URL    : /api/mf_order/

document.addEventListener('DOMContentLoaded', () => {
  if (document.getElementById('exchange_symbol')) {
    init_mutual_fund_names_dropdown();
    const custom_fields = [
      {
        name: 'nav_during_purchase',
        populate: async (row_data) => await get_nav_price(row_data)
      },
      {
        name: 'units',
        populate: (row_data) => row_data['nav_during_purchase'] ? (row_data['amc_amount'] / row_data['nav_during_purchase']).toFixed(4) : ''
      }
    ]
    const column_types = {
      exchange_symbol: { type: 'string' }
      , txn_date: { type: 'date' }
      , txn_type: { type: 'string', allowed: ['Buy', 'Sell'] }
      , txn_amount: { type: 'number_gt_0' }
      , amc_amount: { type: 'number_gt_0' }
      , nav_during_purchase: { type: 'number_gt_0' }
      , units: { type: 'number_gt_0' }
    }
    const form_framework = new FormToTable('#mf_txn_form', 'mf_txn_payload', '#mf_txn_form_to_table', custom_fields, [], column_types, ['exchange_symbol'])

    form_framework.final_submit_button.addEventListener('click', async () => {
      const form_data = form_framework._final_submit() // Returns formdata from all rows
      if (form_data) {
        const mf_txns_load_response = await fetch(`/api/user_txn/mf_txn/`, {
          method: 'POST',
          body: form_data
        })

        const mf_txns_load_data = await mf_txns_load_response.json()
        create_notification(mf_txns_load_data.message, mf_txns_load_data.status)
      } else {
        create_notification('Transaction Failed. Invalid input data!', 'Failed')
      }
    })
  }
})

async function init_mutual_fund_names_dropdown() {
  const mutual_fund_symbol_response = await fetch('/api/metadata/instruments/symbols/?portfolio_type=Mutual%20Fund', {
    method: 'GET'
  })

  const mutual_fund_symbol_data = await mutual_fund_symbol_response.json();

  const exchange_symbol = document.getElementById('exchange_symbol')
  mutual_fund_symbol_data.all_symbols_list.forEach(element => {
    exchange_symbol.innerHTML += `<option id = "options">${element['EXCHANGE_SYMBOL']}</option>`
  });
}

async function get_nav_price(data) {
  const exchange_symbol = data['exchange_symbol'].replaceAll(' ', '%20')
  const txn_date = data['txn_date']
  const nav_during_purchase_response = await fetch(`/api/metrics/instrument_prices/price/?exchange_symbol=${exchange_symbol}&txn_date=${txn_date}`)

  const nav_during_purchase_data = await nav_during_purchase_response.json()
  return nav_during_purchase_data['price']
}

/////////////////////////////////////////////////////////////////////////////////////////



// document.getElementById('mf_txn_form').addEventListener('submit', async function (e) {
// e.preventDefault();

// const exchange_symbol = document.getElementById('exchange_symbol').value;
// const txn_date        = document.getElementById('txn_date').value;
// const txn_amount      = document.getElementById('txn_amount').value;
// const txn_type        = document.getElementById('txn_type').value;
// const amc_amount      = document.getElementById('amc_amount').value;
// const units           = document.getElementById('units').value;

// const mf_txn_payload = {
// 'EXCHANGE_SYMBOL'      : exchange_symbol
// ,'TXN_DATE'            : txn_date
// ,'TXN_AMOUNT'          : txn_amount
// ,'TXN_TYPE'            : txn_type
// ,'AMC_AMOUNT'          : amc_amount
// ,'UNITS'               : units
// }

// const formData = new FormData();
// formData.append('mf_txn_payload', JSON.stringify(mf_txn_payload));

// const mf_txn_response = await fetch(`/api/user_txn/mf_txn/`, {
// method: 'POST',
// body: formData
// })

// const mf_txn_data = await mf_txn_response.json();

// create_notification(mf_txn_data.message, mf_txn_data.status)
// })