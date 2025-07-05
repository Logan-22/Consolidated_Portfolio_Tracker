
// MF Order Entry into Stock Order Table

// Method : POST
// URL    : /api/stock_order/

document.addEventListener('DOMContentLoaded', () => {
  if (document.getElementById('exchange_symbol')) {
    init_alt_symbol_dropdown();
  }
});

async function init_alt_symbol_dropdown(){
const stock_symbol_response = await fetch ('/api/metadata_store/symbols?portfolio_type=Stock', {
  method: 'GET'
})

const stock_symbol_data = await stock_symbol_response.json();
const exchange_symbol = document.getElementById('exchange_symbol')
stock_symbol_data.all_symbols_list.forEach(element => {
  exchange_symbol.innerHTML += `<option id = "options">${element.exchange_symbol}</option>`
});
}

/////////////////////////////////////////////////////////////////////////////////////////

document.getElementById('stock_order_entry_form').addEventListener('submit', async function (e) {
e.preventDefault();

const alt_symbol = document.getElementById('alt_symbol').value;
const trade_entry_date = document.getElementById('trade_entry_date').value;
const trade_entry_time = document.getElementById('trade_entry_time').value;
const trade_exit_date = document.getElementById('trade_exit_date').value;
const trade_exit_time = document.getElementById('trade_exit_time').value;
const stock_quantity = parseInt(document.getElementById('stock_quantity').value);
const trade_type = document.getElementById('trade_type').value;
const leverage = parseInt(document.getElementById('leverage').value);
const trade_position = document.getElementById('trade_position').value;
const stock_buy_price = parseFloat(document.getElementById('stock_buy_price').value);
const stock_sell_price = parseFloat(document.getElementById('stock_sell_price').value);
const brokerage = parseFloat(document.getElementById('brokerage').value);
const exchange_transaction_fees = parseFloat(document.getElementById('exchange_transaction_fees').value);
const igst = parseFloat(document.getElementById('igst').value);
const securities_transaction_tax = parseFloat(document.getElementById('securities_transaction_tax').value);
const sebi_turnover_fees = parseFloat(document.getElementById('sebi_turnover_fees').value);
let auto_square_off_charges = parseFloat(document.getElementById('auto_square_off_charges').value);
let depository_charges = parseFloat(document.getElementById('depository_charges').value);

// Derived Fields

let holding_days = null
let sell_minus_buy = null
let actual_p_l_w_o_leverage = null
let deployed_capital = null
let net_obligation = null
let total_fees = null
let net_receivable = null
let actual_p_l_w_leverage = null

if (!!trade_exit_date & !!trade_exit_time) {
  if (trade_type === "Intraday"){
    holding_days = 0
  } else{
    const trade_start_date = new Date(trade_entry_date);
    const trade_end_date   = new Date(trade_exit_date);
    const diffTime         = Math.abs(trade_end_date - trade_start_date);
    const diffDays         = Math.floor(diffTime / (1000 * 60 * 60 * 24)); 
    holding_days           = diffDays
  }
  sell_minus_buy = stock_sell_price - stock_buy_price
  if (trade_position === "Long"){
    actual_p_l_w_o_leverage = sell_minus_buy/stock_buy_price * 100
    deployed_capital = (stock_buy_price / leverage) * stock_quantity
  } else {
    actual_p_l_w_o_leverage = sell_minus_buy/stock_sell_price * 100
    deployed_capital = (stock_sell_price / leverage) * stock_quantity

  }
  if (! auto_square_off_charges){
    auto_square_off_charges = 0
  }
  if (! depository_charges){
    depository_charges = 0
  }
  if (! sebi_turnover_fees){
    sebi_turnover_fees = 0
  }
  if (! securities_transaction_tax){
    securities_transaction_tax = 0
  }
  if (! igst){
    igst = 0
  }
  if (! exchange_transaction_fees){
    exchange_transaction_fees = 0
  }
  if (! brokerage){
    brokerage = 0
  }
  net_obligation = sell_minus_buy * stock_quantity
  total_fees = brokerage + exchange_transaction_fees + igst + securities_transaction_tax + sebi_turnover_fees
  net_receivable = net_obligation - total_fees - auto_square_off_charges - depository_charges
  actual_p_l_w_leverage = net_receivable / deployed_capital * 100
}

const formData = new FormData();
formData.append('alt_symbol', alt_symbol);
formData.append('trade_entry_date', trade_entry_date);
formData.append('trade_entry_time', trade_entry_time);
formData.append('trade_exit_date', trade_exit_date);
formData.append('trade_exit_time', trade_exit_time);
formData.append('stock_quantity', stock_quantity);
formData.append('trade_type', trade_type);
formData.append('leverage', leverage);
formData.append('trade_position', trade_position);
formData.append('stock_buy_price', stock_buy_price);
formData.append('stock_sell_price', stock_sell_price);
formData.append('brokerage', brokerage);
formData.append('exchange_transaction_fees', exchange_transaction_fees);
formData.append('igst', igst);
formData.append('securities_transaction_tax', securities_transaction_tax);
formData.append('sebi_turnover_fees', sebi_turnover_fees);
formData.append('auto_square_off_charges', auto_square_off_charges);
formData.append('depository_charges', depository_charges);
// Derived Fields
formData.append('holding_days', holding_days);
formData.append('sell_minus_buy', sell_minus_buy);
formData.append('actual_p_l_w_o_leverage', actual_p_l_w_o_leverage);
formData.append('deployed_capital', deployed_capital);
formData.append('net_obligation', net_obligation);
formData.append('total_fees', total_fees);
formData.append('net_receivable', net_receivable);
formData.append('actual_p_l_w_leverage', actual_p_l_w_leverage);

const response = await fetch(`/api/stock_order/`, {
method: 'POST',
body: formData
})

const data = await response.json();
const resultDiv = document.getElementById('result')

if(data.status === "Success"){
    resultDiv.innerHTML = `<strong>${data.message}</strong>`
    //document.getElementById("stock_order_entry_form").reset();
}
else{
    resultDiv.innerHTML = `<strong>${data.message}</strong>`
}
})