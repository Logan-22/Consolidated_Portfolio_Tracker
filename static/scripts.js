// Insert into corresponding NAV Table when the site loads

// Method : GET
// URL    : /api/hist_price/

document.addEventListener('DOMContentLoaded', async () => {
  if (document.getElementById('index_nav')) {
    await create_managed_tables_in_db()
    await create_portfolio_views_in_db()
    await upsert_price_table()
    await upsert_mf_hist_returns_table()
    await upsert_realised_stock_hist_returns_table()
    await upsert_realised_swing_stock_hist_returns_table()
    }
  }
);

// Create Managed Tables in DB

async function create_managed_tables_in_db(){

const create_table_response=  await fetch ('/api/create_managed_tables/', {
  method: 'GET'
})

const create_table_data_from_response = await create_table_response.json();

const resultDiv = document.getElementById('result')

if(create_table_data_from_response.status === "Success"){
    resultDiv.innerHTML += `<strong>${create_table_data_from_response.message}</strong></br>`
}
else{
    resultDiv.innerHTML += `<strong>${create_table_data_from_response.message}</strong></br>`
}
}

// Create Portfolio Views in DB

async function create_portfolio_views_in_db(){

const replace_view_response=  await fetch ('/api/create_portfolio_views/', {
  method: 'GET'
})

const replace_view_data = await replace_view_response.json();

const resultDiv = document.getElementById('result')

if(replace_view_data.status === "Success"){
    resultDiv.innerHTML += `<strong>${replace_view_data.message}</strong></br>`
}
else{
    resultDiv.innerHTML += `<strong>${replace_view_data.message}</strong></br>`
}
}

async function upsert_price_table(){

const today = new Date();
const year  = today.getFullYear();
const month = String(today.getMonth() + 1).padStart(2, '0');
const day   = String(today.getDate()).padStart(2, '0');

const end_date = `${year}-${month}-${day}` // Today in YYYY-MM-DD Format

const max_value_date_response = await fetch ('/api/price_table/max_value_date?process_flag=1', {
  method: 'GET'
})

const max_value_date_data_from_response = await max_value_date_response.json();

max_value_date_data_from_response.max_value_date_data.forEach(async element=> {
const alt_symbol      = element.alt_symbol
let start_date        = ""
if(element.max_value_date){
start_date      = element.max_value_date
}
else{
const week_before = new Date();
week_before.setDate(week_before.getDate() - 7);
const week_before_year  = week_before.getFullYear();
const week_before_month = String(week_before.getMonth() + 1).padStart(2, '0');
const week_before_day   = String(week_before.getDate()).padStart(2, '0');

const week_before_date = `${week_before_year}-${week_before_month}-${week_before_day}`
start_date             = week_before_date
}
const yahoo_symbol    = element.yahoo_symbol
const exchange_symbol = element.exchange_symbol
const portfolio_type  = element.portfolio_type

const formData = new FormData();
formData.append('alt_symbol', alt_symbol);
formData.append('yahoo_symbol', yahoo_symbol);
formData.append('exchange_symbol', exchange_symbol);
formData.append('portfolio_type', portfolio_type);

await fetch(`/api/price_table/close_price/${alt_symbol}?start_from=${start_date}&end_till=${end_date}`, {
method: 'POST',
body: formData
})
})

const dup_check_response = await fetch(`/api/price_table/duplicate_check/`, {
method: 'GET'
})

const dup_check_data_from_response = await dup_check_response.json();

const resultDiv = document.getElementById('result')
if(dup_check_data_from_response.status === "Success"){
    resultDiv.innerHTML += `<strong>${dup_check_data_from_response.message}</strong></br>`
    resultDiv.innerHTML += `<strong>${dup_check_data_from_response.status}</strong></br>`
}
else{
    resultDiv.innerHTML += `<strong>${dup_check_data_from_response.message}</strong></br>`
    resultDiv.innerHTML += `<strong>${dup_check_data_from_response.status}</strong></br>`
    dup_check_data_from_response.dup_check_data.forEach( element => {
    resultDiv.innerHTML += `<strong>Alt Symbol ${element.alt_symbol} is having ${element.count} entries for ${element.value_date} Date</strong></br>`
    })
    }
}

// GET /api/mf_hist_returns/max_date/

async function upsert_mf_hist_returns_table(){
const mf_hist_max_next_proc_date_response = await fetch ('/api/mf_hist_returns/max_next_proc_date/', {
  method: 'GET'
})

const mf_hist_max_next_proc_date_data = await mf_hist_max_next_proc_date_response.json();
const max_next_proc_date_in_mf_hist_returns = mf_hist_max_next_proc_date_data.data.max_next_processing_date

// Get the Max of all Nav tables and get the minimum out of the those

const price_table_max_date_response = await fetch ('/api/price_table/max_value_date?consider_for_hist_returns=1', {
  method: 'GET'
})

const price_table_max_date_data = await price_table_max_date_response.json();

let min_value_date = new Date()
let null_counter = 0

price_table_max_date_data.max_value_date_data.forEach(async element=> {
if (element.max_value_date){
const max_value_date = new Date(element.max_value_date)
if(max_value_date < min_value_date){
  min_value_date = max_value_date
}
}
else{
  null_counter += 1
}
})

if (null_counter == 0){

const year = min_value_date.getFullYear();
const month = String(min_value_date.getMonth() + 1).padStart(2, '0');
const day = String(min_value_date.getDate()).padStart(2, '0');

const end_date = `${year}-${month}-${day}` // Least date present in all NAV Tables

const response = await fetch (`/api/mf_hist_returns/${max_next_proc_date_in_mf_hist_returns}/${end_date}`, {
  method: 'GET'
})

const data = await response.json();

const resultDiv = document.getElementById('result')
resultDiv.innerHTML += `<strong>${data.message}</strong></br>`
resultDiv.innerHTML += `<strong>${data.status}</strong></br>`
}
else
{
const resultDiv = document.getElementById('result')
resultDiv.innerHTML += '<strong>Partial Load in the PRICE_TABLE</strong></br>'
resultDiv.innerHTML += '<strong>Error since ALT_SYMBOL(s) not present in PRICE_TABLE table</strong></br>'
}

}

// GET /api/realised_stock_hist_returns/max_trade_date/

async function upsert_realised_stock_hist_returns_table(){
const realised_stock_hist_max_trade_date_response = await fetch ('/api/realised_stock_hist_returns/max_trade_date/', {
  method: 'GET'
})

const realised_stock_hist_max_trade_date_data = await realised_stock_hist_max_trade_date_response.json();
const realised_stock_hist_max_trade_date = realised_stock_hist_max_trade_date_data.data.max_trade_date

const today = new Date();
const year = today.getFullYear();
const month = String(today.getMonth() + 1).padStart(2, '0');
const day = String(today.getDate()).padStart(2, '0');

const end_date = `${year}-${month}-${day}`

const response = await fetch (`/api/realised_stock_hist_returns/${realised_stock_hist_max_trade_date}/${end_date}`, {
  method: 'GET'
})

const data = await response.json();

const resultDiv = document.getElementById('result')
resultDiv.innerHTML += `<strong>${data.message}</strong></br>`
resultDiv.innerHTML += `<strong>${data.status}</strong></br>`

}

// GET /api/realised_swing_stock_hist_returns/max_trade_date/

async function upsert_realised_swing_stock_hist_returns_table(){
const realised_swing_stock_hist_max_trade_date_response = await fetch ('/api/realised_swing_stock_hist_returns/max_trade_close_date/', {
  method: 'GET'
})

const realised_swing_stock_hist_max_trade_date_data = await realised_swing_stock_hist_max_trade_date_response.json();
const realised_swing_stock_hist_max_trade_close_date = realised_swing_stock_hist_max_trade_date_data.data.max_trade_close_date

const today = new Date();
const year = today.getFullYear();
const month = String(today.getMonth() + 1).padStart(2, '0');
const day = String(today.getDate()).padStart(2, '0');

const end_date = `${year}-${month}-${day}`

const response = await fetch (`/api/realised_swing_stock_hist_returns/${realised_swing_stock_hist_max_trade_close_date}/${end_date}`, {
  method: 'GET'
})

const data = await response.json();

const resultDiv = document.getElementById('result')
resultDiv.innerHTML += `<strong>${data.message}</strong></br>`
resultDiv.innerHTML += `<strong>${data.status}</strong></br>`

}

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
