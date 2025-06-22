// Insert into corresponding NAV Table when the site loads

// Method : GET
// URL    : /api/hist_price/

document.addEventListener('DOMContentLoaded', () => {
  if (document.getElementById('index_nav')) {
    create_mf_portfolio_view();
    upsert_price_tables();
    }
  }
);

async function create_mf_portfolio_view(){

const view_response=  await fetch ('/api/create_portfolio_view/', {
  method: 'GET'
})

const view_data = await view_response.json();

const resultDiv = document.getElementById('result')

if(view_data.status === "Success"){
    resultDiv.innerHTML += `<strong>${view_data.message}</strong></br>`
}
else{
    resultDiv.innerHTML += `<strong>${view_data.message}</strong></br>`
}
}

async function upsert_price_tables(){

const max_date_response = await fetch ('/api/price_tables/max_date/', {
  method: 'GET'
})

const max_date_data = await max_date_response.json();

for (table_name in max_date_data.max_date_from_tables){
const max_start_date = max_date_data.max_date_from_tables[table_name]
const alt_symbol = String(table_name).replaceAll('_', ' ').replaceAll("'", "")

const symbol_response = await fetch(`/api/symbol/${alt_symbol}/`, {
method: 'GET'
})

const symbol_data = await symbol_response.json();
const symbol = symbol_data.symbol_list[0]

const start_date = max_start_date

const today = new Date();
const year = today.getFullYear();
const month = String(today.getMonth() + 1).padStart(2, '0');
const day = String(today.getDate()).padStart(2, '0');

const end_date = `${year}-${month}-${day}`
const formData = new FormData();
formData.append('alt_symbol', alt_symbol);

await fetch(`/api/hist_price/${symbol}/${start_date}/${end_date}`, {
method: 'POST',
body: formData
})

//end of for loop
}

const dup_check_response = await fetch(`/api/nav_tables/dup_check/`, {
method: 'GET'
})

const dup_check_data = await dup_check_response.json();

const resultDiv = document.getElementById('result')
if(dup_check_data.status === "Success"){
    resultDiv.innerHTML += `<strong>${dup_check_data.message}</strong></br>`
    resultDiv.innerHTML += `<strong>${dup_check_data.status}</strong></br>`
}
else{
    resultDiv.innerHTML += `<strong>${dup_check_data.message}</strong></br>`
    for (dup_table in dup_check_data.dup_tables){
    resultDiv.innerHTML += `<strong>${dup_check_data.status}</strong></br>`
      resultDiv.innerHTML += `<strong>${dup_table} ---> ${dup_check_data.dup_tables[dup_table]} Value Date</strong></br>`
    }
}
upsert_mf_hist_returns_table();
upsert_realised_stock_hist_returns_table();
upsert_realised_swing_stock_hist_returns_table();
}

// GET /api/mf_hist_returns/max_date/

async function upsert_mf_hist_returns_table(){
const mf_hist_max_next_proc_date_response = await fetch ('/api/mf_hist_returns/max_next_proc_date/', {
  method: 'GET'
})

const mf_hist_max_next_proc_date_data = await mf_hist_max_next_proc_date_response.json();
const max_next_proc_date_in_mf_hist_returns = mf_hist_max_next_proc_date_data.data.max_next_processing_date

// Get the Max of all Nav tables and get the minimum out of the those

const nav_max_date_response = await fetch ('/api/all_price_tables/max_date/', {
  method: 'GET'
})

const nav_max_date_data = await nav_max_date_response.json();

let min_nav_date = new Date()

Object.values(nav_max_date_data.max_date_from_tables).forEach(date => {
  date = new Date(date)
  if ( date < min_nav_date){
    min_nav_date = date
  }
})

const year = min_nav_date.getFullYear();
const month = String(min_nav_date.getMonth() + 1).padStart(2, '0');
const day = String(min_nav_date.getDate()).padStart(2, '0');

const end_date = `${year}-${month}-${day}` // Least date present in all NAV Tables

const response = await fetch (`/api/mf_hist_returns/${max_next_proc_date_in_mf_hist_returns}/${end_date}`, {
  method: 'GET'
})

const data = await response.json();

const resultDiv = document.getElementById('result')
resultDiv.innerHTML += `<strong>${data.message}</strong></br>`
resultDiv.innerHTML += `<strong>${data.status}</strong></br>`

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
