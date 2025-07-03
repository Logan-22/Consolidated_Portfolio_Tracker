// Insert into corresponding NAV Table when the site loads

// Method : GET
// URL    : /api/hist_price/

// Global
let latest_consolidated_returns_data;
let latest_agg_returns_data;
let processing_date_value;

document.addEventListener('DOMContentLoaded', async () => {
  if (document.getElementById('index_nav')) {
    await create_managed_tables_in_db()
    await create_portfolio_views_in_db()
    await upsert_price_table()
    await upsert_mf_hist_returns_table()
    await upsert_realised_intraday_stock_hist_returns_table()
    await upsert_realised_swing_stock_hist_returns_table()
    await upsert_unrealised_swing_stock_hist_returns_table()
    await upsert_consolidated_hist_returns()
    await get_consolidated_hist_returns()
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

const price_table_max_date_response = await fetch ('/api/price_table/max_value_date?consider_for_hist_returns=1&portfolio_type=Mutual+Fund', {
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

// GET /api/realised_intraday_stock_hist_returns/max_trade_date/

async function upsert_realised_intraday_stock_hist_returns_table(){
const realised_stock_hist_max_trade_date_response = await fetch ('/api/realised_intraday_stock_hist_returns/max_trade_date/', {
  method: 'GET'
})

const realised_stock_hist_max_trade_date_data = await realised_stock_hist_max_trade_date_response.json();
const realised_stock_hist_max_trade_date = realised_stock_hist_max_trade_date_data.data.max_trade_date

const today = new Date();
const year = today.getFullYear();
const month = String(today.getMonth() + 1).padStart(2, '0');
const day = String(today.getDate()).padStart(2, '0');

const end_date = `${year}-${month}-${day}`

const response = await fetch (`/api/realised_intraday_stock_hist_returns/${realised_stock_hist_max_trade_date}/${end_date}`, {
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

// GET /api/realised_swing_stock_hist_returns/max_next_proc_date/

async function upsert_unrealised_swing_stock_hist_returns_table(){
const unrealised_returns_max_next_proc_date_response = await fetch ('/api/unrealised_swing_stock_hist_returns/max_next_proc_date/', {
  method: 'GET'
})

const unrealised_returns_max_next_proc_date_data = await unrealised_returns_max_next_proc_date_response.json();
const max_next_proc_date_in_unrealised_returns = unrealised_returns_max_next_proc_date_data.data.max_next_processing_date

// Get the Max of all Nav tables and get the minimum out of the those

const price_table_max_date_response = await fetch ('/api/price_table/max_value_date?consider_for_hist_returns=1&portfolio_type=Stock', {
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

const response = await fetch (`/api/unrealised_stock_hist_returns/${max_next_proc_date_in_unrealised_returns}/${end_date}`, {
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

async function upsert_consolidated_hist_returns(){
const consolidated_returns_max_next_proc_date_response = await fetch ('/api/consolidated_hist_returns/max_next_proc_date/', {
  method: 'GET'
})

const consolidated_returns_max_next_proc_date_data = await consolidated_returns_max_next_proc_date_response.json();
const max_next_proc_date_in_consolidated_returns = consolidated_returns_max_next_proc_date_data.data.max_next_processing_date

// Get the Max of all Hist Returns tables and get the minimum out of the those

const min_date_from_hist_returns_table_max_date_response = await fetch ('/api/hist_returns_tables/max_processing_date/', {
  method: 'GET'
})

const min_date_from_hist_returns_table_max_date_data = await min_date_from_hist_returns_table_max_date_response.json();
const min_date_from_hist_returns_table_max_date = min_date_from_hist_returns_table_max_date_data.max_proc_date_data.min_of_max_proc_date_from_hist_tables

const today = new Date();
const year = today.getFullYear();
const month = String(today.getMonth() + 1).padStart(2, '0');
const day = String(today.getDate()).padStart(2, '0');

let end_date = `${year}-${month}-${day}`
if(min_date_from_hist_returns_table_max_date){
end_date = min_date_from_hist_returns_table_max_date
}

const process_consolidated_returns_response = await fetch (`/api/consolidated_stock_hist_returns/${max_next_proc_date_in_consolidated_returns}/${end_date}`, {
  method: 'GET'
})

const process_consolidated_returns_data = await process_consolidated_returns_response.json();

const resultDiv = document.getElementById('result')
resultDiv.innerHTML += `<strong>${process_consolidated_returns_data.message}</strong></br>`
resultDiv.innerHTML += `<strong>${process_consolidated_returns_data.status}</strong></br>`
}

async function get_consolidated_hist_returns(){
const consolidated_returns_response = await fetch ('/api/consolidated_hist_returns/all/', {
  method: 'GET'
})

const consolidated_returns_data = await consolidated_returns_response.json();
console.log(consolidated_returns_data)

const total_invested_amount = document.getElementById("total_invested_amount")
const current_value         = document.getElementById("current_value")
const previous_value        = document.getElementById("previous_value")
const p_l                   = document.getElementById("p_l")
const perc_p_l              = document.getElementById("perc_p_l")
const day_p_l               = document.getElementById("day_p_l")
const perc_day_p_l          = document.getElementById("perc_day_p_l")

const mf_invested_amount                = document.getElementById("mf_invested_amount")
const mf_current_value                  = document.getElementById("mf_current_value")
const mf_previous_value                 = document.getElementById("mf_previous_value")
const mf_p_l                            = document.getElementById("mf_p_l")
const mf_perc_p_l                       = document.getElementById("mf_perc_p_l")
const mf_day_p_l                        = document.getElementById("mf_day_p_l")
const mf_perc_day_p_l                   = document.getElementById("mf_perc_day_p_l")

const unrealised_swing_invested_amount  = document.getElementById("unrealised_swing_invested_amount")
const unrealised_swing_current_value    = document.getElementById("unrealised_swing_current_value")
const unrealised_swing_previous_value   = document.getElementById("unrealised_swing_previous_value")
const unrealised_swing_p_l              = document.getElementById("unrealised_swing_p_l")
const unrealised_swing_perc_p_l         = document.getElementById("unrealised_swing_perc_p_l")
const unrealised_swing_day_p_l          = document.getElementById("unrealised_swing_day_p_l")
const unrealised_swing_perc_day_p_l     = document.getElementById("unrealised_swing_perc_day_p_l")

const realised_swing_invested_amount    = document.getElementById("realised_swing_invested_amount")
const realised_swing_current_value      = document.getElementById("realised_swing_current_value")
const realised_swing_previous_value     = document.getElementById("realised_swing_previous_value")
const realised_swing_p_l                = document.getElementById("realised_swing_p_l")
const realised_swing_perc_p_l           = document.getElementById("realised_swing_perc_p_l")

const realised_intraday_invested_amount = document.getElementById("realised_intraday_invested_amount")
const realised_intraday_current_value   = document.getElementById("realised_intraday_current_value")
const realised_intraday_previous_value  = document.getElementById("realised_intraday_previous_value")
const realised_intraday_p_l             = document.getElementById("realised_intraday_p_l")
const realised_intraday_perc_p_l        = document.getElementById("realised_intraday_perc_p_l")

if(consolidated_returns_data.data.latest_cons_data){
latest_consolidated_returns_data = consolidated_returns_data.data.latest_cons_data[0]

total_invested_amount.textContent = latest_consolidated_returns_data.fin_invested_amount
current_value.textContent         = latest_consolidated_returns_data.fin_current_value
previous_value.textContent        = latest_consolidated_returns_data.fin_previous_value
p_l.textContent                   = latest_consolidated_returns_data.fin_total_p_l
perc_p_l.textContent              = latest_consolidated_returns_data.perc_fin_total_p_l
day_p_l.textContent               = latest_consolidated_returns_data.fin_day_p_l
perc_day_p_l.textContent          = latest_consolidated_returns_data.perc_fin_day_p_l
}

if(consolidated_returns_data.data.latest_agg_data){
latest_agg_returns_data = consolidated_returns_data.data.latest_agg_data

latest_agg_returns_data.forEach(element => {
if(element.portfolio_type == "Mutual Funds"){
mf_invested_amount.textContent                     = element.agg_total_invested_amount
mf_current_value.textContent                       = element.agg_current_value
mf_previous_value.textContent                      = element.agg_previous_value
mf_p_l.textContent                                 = element.agg_total_p_l
mf_perc_p_l.textContent                            = element.perc_agg_total_p_l
mf_day_p_l.textContent                             = element.agg_day_p_l
mf_perc_day_p_l.textContent                        = element.perc_agg_day_p_l
}
else if(element.portfolio_type == "Unrealised Swing Stocks"){
unrealised_swing_invested_amount.textContent       = element.agg_total_invested_amount
unrealised_swing_current_value.textContent         = element.agg_current_value
unrealised_swing_previous_value.textContent        = element.agg_previous_value
unrealised_swing_p_l.textContent                   = element.agg_total_p_l
unrealised_swing_perc_p_l.textContent              = element.perc_agg_total_p_l
unrealised_swing_day_p_l.textContent               = element.agg_day_p_l
unrealised_swing_perc_day_p_l.textContent          = element.perc_agg_day_p_l
}
else if(element.portfolio_type == "Realised Swing Stocks"){
realised_swing_invested_amount.textContent         = element.agg_total_invested_amount
realised_swing_current_value.textContent           = element.agg_current_value
realised_swing_previous_value.textContent          = element.agg_previous_value
realised_swing_p_l.textContent                     = element.agg_total_p_l
realised_swing_perc_p_l.textContent                = element.perc_agg_total_p_l
}
else if(element.portfolio_type == "Intraday Stocks"){
realised_intraday_invested_amount.textContent      = element.agg_total_invested_amount
realised_intraday_current_value.textContent        = element.agg_current_value
realised_intraday_previous_value.textContent       = element.agg_previous_value
realised_intraday_p_l.textContent                  = element.agg_total_p_l
realised_intraday_perc_p_l.textContent             = element.perc_agg_total_p_l
}
})
}

// Reveal Processing Date Picker

const processing_date = document.getElementsByName("processing_date")
processing_date.forEach(element => element.classList = "")

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
