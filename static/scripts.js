import { create_notification } from './create_notification.js'
//create_notification(message, type, duration)

// Insert into corresponding NAV Table when the site loads

// Method : GET
// URL    : /api/hist_price/

// Global
let latest_consolidated_returns_data;
let latest_agg_returns_data;
let cons_returns_data;
let agg_returns_data;
let processing_date_value;
let create_table_status;
let create_view_status;
let dup_check_status;
let mf_hist_status;
let realised_intraday_hist_status;
let realised_swing_hist_status;
let unrealised_swing_hist_status;
let process_consolidated_hist_status;
let get_consolidated_hist_status;

const total_invested_amount             = document.getElementById("total_invested_amount")
const current_value                     = document.getElementById("current_value")
const previous_value                    = document.getElementById("previous_value")
const p_l                               = document.getElementById("p_l")
const perc_p_l                          = document.getElementById("perc_p_l")
const day_p_l                           = document.getElementById("day_p_l")
const perc_day_p_l                      = document.getElementById("perc_day_p_l")

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

document.addEventListener('DOMContentLoaded', async () => {
  if (document.getElementById('container')) {
    await create_managed_tables_in_db()
    await create_portfolio_views_in_db()
    await upsert_price_table()
    await upsert_mf_hist_returns_table()
    await upsert_realised_intraday_stock_hist_returns_table()
    await upsert_realised_swing_stock_hist_returns_table()
    await upsert_unrealised_swing_stock_hist_returns_table()
    await upsert_consolidated_hist_returns()
    await get_consolidated_hist_returns()
    await create_consolidated_notification()
    }
  }
);

// Create Managed Tables in DB

async function create_managed_tables_in_db(){

const create_table_response=  await fetch ('/api/create_managed_tables/', {
  method: 'GET'
})

const create_table_data = await create_table_response.json();

create_table_status = create_table_data.status
if(create_table_status != "Success"){
create_notification(create_table_data.message, create_table_data.status)
}

}

// Create Portfolio Views in DB

async function create_portfolio_views_in_db(){

const create_view_response=  await fetch ('/api/create_portfolio_views/', {
  method: 'GET'
})

const create_view_data = await create_view_response.json();

create_view_status = create_view_data.status
if(create_view_status != "Success"){
create_notification(create_view_data.message, create_view_data.status)
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

dup_check_status = dup_check_data_from_response.status
if(dup_check_status != "Success"){
dup_check_data_from_response.dup_check_data.forEach( duplicate => {
create_notification(`Symbol ${duplicate.alt_symbol} is having ${duplicate.count} entries for ${duplicate.value_date} Date`, dup_check_data_from_response.status)
})}

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

const mf_hist_returns_response = await fetch (`/api/mf_hist_returns/${max_next_proc_date_in_mf_hist_returns}/${end_date}`, {
  method: 'GET'
})

const mf_hist_returns_data = await mf_hist_returns_response.json();

mf_hist_status = mf_hist_returns_data.status
if(mf_hist_status != "Success"){
create_notification(mf_hist_returns_data.message, mf_hist_returns_data.status)
}

}
else
{
create_notification('Partial Load in the PRICE_TABLE', 'error')
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

const realised_intraday_hist_returns_response = await fetch (`/api/realised_intraday_stock_hist_returns/${realised_stock_hist_max_trade_date}/${end_date}`, {
  method: 'GET'
})

const realised_intraday_hist_returns_data = await realised_intraday_hist_returns_response.json();

realised_intraday_hist_status = realised_intraday_hist_returns_data.status
if(realised_intraday_hist_status != "Success"){
create_notification(realised_intraday_hist_returns_data.message, realised_intraday_hist_returns_data.status)
}

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

const realised_swing_hist_returns_response = await fetch (`/api/realised_swing_stock_hist_returns/${realised_swing_stock_hist_max_trade_close_date}/${end_date}`, {
  method: 'GET'
})

const realised_swing_hist_returns_data = await realised_swing_hist_returns_response.json();

realised_swing_hist_status = realised_swing_hist_returns_data.status
if(realised_swing_hist_status != "Success"){
create_notification(realised_swing_hist_returns_data.message, realised_swing_hist_returns_data.status)
}

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

const unrealised_swing_returns_response = await fetch (`/api/unrealised_stock_hist_returns/${max_next_proc_date_in_unrealised_returns}/${end_date}`, {
  method: 'GET'
})

const unrealised_swing_returns_data = await unrealised_swing_returns_response.json();

unrealised_swing_hist_status = unrealised_swing_returns_data.status
if(unrealised_swing_hist_status != "Success"){
create_notification(unrealised_swing_returns_data.message, unrealised_swing_returns_data.status)
}

}
else
{
create_notification('Partial Load in the PRICE_TABLE', 'error')
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

process_consolidated_hist_status = process_consolidated_returns_data.status
if(process_consolidated_hist_status != "Success"){
create_notification(process_consolidated_returns_data.message, process_consolidated_returns_data.status)
}

}

async function get_consolidated_hist_returns(){
const consolidated_returns_response = await fetch ('/api/consolidated_hist_returns/all/', {
  method: 'GET'
})

const consolidated_returns_data = await consolidated_returns_response.json();
console.log(consolidated_returns_data)
get_consolidated_hist_status = consolidated_returns_data.status
if(get_consolidated_hist_status != "Success"){
create_notification(consolidated_returns_data.message, consolidated_returns_data.status)
}

if(consolidated_returns_data.data.latest_cons_data){
latest_consolidated_returns_data = consolidated_returns_data.data.latest_cons_data[0]

total_invested_amount.textContent = `₹ ${latest_consolidated_returns_data.fin_invested_amount.toLocaleString('en-IN')}`
current_value.textContent         = `₹ ${latest_consolidated_returns_data.fin_current_value.toLocaleString('en-IN')}`
previous_value.textContent        = `₹ ${latest_consolidated_returns_data.fin_previous_value.toLocaleString('en-IN')}`
p_l.textContent                   = `₹ ${latest_consolidated_returns_data.fin_total_p_l.toLocaleString('en-IN')}`
perc_p_l.textContent              = `${latest_consolidated_returns_data.perc_fin_total_p_l} %`
day_p_l.textContent               = `₹ ${latest_consolidated_returns_data.fin_day_p_l.toLocaleString('en-IN')}`
perc_day_p_l.textContent          = `${latest_consolidated_returns_data.perc_fin_day_p_l} %`
}

if(consolidated_returns_data.data.latest_agg_data){
latest_agg_returns_data = consolidated_returns_data.data.latest_agg_data

latest_agg_returns_data.forEach(element => {
if(element.portfolio_type == "Mutual Funds"){
mf_invested_amount.textContent                     = `₹ ${element.agg_total_invested_amount.toLocaleString('en-IN')}`
mf_current_value.textContent                       = `₹ ${element.agg_current_value.toLocaleString('en-IN')}`
mf_previous_value.textContent                      = `₹ ${element.agg_previous_value.toLocaleString('en-IN')}`
mf_p_l.textContent                                 = `₹ ${element.agg_total_p_l.toLocaleString('en-IN')}`
mf_perc_p_l.textContent                            = `${element.perc_agg_total_p_l} %`
mf_day_p_l.textContent                             = `₹ ${element.agg_day_p_l.toLocaleString('en-IN')}`
mf_perc_day_p_l.textContent                        = `${element.perc_agg_day_p_l} %`
}
else if(element.portfolio_type == "Unrealised Swing Stocks"){
unrealised_swing_invested_amount.textContent       = `₹ ${element.agg_total_invested_amount.toLocaleString('en-IN')}`
unrealised_swing_current_value.textContent         = `₹ ${element.agg_current_value.toLocaleString('en-IN')}`
unrealised_swing_previous_value.textContent        = `₹ ${element.agg_previous_value.toLocaleString('en-IN')}`
unrealised_swing_p_l.textContent                   = `₹ ${element.agg_total_p_l.toLocaleString('en-IN')}`
unrealised_swing_perc_p_l.textContent              = `${element.perc_agg_total_p_l} %`
unrealised_swing_day_p_l.textContent               = `₹ ${element.agg_day_p_l.toLocaleString('en-IN')}`
unrealised_swing_perc_day_p_l.textContent          = `${element.perc_agg_day_p_l} %`
}
else if(element.portfolio_type == "Realised Swing Stocks"){
realised_swing_invested_amount.textContent         = `₹ ${element.agg_total_invested_amount.toLocaleString('en-IN')}`
realised_swing_current_value.textContent           = `₹ ${element.agg_current_value.toLocaleString('en-IN')}`
realised_swing_previous_value.textContent          = `₹ ${element.agg_previous_value.toLocaleString('en-IN')}`
realised_swing_p_l.textContent                     = `₹ ${element.agg_total_p_l.toLocaleString('en-IN')}`
realised_swing_perc_p_l.textContent                = `${element.perc_agg_total_p_l} %`
}
else if(element.portfolio_type == "Intraday Stocks"){
realised_intraday_invested_amount.textContent      = `₹ ${element.agg_total_invested_amount.toLocaleString('en-IN')}`
realised_intraday_current_value.textContent        = `₹ ${element.agg_current_value.toLocaleString('en-IN')}`
realised_intraday_previous_value.textContent       = `₹ ${element.agg_previous_value.toLocaleString('en-IN')}`
realised_intraday_p_l.textContent                  = `₹ ${element.agg_total_p_l.toLocaleString('en-IN')}`
realised_intraday_perc_p_l.textContent             = `${element.perc_agg_total_p_l} %`
}
})
}

if(consolidated_returns_data.data.agg_data){
agg_returns_data = consolidated_returns_data.data.agg_data
}

if(consolidated_returns_data.data.cons_data){
cons_returns_data = consolidated_returns_data.data.cons_data
}

// Reveal Processing Date Picker

const processing_date = document.getElementsByName("processing_date")
processing_date.forEach(element => element.classList = "")

const processing_date_input = document.getElementById("processing_date")
processing_date_input.value = latest_consolidated_returns_data.processing_date

await add_class_list_based_on_value();

processing_date_input.addEventListener("change", e => change_processing_date(e))

}

async function add_class_list_based_on_value(){

const p_l_value = Number(p_l.textContent.split(" ")[1].replaceAll(",",""))
if (p_l_value > 0){
p_l.classList = "color-profit"
} else if (p_l_value < 0){
p_l.classList = "color-loss"
} else{
p_l.classList = "color-neutral"
}

const perc_p_l_value = Number(perc_p_l.textContent.split(" ")[0].replaceAll(",",""))
if (perc_p_l_value > 0){
perc_p_l.classList = "color-profit"
} else if (perc_p_l_value < 0){
perc_p_l.classList = "color-loss"
} else{
perc_p_l.classList = "color-neutral"
}

const day_p_l_value = Number(day_p_l.textContent.split(" ")[1].replaceAll(",",""))
if (day_p_l_value > 0){
day_p_l.classList = "color-profit"
} else if (day_p_l_value < 0){
day_p_l.classList = "color-loss"
} else{
day_p_l.classList = "color-neutral"
}

const perc_day_p_l_value = Number(perc_day_p_l.textContent.split(" ")[0].replaceAll(",",""))
if (perc_day_p_l_value > 0){
perc_day_p_l.classList = "color-profit"
} else if (perc_day_p_l_value < 0){
perc_day_p_l.classList = "color-loss"
} else{
perc_day_p_l.classList = "color-neutral"
}

const mf_p_l_value = Number(mf_p_l.textContent.split(" ")[1].replaceAll(",",""))
const mf_card      = document.getElementById("mf_card")
if (mf_p_l_value > 0){
mf_p_l.classList  = "color-profit"
mf_card.classList += " border-left-profit"
} else if (mf_p_l_value < 0){
mf_p_l.classList  = "color-loss"
mf_card.classList += " border-left-loss"
} else{
mf_p_l.classList  = "color-neutral"
mf_card.classList += " border-left-neutral"
}

const mf_perc_p_l_value = Number(mf_perc_p_l.textContent.split(" ")[0].replaceAll(",",""))
if (mf_perc_p_l_value > 0){
mf_perc_p_l.classList  = "color-profit"
} else if (mf_perc_p_l_value < 0){
mf_perc_p_l.classList  = "color-loss"
} else{
mf_perc_p_l.classList  = "color-neutral"
}

const mf_day_p_l_value = Number(mf_day_p_l.textContent.split(" ")[1].replaceAll(",",""))
if (mf_day_p_l_value > 0){
mf_day_p_l.classList  = "color-profit"
} else if (mf_day_p_l_value < 0){
mf_day_p_l.classList  = "color-loss"
} else{
mf_day_p_l.classList  = "color-neutral"
}

const mf_perc_day_p_l_value = Number(mf_perc_day_p_l.textContent.split(" ")[0].replaceAll(",",""))
if (mf_perc_day_p_l_value > 0){
mf_perc_day_p_l.classList  = "color-profit"
} else if (mf_perc_day_p_l_value < 0){
mf_perc_day_p_l.classList  = "color-loss"
} else{
mf_perc_day_p_l.classList  = "color-neutral"
}


const unrealised_swing_p_l_value = Number(unrealised_swing_p_l.textContent.split(" ")[1].replaceAll(",",""))
const unrealised_swing_card      = document.getElementById("unrealised_swing_card")
if (unrealised_swing_p_l_value > 0){
unrealised_swing_p_l.classList  = "color-profit"
unrealised_swing_card.classList += " border-left-profit"
} else if (unrealised_swing_p_l_value < 0){
unrealised_swing_p_l.classList  = "color-loss"
unrealised_swing_card.classList += " border-left-loss"
} else{
unrealised_swing_p_l.classList  = "color-neutral"
unrealised_swing_card.classList += " border-left-neutral"
}

const unrealised_swing_perc_p_l_value = Number(unrealised_swing_perc_p_l.textContent.split(" ")[0].replaceAll(",",""))
if (unrealised_swing_perc_p_l_value > 0){
unrealised_swing_perc_p_l.classList  = "color-profit"
} else if (unrealised_swing_perc_p_l_value < 0){
unrealised_swing_perc_p_l.classList  = "color-loss"
} else{
unrealised_swing_perc_p_l.classList  = "color-neutral"
}

const unrealised_swing_day_p_l_value = Number(unrealised_swing_day_p_l.textContent.split(" ")[1].replaceAll(",",""))
if (unrealised_swing_day_p_l_value > 0){
unrealised_swing_day_p_l.classList  = "color-profit"
} else if (unrealised_swing_day_p_l_value < 0){
unrealised_swing_day_p_l.classList  = "color-loss"
} else{
unrealised_swing_day_p_l.classList  = "color-neutral"
}

const unrealised_swing_perc_day_p_l_value = Number(unrealised_swing_perc_day_p_l.textContent.split(" ")[0].replaceAll(",",""))
if (unrealised_swing_perc_day_p_l_value > 0){
unrealised_swing_perc_day_p_l.classList  = "color-profit"
} else if (unrealised_swing_perc_day_p_l_value < 0){
unrealised_swing_perc_day_p_l.classList  = "color-loss"
} else{
unrealised_swing_perc_day_p_l.classList  = "color-neutral"
}


const realised_swing_p_l_value = Number(realised_swing_p_l.textContent.split(" ")[1].replaceAll(",",""))
const realised_swing_card      = document.getElementById("realised_swing_card")
if (realised_swing_p_l_value > 0){
realised_swing_p_l.classList  = "color-profit"
realised_swing_card.classList += " border-left-profit"
} else if (realised_swing_p_l_value < 0){
realised_swing_p_l.classList  = "color-loss"
realised_swing_card.classList += " border-left-loss"
} else{
realised_swing_p_l.classList  = "color-neutral"
realised_swing_card.classList += " border-left-neutral"
}

const realised_swing_perc_p_l_value = Number(realised_swing_perc_p_l.textContent.split(" ")[0].replaceAll(",",""))
if (realised_swing_perc_p_l_value > 0){
realised_swing_perc_p_l.classList  = "color-profit"
} else if (realised_swing_perc_p_l_value < 0){
realised_swing_perc_p_l.classList  = "color-loss"
} else{
realised_swing_perc_p_l.classList  = "color-neutral"
}


const realised_intraday_p_l_value = Number(realised_intraday_p_l.textContent.split(" ")[1].replaceAll(",",""))
const realised_intraday_card      = document.getElementById("realised_intraday_card")
if (realised_intraday_p_l_value > 0){
realised_intraday_p_l.classList  = "color-profit"
realised_intraday_card.classList += " border-left-profit"
} else if (realised_intraday_p_l_value < 0){
realised_intraday_p_l.classList  = "color-loss"
realised_intraday_card.classList += " border-left-loss"
} else{
realised_intraday_p_l.classList  = "color-neutral"
realised_intraday_card.classList += " border-left-neutral"
}

const realised_intraday_perc_p_l_value = Number(realised_intraday_perc_p_l.textContent.split(" ")[0].replaceAll(",",""))
if (realised_intraday_perc_p_l_value > 0){
realised_intraday_perc_p_l.classList  = "color-profit"
} else if (realised_intraday_perc_p_l_value < 0){
realised_intraday_perc_p_l.classList  = "color-loss"
} else{
realised_intraday_perc_p_l.classList  = "color-neutral"
}
}

async function create_consolidated_notification(){
if (create_table_status              == "Success" &&
    create_view_status               == "Success" &&
    dup_check_status                 == "Success" &&
    mf_hist_status                   == "Success" &&
    realised_intraday_hist_status    == "Success" &&
    realised_swing_hist_status       == "Success" &&
    unrealised_swing_hist_status     == "Success" &&
    process_consolidated_hist_status == "Success" &&
    get_consolidated_hist_status     == "Success"){
    create_notification('All scheduled background processes executed successfully.', 'success')
    }
}

async function change_processing_date(e){
e.preventDefault();
processing_date_value = document.getElementById("processing_date").value

let found_cons_data = 0

cons_returns_data.forEach(element => {
if(element.processing_date == processing_date_value || (processing_date_value > element.processing_date && processing_date_value < element.next_processing_date)){
total_invested_amount.textContent = `₹ ${element.fin_invested_amount.toLocaleString('en-IN')}`
current_value.textContent         = `₹ ${element.fin_current_value.toLocaleString('en-IN')}`
previous_value.textContent        = `₹ ${element.fin_previous_value.toLocaleString('en-IN')}`
p_l.textContent                   = `₹ ${element.fin_total_p_l.toLocaleString('en-IN')}`
perc_p_l.textContent              = `${element.perc_fin_total_p_l} %`
day_p_l.textContent               = `₹ ${element.fin_day_p_l.toLocaleString('en-IN')}`
perc_day_p_l.textContent          = `${element.perc_fin_day_p_l} %`
found_cons_data                   = 1
}
})

if (found_cons_data == 0) {
total_invested_amount.textContent = "₹ 0"
current_value.textContent         = "₹ 0"
previous_value.textContent        = "₹ 0"
p_l.textContent                   = "₹ 0"
perc_p_l.textContent              = "0 %"
day_p_l.textContent               = "₹ 0"
perc_day_p_l.textContent          = "0 %"
}

let found_agg_data = 0

agg_returns_data.forEach(element => {
if(element.processing_date == processing_date_value || (processing_date_value > element.processing_date && processing_date_value < element.next_processing_date)){
found_agg_data = 1
if(element.portfolio_type == "Mutual Funds"){
mf_invested_amount.textContent                     = `₹ ${element.agg_total_invested_amount.toLocaleString('en-IN')}`
mf_current_value.textContent                       = `₹ ${element.agg_current_value.toLocaleString('en-IN')}`
mf_previous_value.textContent                      = `₹ ${element.agg_previous_value.toLocaleString('en-IN')}`
mf_p_l.textContent                                 = `₹ ${element.agg_total_p_l.toLocaleString('en-IN')}`
mf_perc_p_l.textContent                            = `${element.perc_agg_total_p_l} %`
mf_day_p_l.textContent                             = `₹ ${element.agg_day_p_l.toLocaleString('en-IN')}`
mf_perc_day_p_l.textContent                        = `${element.perc_agg_day_p_l} %`
}
else if(element.portfolio_type == "Unrealised Swing Stocks"){
unrealised_swing_invested_amount.textContent       = `₹ ${element.agg_total_invested_amount.toLocaleString('en-IN')}`
unrealised_swing_current_value.textContent         = `₹ ${element.agg_current_value.toLocaleString('en-IN')}`
unrealised_swing_previous_value.textContent        = `₹ ${element.agg_previous_value.toLocaleString('en-IN')}`
unrealised_swing_p_l.textContent                   = `₹ ${element.agg_total_p_l.toLocaleString('en-IN')}`
unrealised_swing_perc_p_l.textContent              = `${element.perc_agg_total_p_l} %`
unrealised_swing_day_p_l.textContent               = `₹ ${element.agg_day_p_l.toLocaleString('en-IN')}`
unrealised_swing_perc_day_p_l.textContent          = `${element.perc_agg_day_p_l} %`
}
else if(element.portfolio_type == "Realised Swing Stocks"){
realised_swing_invested_amount.textContent         = `₹ ${element.agg_total_invested_amount.toLocaleString('en-IN')}`
realised_swing_current_value.textContent           = `₹ ${element.agg_current_value.toLocaleString('en-IN')}`
realised_swing_previous_value.textContent          = `₹ ${element.agg_previous_value.toLocaleString('en-IN')}`
realised_swing_p_l.textContent                     = `₹ ${element.agg_total_p_l.toLocaleString('en-IN')}`
realised_swing_perc_p_l.textContent                = `${element.perc_agg_total_p_l} %`
}
else if(element.portfolio_type == "Intraday Stocks"){
realised_intraday_invested_amount.textContent      = `₹ ${element.agg_total_invested_amount.toLocaleString('en-IN')}`
realised_intraday_current_value.textContent        = `₹ ${element.agg_current_value.toLocaleString('en-IN')}`
realised_intraday_previous_value.textContent       = `₹ ${element.agg_previous_value.toLocaleString('en-IN')}`
realised_intraday_p_l.textContent                  = `₹ ${element.agg_total_p_l.toLocaleString('en-IN')}`
realised_intraday_perc_p_l.textContent             = `${element.perc_agg_total_p_l} %`
}
}
})

if(found_agg_data == 0){
mf_invested_amount.textContent                     = "₹ 0"
mf_current_value.textContent                       = "₹ 0"
mf_previous_value.textContent                      = "₹ 0"
mf_p_l.textContent                                 = "₹ 0"
mf_perc_p_l.textContent                            = "0 %"
mf_day_p_l.textContent                             = "₹ 0"
mf_perc_day_p_l.textContent                        = "0 %"

unrealised_swing_invested_amount.textContent       = "₹ 0"
unrealised_swing_current_value.textContent         = "₹ 0"
unrealised_swing_previous_value.textContent        = "₹ 0"
unrealised_swing_p_l.textContent                   = "₹ 0"
unrealised_swing_perc_p_l.textContent              = "0 %"
unrealised_swing_day_p_l.textContent               = "₹ 0"
unrealised_swing_perc_day_p_l.textContent          = "0 %"

realised_swing_invested_amount.textContent         = "₹ 0"
realised_swing_current_value.textContent           = "₹ 0"
realised_swing_previous_value.textContent          = "₹ 0"
realised_swing_p_l.textContent                     = "₹ 0"
realised_swing_perc_p_l.textContent                = "0 %"

realised_intraday_invested_amount.textContent      = "₹ 0"
realised_intraday_current_value.textContent        = "₹ 0"
realised_intraday_previous_value.textContent       = "₹ 0"
realised_intraday_p_l.textContent                  = "₹ 0"
realised_intraday_perc_p_l.textContent             = "0 %"
}

add_class_list_based_on_value()
}