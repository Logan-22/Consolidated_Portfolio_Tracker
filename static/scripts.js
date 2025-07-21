import { create_notification } from './create_notification.js'
//create_notification(message, type, duration)

// Global Variables
let latest_consolidated_returns_data;
let latest_agg_returns_data;
let cons_returns_data;
let agg_returns_data;
let cons_alloc_data;
let cons_alloc_portfolio_data;
let agg_alloc_data;
let latest_cons_alloc_data;
let latest_cons_alloc_portfolio_data;
let latest_agg_alloc_data;
let processing_date_value;
let create_folder_status;
let create_table_status;
let create_view_status;
let dup_check_status;
let mf_hist_status;
let realised_intraday_hist_status;
let realised_swing_hist_status;
let unrealised_swing_hist_status;
let process_consolidated_hist_status;
let get_consolidated_hist_return_status;
let process_consolidated_hist_allocation_status;
let get_consolidated_hist_allocation_status;

// Global Variables for Chart

let consolidated_returns_chart;
let consolidated_allocation_chart_invested_amount_by_portfolio_type;
let consolidated_allocation_chart_profit_or_loss_by_portfolio_type;
let consolidated_allocation_chart_invested_amount_by_portfolio_category;
let consolidated_allocation_chart_profit_or_loss_by_portfolio_category;
let consolidated_allocation_chart_invested_amount_by_portfolio_name;
let consolidated_allocation_chart_profit_or_loss_by_portfolio_name;

const cons_processing_date_array = []
const cons_perc_total_p_l_array  = []
const cons_perc_day_p_l_array    = []
const cons_current_value_array   = []
const cons_day_p_l_array         = []
const cons_invested_amount_array = []
const cons_previous_value_array  = []
const cons_total_p_l_array       = []

// Global variables for elememts

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

const realised_intraday_p_l             = document.getElementById("realised_intraday_p_l")

document.addEventListener('DOMContentLoaded', async () => {
  if (document.getElementById('container')) {
    await create_managed_folders_in_directory()
    await create_managed_tables_in_db()
    await create_portfolio_views_in_db()
    await upsert_price_table()
    await upsert_mf_hist_returns_table()
    await upsert_realised_intraday_stock_hist_returns_table()
    await upsert_realised_swing_stock_hist_returns_table()
    await upsert_unrealised_swing_stock_hist_returns_table()
    await upsert_consolidated_hist_returns()
    await upsert_consolidated_hist_allocation()
    await get_consolidated_hist_returns()
    await get_consolidated_hist_allocation()
    await create_consolidated_notification()
    }
  }
);

// Create Managed Folders in Directory

async function create_managed_folders_in_directory(){
const create_folder_response=  await fetch ('/api/create_managed_folders/', {
  method: 'GET'
})

const create_folder_data = await create_folder_response.json();

create_folder_status = create_folder_data.status
if(create_folder_status != "Success"){
create_notification(create_folder_data.message, create_folder_data.status)
}
}

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

async function upsert_consolidated_hist_allocation(){
const consolidated_allocation_max_next_proc_date_response = await fetch ('/api/consolidated_hist_allocation/max_next_proc_date/', {
  method: 'GET'
})

const consolidated_allocation_max_next_proc_date_data = await consolidated_allocation_max_next_proc_date_response.json();
const max_next_proc_date_in_consolidated_allocation = consolidated_allocation_max_next_proc_date_data.data.max_next_processing_date

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

const process_consolidated_allocation_response = await fetch (`/api/process_consolidated_hist_allocation?start_date=${max_next_proc_date_in_consolidated_allocation}&end_date=${end_date}`, {
  method: 'GET'
})

const process_consolidated_allocation_data = await process_consolidated_allocation_response.json();

process_consolidated_hist_allocation_status = process_consolidated_allocation_data.status
if(process_consolidated_hist_allocation_status != "Success"){
create_notification(process_consolidated_allocation_data.message, process_consolidated_allocation_data.status)
}
}

async function get_consolidated_hist_returns(){
const consolidated_returns_response = await fetch ('/api/consolidated_hist_returns/all/', {
  method: 'GET'
})

const consolidated_returns_data = await consolidated_returns_response.json();

get_consolidated_hist_return_status = consolidated_returns_data.status
if(get_consolidated_hist_return_status != "Success"){
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
realised_swing_p_l.textContent                     = `₹ ${element.agg_total_p_l.toLocaleString('en-IN')}`
realised_swing_perc_p_l.textContent                = `${element.perc_agg_total_p_l} %`
}
else if(element.portfolio_type == "Intraday Stocks"){
realised_intraday_p_l.textContent                  = `₹ ${element.agg_total_p_l.toLocaleString('en-IN')}`
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

// Prepare arrays for Chart

cons_returns_data.forEach(cons_hist_return => {
  cons_processing_date_array.push(cons_hist_return.processing_date)
  cons_perc_total_p_l_array.push(cons_hist_return.perc_fin_total_p_l)
  cons_perc_day_p_l_array.push(cons_hist_return.perc_fin_day_p_l)
  cons_current_value_array.push(cons_hist_return.fin_current_value)
  cons_day_p_l_array.push(cons_hist_return.fin_day_p_l)
  cons_invested_amount_array.push(cons_hist_return.fin_invested_amount)
  cons_previous_value_array.push(cons_hist_return.fin_previous_value)
  cons_total_p_l_array.push(cons_hist_return.fin_total_p_l)
});

await initialize_consolidated_returns_chart();

processing_date_input.addEventListener("change", e => change_processing_date(e))

}

async function get_consolidated_hist_allocation(){
const consolidated_allocation_response = await fetch ('/api/consolidated_hist_allocation/all/', {
  method: 'GET'
})

const consolidated_allocation_data = await consolidated_allocation_response.json();

get_consolidated_hist_allocation_status = consolidated_allocation_data.status
if(get_consolidated_hist_allocation_status != "Success"){
create_notification(consolidated_allocation_data.message, consolidated_allocation_data.status)
}

if(consolidated_allocation_data.data.consolidated_allocation_portfolio_data){
  cons_alloc_portfolio_data = consolidated_allocation_data.data.consolidated_allocation_portfolio_data
}

if(consolidated_allocation_data.data.consolidated_allocation_data){
  cons_alloc_data = consolidated_allocation_data.data.consolidated_allocation_data
}

if(consolidated_allocation_data.data.agg_consolidated_allocation_data){
  agg_alloc_data = consolidated_allocation_data.data.agg_consolidated_allocation_data
}

if(consolidated_allocation_data.data.latest_consolidated_allocation_portfolio_data){
  latest_cons_alloc_portfolio_data = consolidated_allocation_data.data.latest_consolidated_allocation_portfolio_data
}

if(consolidated_allocation_data.data.latest_consolidated_allocation_data){
  latest_cons_alloc_data = consolidated_allocation_data.data.latest_consolidated_allocation_data
}

if(consolidated_allocation_data.data.latest_agg_consolidated_allocation_data){
  latest_agg_alloc_data = consolidated_allocation_data.data.latest_agg_consolidated_allocation_data
}

await initialize_consolidated_allocation_chart();
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
}

async function initialize_consolidated_returns_chart(){
if(consolidated_returns_chart){
  consolidated_returns_chart.destroy();
}

const ctx = document.getElementById('consolidated_returns_chart').getContext('2d');

consolidated_returns_chart = new Chart(ctx, {type: 'line',
        data: 
        {
        labels: cons_processing_date_array,  // Dates on the X-axis
        datasets: [
          {
            label: '% Total Profit/Loss %',
            data: cons_perc_total_p_l_array, 
            borderColor: 'rgba(75, 192, 192, 1)',
            // backgroundColor: 'rgba(75, 192, 192, 0.2)',
            // fill: true,
            tension: 0.4
          },
          {
            label: '% Day Profit/Loss',
            data: cons_perc_day_p_l_array,
            borderColor: 'rgba(12, 176, 6, 0.82)',
            // backgroundColor: 'rgba(75, 192, 192, 0.2)',
            // fill: true,
            tension: 0.4,
          },
          {
            label: '₹ Invested Amount',
            data: cons_invested_amount_array,
            borderColor: 'rgba(255, 99, 132, 1)',
            // backgroundColor: 'rgba(75, 192, 192, 0.2)',
            // fill: true,
            tension: 0.4,
            hidden: true
          },
          {
            label: '₹ Perceived Value',
            data: cons_current_value_array,
            borderColor: 'rgba(153, 51, 255, 1)',
            // backgroundColor: 'rgba(75, 192, 192, 0.2)',
            // fill: true,
            tension: 0.4,
            hidden: true
          },
          {
            label: '₹ Previous Perceived Value',
            data: cons_previous_value_array,
            borderColor: 'rgba(255, 165, 0, 1)',
            // backgroundColor: 'rgba(75, 192, 192, 0.2)',
            // fill: true,
            tension: 0.4,
            hidden: true
          },
          {
            label: '₹ Total P/L',
            data: cons_total_p_l_array,
            borderColor: 'rgba(255, 205, 86, 1)',
            // backgroundColor: 'rgba(75, 192, 192, 0.2)',
            // fill: true,
            tension: 0.4,
            hidden: true
          },
          {
            label: '₹ Day P/L',
            data: cons_day_p_l_array,
            borderColor: 'rgba(255, 105, 180, 1)',
            // backgroundColor: 'rgba(75, 192, 192, 0.2)',
            // fill: true,
            tension: 0.4,
            hidden: true
          }
        ]
        },
        options: {
          responsive: true,
          plugins: {
            title: {
              display: true,
              text: 'Consolidated Portfolio Chart',
              font: {
                    size: 20,
                    weight: 'bold',
                    family: 'Cambria'
                    },
              color: get_accent_color_from_theme()
            },
            tooltip: {
              mode: 'index',
              intersect: false
            }
          },
          scales: {
            x: {
              type: 'category',
              title: {
                display: true,
                text: 'Date'
              },
            grid: {
              color: 'rgba(150, 150, 150, 0.2)', // Light grid color for X-axis
              lineWidth: 1 // Set line width if needed
              }
            },
            y: {
              title: {
                display: true,
                text: 'Metrics'
              },
              beginAtZero: false,
            grid: {
              color: 'rgba(150, 150, 150, 0.2)', // Light grid color for X-axis
              lineWidth: 1 // Set line width if needed
              }
            }
          }
        }
      });
}

async function initialize_consolidated_allocation_chart(){

// Invested Amount by Portfolio Type  
if(consolidated_allocation_chart_invested_amount_by_portfolio_type){
  consolidated_allocation_chart_invested_amount_by_portfolio_type.destroy();
}

const ctx_invested_amount_by_portfolio_type = document.getElementById('consolidated_allocation_chart_invested_amount_by_portfolio_type').getContext('2d');

const allocation_chart_invested_amount_by_portfolio_type_data = {
labels: latest_cons_alloc_portfolio_data.map(portfolio => portfolio.portfolio_type),
datasets: [{
label: 'Invested Amount',
data: latest_cons_alloc_portfolio_data.map(portfolio => portfolio.fin_invested_amount),
backgroundColor: [
  'rgba(75, 192, 192, 0.7)',
  'rgba(255, 205, 86, 0.7)',
  'rgba(255, 99, 132, 0.7)',
  'rgba(201, 203, 207, 0.7)'
],
borderColor: [
  'rgba(75, 192, 192, 1)',
  'rgba(255, 205, 86, 1)',
  'rgba(255, 99, 132, 1)',
  'rgba(201, 203, 207, 1)'
],
borderWidth: 1
}]
};

const allocation_chart_invested_amount_by_portfolio_type_options = {
  responsive: true,
  cutout: '70%',
  plugins: {
    legend: {
      display:false
    },
    title: {
      display: true,
      text: 'Invested Amount by Type',
      font: {
        size: 15,
        weight: 'bold',
        family: 'Arial'
      },
      color: get_accent_color_from_theme(),
      padding: {
        top: 10,
        bottom: 10
      }
    }
  }
};

const allocation_chart_invested_amount_by_portfolio_type_config = {
  type: 'doughnut',
  data: allocation_chart_invested_amount_by_portfolio_type_data,
  options: allocation_chart_invested_amount_by_portfolio_type_options
};

consolidated_allocation_chart_invested_amount_by_portfolio_type = new Chart(ctx_invested_amount_by_portfolio_type, allocation_chart_invested_amount_by_portfolio_type_config);

///////////////////////////////////////////////////////////////////////////////

// Profit or Loss Amount by Portfolio Type  
if(consolidated_allocation_chart_profit_or_loss_by_portfolio_type){
  consolidated_allocation_chart_profit_or_loss_by_portfolio_type.destroy();
}

const ctx_profit_or_loss_by_portfolio_type = document.getElementById('consolidated_allocation_chart_profit_or_loss_by_portfolio_type').getContext('2d');

const allocation_chart_profit_or_loss_by_portfolio_type_data = {
labels: latest_cons_alloc_portfolio_data.map(portfolio => portfolio.portfolio_type),
datasets: [{
label: 'Profit/Loss',
data: latest_cons_alloc_portfolio_data.map(portfolio => portfolio.p_l),
backgroundColor: [
  'rgba(75, 192, 192, 0.7)',
  'rgba(255, 205, 86, 0.7)',
  'rgba(255, 99, 132, 0.7)',
  'rgba(201, 203, 207, 0.7)'
],
borderColor: [
  'rgba(75, 192, 192, 1)',
  'rgba(255, 205, 86, 1)',
  'rgba(255, 99, 132, 1)',
  'rgba(201, 203, 207, 1)'
],
borderWidth: 1
}]
};

const allocation_chart_profit_or_loss_by_portfolio_type_options = {
  responsive: true,
  cutout: '70%',
  plugins: {
    legend: {
      display: false
    },
    title: {
      display: true,
      text: 'Profit/Loss by Type',
      font: {
        size: 15,
        weight: 'bold',
        family: 'Arial'
      },
      color: get_accent_color_from_theme(),
      padding: {
        top: 10,
        bottom: 10
      }
    }
  }
};

const allocation_chart_profit_or_loss_by_portfolio_type_config = {
  type: 'doughnut',
  data: allocation_chart_profit_or_loss_by_portfolio_type_data,
  options: allocation_chart_profit_or_loss_by_portfolio_type_options
};

consolidated_allocation_chart_profit_or_loss_by_portfolio_type = new Chart(ctx_profit_or_loss_by_portfolio_type, allocation_chart_profit_or_loss_by_portfolio_type_config);

///////////////////////////////////////////////////////////////////////////////
// Invested Amount by Portfolio Category  
if(consolidated_allocation_chart_invested_amount_by_portfolio_category){
  consolidated_allocation_chart_invested_amount_by_portfolio_category.destroy();
}

// To Aggregate Percentages of same category
let category_list = []
const category_allocation_object = {}
const allocation_invested_amt_by_portfolio_category = []

latest_cons_alloc_data.forEach(allocation => {
  category_list.push(allocation.portfolio_category)
})
category_list = [... new Set(category_list)] // Deduplicate
category_list.sort()

category_list.forEach(category => {
category_allocation_object[category] = 0
latest_cons_alloc_data.forEach(allocation => {
  if(category == allocation.portfolio_category){
    category_allocation_object[category] += allocation.amount_invested
  }
})
})

category_list.forEach(category => {
  allocation_invested_amt_by_portfolio_category.push(category_allocation_object[category])
})

const ctx_invested_amount_by_portfolio_category = document.getElementById('consolidated_allocation_chart_invested_amount_by_portfolio_category').getContext('2d');

const allocation_chart_invested_amount_by_portfolio_category_data = {
labels: category_list,
datasets: [{
label: 'Invested Amount',
data: allocation_invested_amt_by_portfolio_category,
backgroundColor : [
  'rgba(75, 192, 192, 0.7)',   // teal
  'rgba(255, 99, 132, 0.7)',   // pink/red
  'rgba(255, 205, 86, 0.7)',   // yellow
  'rgba(54, 162, 235, 0.7)',   // blue
  'rgba(153, 102, 255, 0.7)',  // purple
  'rgba(201, 203, 207, 0.7)',  // grey
  'rgba(255, 159, 64, 0.7)',   // orange
  'rgba(100, 181, 246, 0.7)',  // sky blue
  'rgba(139, 195, 74, 0.7)',   // lime green
  'rgba(255, 138, 128, 0.7)',  // light red
  'rgba(121, 134, 203, 0.7)',  // indigo
  'rgba(255, 241, 118, 0.7)',  // light yellow
  'rgba(129, 212, 250, 0.7)',  // light cyan
  'rgba(174, 213, 129, 0.7)',  // greenish
  'rgba(244, 143, 177, 0.7)'   // light pink
],
borderColor: [
  'rgba(75, 192, 192, 1)',
  'rgba(255, 205, 86, 1)',
  'rgba(255, 99, 132, 1)',
  'rgba(201, 203, 207, 1)'
],
borderWidth: 1
}]
};

const allocation_chart_invested_amount_by_portfolio_category_options = {
  responsive: true,
  cutout: '70%',
  plugins: {
    legend: {
      display: false
    },
    title: {
      display: true,
      text: 'Invested Amount by Category',
      font: {
        size: 15,
        weight: 'bold',
        family: 'Arial'
      },
      color: get_accent_color_from_theme(),
      padding: {
        top: 10,
        bottom: 10
      }
    }
  }
};

const allocation_chart_invested_amount_by_portfolio_category_config = {
  type: 'doughnut',
  data: allocation_chart_invested_amount_by_portfolio_category_data,
  options: allocation_chart_invested_amount_by_portfolio_category_options
};

consolidated_allocation_chart_invested_amount_by_portfolio_category = new Chart(ctx_invested_amount_by_portfolio_category, allocation_chart_invested_amount_by_portfolio_category_config);

///////////////////////////////////////////////////////////////////////////////
// Profit/Loss Amount by Portfolio Category  
if(consolidated_allocation_chart_profit_or_loss_by_portfolio_category){
  consolidated_allocation_chart_profit_or_loss_by_portfolio_category.destroy();
}

// To Aggregate Percentages of same category
const p_l_category_allocation_object = {}
const p_l_allocation_amt_by_portfolio_category = []

category_list.forEach(category => {
p_l_category_allocation_object[category] = 0
latest_cons_alloc_data.forEach(allocation => {
  if(category == allocation.portfolio_category){
    p_l_category_allocation_object[category] += allocation.p_l
  }
})
})

category_list.forEach(category => {
  p_l_allocation_amt_by_portfolio_category.push(p_l_category_allocation_object[category])
})

const ctx_profit_or_loss_by_portfolio_category = document.getElementById('consolidated_allocation_chart_profit_or_loss_by_portfolio_category').getContext('2d');

const allocation_chart_profit_or_loss_by_portfolio_category_data = {
labels: category_list,
datasets: [{
label: 'Profit/Loss',
data: p_l_allocation_amt_by_portfolio_category,
backgroundColor : [
  'rgba(75, 192, 192, 0.7)',   // teal
  'rgba(255, 99, 132, 0.7)',   // pink/red
  'rgba(255, 205, 86, 0.7)',   // yellow
  'rgba(54, 162, 235, 0.7)',   // blue
  'rgba(153, 102, 255, 0.7)',  // purple
  'rgba(201, 203, 207, 0.7)',  // grey
  'rgba(255, 159, 64, 0.7)',   // orange
  'rgba(100, 181, 246, 0.7)',  // sky blue
  'rgba(139, 195, 74, 0.7)',   // lime green
  'rgba(255, 138, 128, 0.7)',  // light red
  'rgba(121, 134, 203, 0.7)',  // indigo
  'rgba(255, 241, 118, 0.7)',  // light yellow
  'rgba(129, 212, 250, 0.7)',  // light cyan
  'rgba(174, 213, 129, 0.7)',  // greenish
  'rgba(244, 143, 177, 0.7)'   // light pink
],
borderColor: [
  'rgba(75, 192, 192, 1)',
  'rgba(255, 205, 86, 1)',
  'rgba(255, 99, 132, 1)',
  'rgba(201, 203, 207, 1)'
],
borderWidth: 1
}]
};

const allocation_chart_profit_or_loss_by_portfolio_category_options = {
  responsive: true,
  cutout: '70%',
  plugins: {
    legend: {
      display :false
    },
    title: {
      display: true,
      text: 'Profit/Loss by Category',
      font: {
        size: 15,
        weight: 'bold',
        family: 'Arial'
      },
      color: get_accent_color_from_theme(),
      padding: {
        top: 10,
        bottom: 10
      }
    }
  }
};

const allocation_chart_profit_or_loss_by_portfolio_category_config = {
  type: 'doughnut',
  data: allocation_chart_profit_or_loss_by_portfolio_category_data,
  options: allocation_chart_profit_or_loss_by_portfolio_category_options
};

consolidated_allocation_chart_profit_or_loss_by_portfolio_category = new Chart(ctx_profit_or_loss_by_portfolio_category, allocation_chart_profit_or_loss_by_portfolio_category_config);

///////////////////////////////////////////////////////////////////////////////

// Invested Amount by Portfolio Name
if(consolidated_allocation_chart_invested_amount_by_portfolio_name){
  consolidated_allocation_chart_invested_amount_by_portfolio_name.destroy();
}

const ctx_invested_amount_by_portfolio_name = document.getElementById('consolidated_allocation_chart_invested_amount_by_portfolio_name').getContext('2d');

const allocation_chart_invested_amount_by_portfolio_name_data = {
labels: latest_agg_alloc_data.map(portfolio => portfolio.portfolio_name),
datasets: [{
label: 'Invested Amount',
data: latest_agg_alloc_data.map(portfolio => portfolio.amount_invested),
backgroundColor: [
  'rgba(75, 192, 192, 0.7)',    // Teal
  'rgba(255, 205, 86, 0.7)',    // Yellow
  'rgba(255, 99, 132, 0.7)',    // Pink/Red
  'rgba(201, 203, 207, 0.7)',   // Gray
  'rgba(54, 162, 235, 0.7)',    // Blue
  'rgba(153, 102, 255, 0.7)',   // Purple
  'rgba(255, 159, 64, 0.7)',    // Orange
  'rgba(100, 181, 246, 0.7)',   // Light Blue
  'rgba(139, 195, 74, 0.7)',    // Light Green
  'rgba(255, 138, 128, 0.7)',   // Soft Red
  'rgba(121, 134, 203, 0.7)',   // Indigo
  'rgba(255, 241, 118, 0.7)',   // Light Yellow
  'rgba(129, 212, 250, 0.7)',   // Cyan
  'rgba(174, 213, 129, 0.7)',   // Pastel Green
  'rgba(244, 143, 177, 0.7)'    // Light Pink
],
borderColor: [
  'rgba(75, 192, 192, 1)',    // Teal
  'rgba(255, 205, 86, 1)',    // Yellow
  'rgba(255, 99, 132, 1)',    // Pink/Red
  'rgba(201, 203, 207, 1)',   // Gray
  'rgba(54, 162, 235, 1)',    // Blue
  'rgba(153, 102, 255, 1)',   // Purple
  'rgba(255, 159, 64, 1)',    // Orange
  'rgba(100, 181, 246, 1)',   // Light Blue
  'rgba(139, 195, 74, 1)',    // Light Green
  'rgba(255, 138, 128, 1)',   // Soft Red
  'rgba(121, 134, 203, 1)',   // Indigo
  'rgba(255, 241, 118, 1)',   // Light Yellow
  'rgba(129, 212, 250, 1)',   // Cyan
  'rgba(174, 213, 129, 1)',   // Pastel Green
  'rgba(244, 143, 177, 1)'    // Light Pink
],
borderWidth: 1
}]
};

const allocation_chart_invested_amount_by_portfolio_name_options = {
  responsive: true,
  cutout: '70%',
  plugins: {
    legend: {
      display: false
    },
    title: {
      display: true,
      text: 'Invested Amount by Name',
      font: {
        size: 15,
        weight: 'bold',
        family: 'Arial'
      },
      color: get_accent_color_from_theme(),
      padding: {
        top: 10,
        bottom: 10
      }
    }
  }
};

const allocation_chart_invested_amount_by_portfolio_name_config = {
  type: 'doughnut',
  data: allocation_chart_invested_amount_by_portfolio_name_data,
  options: allocation_chart_invested_amount_by_portfolio_name_options
};

consolidated_allocation_chart_invested_amount_by_portfolio_name = new Chart(ctx_invested_amount_by_portfolio_name, allocation_chart_invested_amount_by_portfolio_name_config);

///////////////////////////////////////////////////////////////////////////////

// Invested Amount by Portfolio Name
if(consolidated_allocation_chart_profit_or_loss_by_portfolio_name){
  consolidated_allocation_chart_profit_or_loss_by_portfolio_name.destroy();
}

const ctx_profit_or_loss_by_portfolio_name = document.getElementById('consolidated_allocation_chart_profit_or_loss_by_portfolio_name').getContext('2d');

const allocation_chart_profit_or_loss_by_portfolio_name_data = {
labels: latest_agg_alloc_data.map(portfolio => portfolio.portfolio_name),
datasets: [{
label: 'Profit/Loss',
data: latest_agg_alloc_data.map(portfolio => portfolio.p_l),
backgroundColor: [
  'rgba(75, 192, 192, 0.7)',    // Teal
  'rgba(255, 205, 86, 0.7)',    // Yellow
  'rgba(255, 99, 132, 0.7)',    // Pink/Red
  'rgba(201, 203, 207, 0.7)',   // Gray
  'rgba(54, 162, 235, 0.7)',    // Blue
  'rgba(153, 102, 255, 0.7)',   // Purple
  'rgba(255, 159, 64, 0.7)',    // Orange
  'rgba(100, 181, 246, 0.7)',   // Light Blue
  'rgba(139, 195, 74, 0.7)',    // Light Green
  'rgba(255, 138, 128, 0.7)',   // Soft Red
  'rgba(121, 134, 203, 0.7)',   // Indigo
  'rgba(255, 241, 118, 0.7)',   // Light Yellow
  'rgba(129, 212, 250, 0.7)',   // Cyan
  'rgba(174, 213, 129, 0.7)',   // Pastel Green
  'rgba(244, 143, 177, 0.7)'    // Light Pink
],
borderColor: [
  'rgba(75, 192, 192, 1)',    // Teal
  'rgba(255, 205, 86, 1)',    // Yellow
  'rgba(255, 99, 132, 1)',    // Pink/Red
  'rgba(201, 203, 207, 1)',   // Gray
  'rgba(54, 162, 235, 1)',    // Blue
  'rgba(153, 102, 255, 1)',   // Purple
  'rgba(255, 159, 64, 1)',    // Orange
  'rgba(100, 181, 246, 1)',   // Light Blue
  'rgba(139, 195, 74, 1)',    // Light Green
  'rgba(255, 138, 128, 1)',   // Soft Red
  'rgba(121, 134, 203, 1)',   // Indigo
  'rgba(255, 241, 118, 1)',   // Light Yellow
  'rgba(129, 212, 250, 1)',   // Cyan
  'rgba(174, 213, 129, 1)',   // Pastel Green
  'rgba(244, 143, 177, 1)'    // Light Pink
],
borderWidth: 1
}]
};

const allocation_chart_profit_or_loss_by_portfolio_name_options = {
  responsive: true,
  cutout: '70%',
  plugins: {
    legend: {
      display: false
    },
    title: {
      display: true,
      text: 'Profit/Loss by Name',
      font: {
        size: 15,
        weight: 'bold',
        family: 'Arial'
      },
      color: get_accent_color_from_theme(),
      padding: {
        top: 10,
        bottom: 10
      }
    }
  }
};

const allocation_chart_profit_or_loss_by_portfolio_name_config = {
  type: 'doughnut',
  data: allocation_chart_profit_or_loss_by_portfolio_name_data,
  options: allocation_chart_profit_or_loss_by_portfolio_name_options
};

consolidated_allocation_chart_profit_or_loss_by_portfolio_name = new Chart(ctx_profit_or_loss_by_portfolio_name, allocation_chart_profit_or_loss_by_portfolio_name_config);

}

async function create_consolidated_notification(){
if (create_table_status                         == "Success" &&
    create_view_status                          == "Success" &&
    dup_check_status                            == "Success" &&
    mf_hist_status                              == "Success" &&
    realised_intraday_hist_status               == "Success" &&
    realised_swing_hist_status                  == "Success" &&
    unrealised_swing_hist_status                == "Success" &&
    process_consolidated_hist_status            == "Success" &&
    get_consolidated_hist_return_status         == "Success" &&
    process_consolidated_hist_allocation_status == "Success" &&
    get_consolidated_hist_allocation_status     == "Success"){
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

// Reset and Update the Doughnut charts

consolidated_allocation_chart_invested_amount_by_portfolio_type.data.datasets[0].data = []
consolidated_allocation_chart_invested_amount_by_portfolio_type.data.labels = []
consolidated_allocation_chart_profit_or_loss_by_portfolio_type.data.datasets[0].data = []
consolidated_allocation_chart_profit_or_loss_by_portfolio_type.data.labels = []

cons_alloc_portfolio_data.forEach(allocation => {
if(allocation.processing_date == processing_date_value || (processing_date_value > allocation.processing_date && processing_date_value < allocation.next_processing_date))
{
consolidated_allocation_chart_invested_amount_by_portfolio_type.data.datasets[0].data.push(allocation.amount_invested)
consolidated_allocation_chart_invested_amount_by_portfolio_type.data.labels.push(allocation.portfolio_type)
consolidated_allocation_chart_invested_amount_by_portfolio_type.update()
consolidated_allocation_chart_profit_or_loss_by_portfolio_type.data.datasets[0].data.push(allocation.p_l)
consolidated_allocation_chart_profit_or_loss_by_portfolio_type.data.labels.push(allocation.portfolio_type)
consolidated_allocation_chart_profit_or_loss_by_portfolio_type.update()
}
})

///////////////////////////////////////////////////////////////////////////////

consolidated_allocation_chart_invested_amount_by_portfolio_category.data.datasets[0].data = []
consolidated_allocation_chart_invested_amount_by_portfolio_category.data.labels = []
consolidated_allocation_chart_profit_or_loss_by_portfolio_category.data.datasets[0].data = []
consolidated_allocation_chart_profit_or_loss_by_portfolio_category.data.labels = []
let category_list = []
const category_invested_amount_object = {}
const category_profit_or_loss_object = {}

// Get Category List
cons_alloc_data.forEach(allocation => {
if(allocation.processing_date == processing_date_value || (processing_date_value > allocation.processing_date && processing_date_value < allocation.next_processing_date))
{
category_list.push(allocation.portfolio_category)
}
})

category_list = [... new Set(category_list)] // Deduplicate
category_list.sort()

category_list.forEach(category => {
category_invested_amount_object[category] = 0
category_profit_or_loss_object[category] = 0
cons_alloc_data.forEach(allocation => {
  if(category == allocation.portfolio_category && (allocation.processing_date == processing_date_value || (processing_date_value > allocation.processing_date && processing_date_value < allocation.next_processing_date))){
    category_invested_amount_object[category] += allocation.amount_invested
    category_profit_or_loss_object[category] += allocation.p_l
  }
})
})

category_list.forEach(category => {
consolidated_allocation_chart_invested_amount_by_portfolio_category.data.datasets[0].data.push(category_invested_amount_object[category])
consolidated_allocation_chart_invested_amount_by_portfolio_category.data.labels.push(category)
consolidated_allocation_chart_invested_amount_by_portfolio_category.update()
consolidated_allocation_chart_profit_or_loss_by_portfolio_category.data.datasets[0].data.push(category_profit_or_loss_object[category])
consolidated_allocation_chart_profit_or_loss_by_portfolio_category.data.labels.push(category)
consolidated_allocation_chart_profit_or_loss_by_portfolio_category.update()
})

///////////////////////////////////////////////////////////////////////////////

consolidated_allocation_chart_invested_amount_by_portfolio_name.data.datasets[0].data = []
consolidated_allocation_chart_invested_amount_by_portfolio_name.data.labels = []
consolidated_allocation_chart_profit_or_loss_by_portfolio_name.data.datasets[0].data = []
consolidated_allocation_chart_profit_or_loss_by_portfolio_name.data.labels = []

agg_alloc_data.forEach(allocation => {
if(allocation.processing_date == processing_date_value || (processing_date_value > allocation.processing_date && processing_date_value < allocation.next_processing_date))
{
consolidated_allocation_chart_invested_amount_by_portfolio_name.data.datasets[0].data.push(allocation.amount_invested)
consolidated_allocation_chart_invested_amount_by_portfolio_name.data.labels.push(allocation.portfolio_name)
consolidated_allocation_chart_invested_amount_by_portfolio_name.update()
consolidated_allocation_chart_profit_or_loss_by_portfolio_name.data.datasets[0].data.push(allocation.p_l)
consolidated_allocation_chart_profit_or_loss_by_portfolio_name.data.labels.push(allocation.portfolio_name)
consolidated_allocation_chart_profit_or_loss_by_portfolio_name.update()
}
})

}

function get_accent_color_from_theme(){
  let accent_color = document.body.classList.contains("Zerodha_Theme") ? '#f6461a': document.body.classList.contains("Dark_Theme") ? '#bb86fc' : '#3f51b5'
  return accent_color
}

export async function change_title_color_in_chart(){
consolidated_returns_chart.options.plugins.title.color = get_accent_color_from_theme()
consolidated_allocation_chart_invested_amount_by_portfolio_type.options.plugins.title.color = get_accent_color_from_theme()
consolidated_allocation_chart_profit_or_loss_by_portfolio_type.options.plugins.title.color = get_accent_color_from_theme()
consolidated_allocation_chart_invested_amount_by_portfolio_category.options.plugins.title.color = get_accent_color_from_theme()
consolidated_allocation_chart_profit_or_loss_by_portfolio_category.options.plugins.title.color = get_accent_color_from_theme()
consolidated_allocation_chart_invested_amount_by_portfolio_name.options.plugins.title.color = get_accent_color_from_theme()
consolidated_allocation_chart_profit_or_loss_by_portfolio_name.options.plugins.title.color = get_accent_color_from_theme()

consolidated_returns_chart.update()
consolidated_allocation_chart_invested_amount_by_portfolio_type.update()
consolidated_allocation_chart_profit_or_loss_by_portfolio_type.update()
consolidated_allocation_chart_invested_amount_by_portfolio_category.update()
consolidated_allocation_chart_profit_or_loss_by_portfolio_category.update()
consolidated_allocation_chart_invested_amount_by_portfolio_name.update()
consolidated_allocation_chart_profit_or_loss_by_portfolio_name.update()
}