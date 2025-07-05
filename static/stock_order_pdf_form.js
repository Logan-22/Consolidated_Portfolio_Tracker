import { create_notification } from './create_notification.js'

// Stock Order Entry into Stock Order Table

// Method : POST
// URL    : /api/stock_order_pdf/

const update_button = document.getElementById("update_button")
update_button.classList = "hidden"
const add_fee_button = document.getElementById("add_fee_button")
add_fee_button.classList = "hidden"
const trade_types = [] // To capture the trade types with in a pdf/day

let api_response = {}

let global_data = "" // To access fees data in the Add Fee Component Event

document.getElementById('stock_order_pdf_form').addEventListener('submit', async function (e) {
e.preventDefault();

while(trade_types.length > 0){
trade_types.pop() // Remove existing entries from previous pdf
}

const trade_table = document.getElementById('trade_table')
const fee_table   = document.getElementById('fee_table')

trade_table.innerHTML   = ""
fee_table.innerHTML     = ""
update_button.classList = "hidden"

const stock_pdf      = document.getElementById('stock_pdf')
const stock_pdf_file = stock_pdf.files[0]
const file_password  = document.getElementById('file_password').value;

const formData = new FormData()

formData.append('stock_pdf_file',stock_pdf_file)
formData.append('file_password',file_password)

const pdf_post_response = await fetch(`/api/stock_pdf/`, {
method: 'POST',
body: formData
})

const pdf_post_data = await pdf_post_response.json();
api_response = pdf_post_data
global_data = pdf_post_data

if(pdf_post_data.status === "Success"){
    create_notification(pdf_post_data.message, pdf_post_data.status)
    trade_table.innerHTML = `
    <tr>
    <th class="color-accent">Stock Name</th>
    <th class="color-accent">ISIN</th>
    <th class="color-accent">Order Number</th>
    <th class="color-accent">Order Time</th>
    <th class="color-accent">Trade Number</th>
    <th class="color-accent">Trade Time</th>
    <th class="color-accent">Buy Or Sell</th>
    <th class="color-accent">Stock Quantity</th>
    <th class="color-accent">Brokerage Per Trade</th>
    <th class="color-accent">Net Trade Price</th>
    <th class="color-accent">Net Total</th>
    <th class="color-accent">Trade Set</th>
    <th class="color-accent">Trade Position</th>
    <th class="color-accent">Trade Entry Date</th>
    <th class="color-accent">Trade Entry Time</th>
    <th class="color-accent">Trade Exit Date</th>
    <th class="color-accent">Trade Exit Time</th>
    <th class="color-accent">Trade Type</th>
    <th class="color-accent">Leverage</th>
    </tr>`

    pdf_post_data.data.forEach(element => {
    if(element.trade_exit_date == 'null'){
      element.trade_exit_date = ""
      element.trade_exit_time = ""
    }

    if (! trade_types.includes(element.trade_type)){
      trade_types.push(element.trade_type)
    }
    
    trade_table.innerHTML += `
    <tr id = ${String(element.order_number)}^${String(element.trade_number)}>
      <td contenteditable='true'>${element.stock_symbol}</td>
      <td contenteditable='true'>${element.stock_isin}</td>
      <td contenteditable='true'>${element.order_number}</td>
      <td contenteditable='true'>${element.order_time}</td>
      <td contenteditable='true'>${element.trade_number}</td>
      <td contenteditable='true'>${element.trade_time}</td>
      <td contenteditable='true'>${element.buy_or_sell}</td>
      <td contenteditable='true'>${element.stock_quantity}</td>
      <td contenteditable='true'>${element.brokerage_per_trade}</td>
      <td contenteditable='true'>${element.net_trade_price_per_unit}</td>
      <td contenteditable='true'>${element.net_total_before_levies}</td>
      <td contenteditable='true'>${element.trade_set}</td>
      <td contenteditable='true'>${element.trade_position}</td>
      <td contenteditable='true'>${element.trade_entry_date}</td>
      <td contenteditable='true'>${element.trade_entry_time}</td>
      <td contenteditable='true'>${element.trade_exit_date}</td>
      <td contenteditable='true'>${element.trade_exit_time}</td>
      <td contenteditable='true'>${element.trade_type}</td>
      <td contenteditable='true'>${element.leverage}</td>
    </tr>`

}
)

let intraday_trading_present_class = ""
let swing_trading_present_class    = ""

if (trade_types.includes("Intraday Trading") && trade_types.includes("Swing Trading")){
  intraday_trading_present_class = ""
  swing_trading_present_class = ""
} else 
if (trade_types.includes("Intraday Trading") && ! trade_types.includes("Swing Trading")){
  intraday_trading_present_class = ""
  swing_trading_present_class = "hidden"
} else 
if (! trade_types.includes("Intraday Trading") && trade_types.includes("Swing Trading")){
  intraday_trading_present_class = "hidden"
  swing_trading_present_class = ""
} 

fee_table.innerHTML = `
    <tr id = "fee_header">
    <th class="color-accent">Fee Type</th>
        <td class = '${intraday_trading_present_class}'>Intraday Trading Fees</td>
        <td class = '${swing_trading_present_class}'>Swing Trading Fees</td>
    </tr>
    <tr id = "net_obligation">
    <th class="color-accent">Net Obligation</th>
        <td class = '${intraday_trading_present_class}' contenteditable='true'>${pdf_post_data.fees.net_obligation}</td>
        <td class = '${swing_trading_present_class}' contenteditable='true'>${pdf_post_data.fees.net_obligation}</td>
    </tr>
    <tr id = "brokerage">
    <th class="color-accent">Brokerage</th>
        <td class = '${intraday_trading_present_class}' contenteditable='true'>${pdf_post_data.fees.brokerage}</td>
        <td class = '${swing_trading_present_class}' contenteditable='true'>${pdf_post_data.fees.brokerage}</td>
    </tr>
    <tr id = "exc_trans_charges">
    <th class="color-accent">Exchange Transaction Charges</th>
        <td class = '${intraday_trading_present_class}' contenteditable='true'>${pdf_post_data.fees.exc_trans_charges}</td>
        <td class = '${swing_trading_present_class}' contenteditable='true'>${pdf_post_data.fees.exc_trans_charges}</td>
    </tr>
    <tr id = "igst">
    <th class="color-accent">IGST</th>
        <td class = '${intraday_trading_present_class}' contenteditable='true'>${pdf_post_data.fees.igst}</td>
        <td class = '${swing_trading_present_class}' contenteditable='true'>${pdf_post_data.fees.igst}</td>
    </tr>
    <tr id = "sec_trans_tax">
    <th class="color-accent">Securities Transaction Tax</th>
        <td class = '${intraday_trading_present_class}' contenteditable='true'>${pdf_post_data.fees.sec_trans_tax}</td>
        <td class = '${swing_trading_present_class}' contenteditable='true'>${pdf_post_data.fees.sec_trans_tax}</td>
    </tr>
    <tr id = "sebi_turn_fees">
    <th class="color-accent">SEBI Turnover Fees</th>
        <td class = '${intraday_trading_present_class}' contenteditable='true'>${pdf_post_data.fees.sebi_turn_fees}</td>
        <td class = '${swing_trading_present_class}' contenteditable='true'>${pdf_post_data.fees.sebi_turn_fees}</td>
    </tr>
    <tr id = "auto_square_off_charges_row">
    <th class="color-accent">Auto Square Off Charges(If Any)</th>
        <td id = "intraday_auto_square_off_charges" class = '${intraday_trading_present_class}' contenteditable='true'></td>
        <td id = "swing_auto_square_off_charges" class = '${swing_trading_present_class}' contenteditable='true'></td>
    </tr>
    <tr id = "depository_charges_row">
    <th class="color-accent">Depository Charges(If Any)</th>
        <td id = "intraday_depository_charges" class = '${intraday_trading_present_class}' contenteditable='true'></td>
        <td id = "swing_depository_charges" class = '${swing_trading_present_class}' contenteditable='true'></td>
    </tr>`

    update_button.classList = ""
    add_fee_button.classList = ""

}
else{
create_notification(pdf_post_data.message, pdf_post_data.status)
}
})


// Update Trade Info

document.getElementById("update_button").addEventListener('click', async function(e) {
e.preventDefault();

// Trades Detect Update

const trade_table = document.getElementById("trade_table")

const trade_table_child = trade_table.children

const num_of_trades = trade_table_child.length

for (let i = 1; i < num_of_trades; i++){ // i = 1 to ignore row headers
  const trade_table_child_tbody = trade_table_child[i] // tbody is present within table for each row
  const trade_table_child_tbody_child = trade_table_child_tbody.children
  const trade_table_row_id = trade_table_child_tbody_child[0].id
  
  const trade_table_row = document.getElementById(trade_table_row_id)
  const trade_table_row_child = trade_table_row.children

  const stock_symbol             = trade_table_row_child[0].textContent
  const stock_isin               = trade_table_row_child[1].textContent
  const order_number             = trade_table_row_child[2].textContent
  const order_time               = trade_table_row_child[3].textContent
  const trade_number             = trade_table_row_child[4].textContent
  const trade_time               = trade_table_row_child[5].textContent
  const buy_or_sell              = trade_table_row_child[6].textContent
  const stock_quantity           = trade_table_row_child[7].textContent
  const brokerage_per_trade      = trade_table_row_child[8].textContent
  const net_trade_price_per_unit = trade_table_row_child[9].textContent
  const net_total_before_levies  = trade_table_row_child[10].textContent
  const trade_set                = trade_table_row_child[11].textContent
  const trade_position           = trade_table_row_child[12].textContent
  const trade_entry_date         = trade_table_row_child[13].textContent
  const trade_entry_time         = trade_table_row_child[14].textContent
  const trade_exit_date          = trade_table_row_child[15].textContent
  const trade_exit_time          = trade_table_row_child[16].textContent
  const trade_type               = trade_table_row_child[17].textContent
  const leverage                 = trade_table_row_child[18].textContent

  api_response.data.forEach(item => {
    if(item.order_number === trade_table_row_id.split("^")[0] && item.trade_number === trade_table_row_id.split("^")[1] ){ // Find the matching order number in api response
      if(item.stock_symbol != stock_symbol){
          trade_table_row_child[0].classList += "updated"
          item.stock_symbol = stock_symbol // Update the api value with html value
        }
      if(item.stock_isin != stock_isin){
          trade_table_row_child[1].classList += "updated"
          item.stock_isin = stock_isin
        }
      if(item.order_number != order_number){
          trade_table_row_child[2].classList += "updated"
          item.order_number = order_number
        }
      if(item.order_time != order_time){
          trade_table_row_child[3].classList += "updated"
          item.order_time = order_time
        }
      if(item.trade_number != trade_number){
          trade_table_row_child[4].classList += "updated"
          item.trade_number = trade_number
        }
      if(item.trade_time != trade_time){
          trade_table_row_child[5].classList += "updated"
          item.trade_number = trade_number
        }
      if(item.buy_or_sell != buy_or_sell){
          trade_table_row_child[6].classList += "updated"
          item.buy_or_sell = buy_or_sell
        }
      if(item.stock_quantity != stock_quantity){
          trade_table_row_child[7].classList += "updated"
          item.stock_quantity = stock_quantity
        }
      if(item.brokerage_per_trade != brokerage_per_trade){
          trade_table_row_child[8].classList += "updated"
          item.brokerage_per_trade = brokerage_per_trade
        }
      if(item.net_trade_price_per_unit != net_trade_price_per_unit){
          trade_table_row_child[9].classList += "updated"
          item.net_trade_price_per_unit = net_trade_price_per_unit
        }
      if(item.net_total_before_levies != net_total_before_levies){
          trade_table_row_child[10].classList += "updated"
          item.net_total_before_levies = net_total_before_levies
        }
      if(item.trade_set != trade_set){
          trade_table_row_child[11].classList += "updated"
          item.trade_set = trade_set
        }
      if(item.trade_position != trade_position){
          trade_table_row_child[12].classList += "updated"
          item.trade_position = trade_position
        }
      if(item.trade_entry_date != trade_entry_date){
          trade_table_row_child[13].classList += "updated"
          item.trade_entry_date = trade_entry_date
        }
      if(item.trade_entry_time != trade_entry_time){
          trade_table_row_child[14].classList += "updated"
          item.trade_entry_time = trade_entry_time
        }
      
      if(trade_exit_date == 'null'){
        item.trade_exit_date = null
      }
      else if(item.trade_exit_date != trade_exit_date){
          trade_table_row_child[15].classList += "updated"
          item.trade_exit_date = trade_exit_date
        }
      
      if(trade_exit_time == 'null'){
        item.trade_exit_time = null
      }
      else if(item.trade_exit_time != trade_exit_time){
          trade_table_row_child[16].classList += "updated"
          item.trade_exit_time = trade_exit_time
        }
      
      if(item.trade_type != trade_type){
          trade_table_row_child[17].classList += "updated"
          item.trade_type = trade_type
        }
      if(item.leverage != leverage){
          trade_table_row_child[18].classList += "updated"
          item.leverage = leverage
        }
    }
  })
}

// Fees Detect Update

const fee_table                            = document.getElementById("fee_table")
const fee_table_child                      = fee_table.children
const fee_table_child_tbody                = fee_table_child[0]
const fee_table_child_tbody_child          = fee_table_child_tbody.children

const net_obligation_tr                    = fee_table_child_tbody_child[1]
const intraday_net_obligation_td           = net_obligation_tr.children[1] // Get the first column after th
const swing_net_obligation_td              = net_obligation_tr.children[2] // Get the second column after th
const intraday_net_obligation              = intraday_net_obligation_td.textContent
const swing_net_obligation                 = swing_net_obligation_td.textContent

const brokerage_tr                         = fee_table_child_tbody_child[2]
const intraday_brokerage_td                = brokerage_tr.children[1]
const swing_brokerage_td                   = brokerage_tr.children[2]
const intraday_brokerage                   = intraday_brokerage_td.textContent
const swing_brokerage                      = swing_brokerage_td.textContent

const exc_trans_charges_tr                 = fee_table_child_tbody_child[3]
const intraday_exc_trans_charges_td        = exc_trans_charges_tr.children[1]
const swing_exc_trans_charges_td           = exc_trans_charges_tr.children[2]
const intraday_exc_trans_charges           = intraday_exc_trans_charges_td.textContent
const swing_exc_trans_charges              = swing_exc_trans_charges_td.textContent

const igst_tr                              = fee_table_child_tbody_child[4]
const intraday_igst_td                     = igst_tr.children[1]
const swing_igst_td                        = igst_tr.children[2]
const intraday_igst                        = intraday_igst_td.textContent
const swing_igst                           = swing_igst_td.textContent

const sec_trans_tax_tr                     = fee_table_child_tbody_child[5]
const intraday_sec_trans_tax_td            = sec_trans_tax_tr.children[1]
const swing_sec_trans_tax_td               = sec_trans_tax_tr.children[2]
const intraday_sec_trans_tax               = intraday_sec_trans_tax_td.textContent
const swing_sec_trans_tax                  = swing_sec_trans_tax_td.textContent

const sebi_turn_fees_tr                    = fee_table_child_tbody_child[6]
const intraday_sebi_turn_fees_td           = sebi_turn_fees_tr.children[1]
const swing_sebi_turn_fees_td              = sebi_turn_fees_tr.children[2]
const intraday_sebi_turn_fees              = intraday_sebi_turn_fees_td.textContent
const swing_sebi_turn_fees                 = swing_sebi_turn_fees_td.textContent

const intraday_auto_square_off_charges_td  = document.getElementById("intraday_auto_square_off_charges")
const intraday_auto_square_off_charges     = intraday_auto_square_off_charges_td.textContent
const swing_auto_square_off_charges_td     = document.getElementById("swing_auto_square_off_charges")
const swing_auto_square_off_charges        = swing_auto_square_off_charges_td.textContent

const intraday_depository_charges_td       = document.getElementById("intraday_depository_charges")
const intraday_depository_charges          = intraday_depository_charges_td.textContent
const swing_depository_charges_td          = document.getElementById("swing_depository_charges")
const swing_depository_charges             = swing_depository_charges_td.textContent

if(api_response.fees.net_obligation     != intraday_net_obligation){
intraday_net_obligation_td.classList    += "updated"
}

if(api_response.fees.net_obligation     != swing_net_obligation){
swing_net_obligation_td.classList       += "updated"
}

if(api_response.fees.brokerage          != intraday_brokerage){
intraday_brokerage_td.classList         += "updated"
}

if(api_response.fees.brokerage          != swing_brokerage){
swing_brokerage_td.classList            += "updated"
}

if(api_response.fees.exc_trans_charges  != intraday_exc_trans_charges){
intraday_exc_trans_charges_td.classList += "updated"
}

if(api_response.fees.exc_trans_charges  != swing_exc_trans_charges){
swing_exc_trans_charges_td.classList    += "updated"
}

if(api_response.fees.igst               != intraday_igst){
intraday_igst_td.classList              += "updated"
}

if(api_response.fees.igst               != swing_igst){
swing_igst_td.classList                 += "updated"
}

if(api_response.fees.sec_trans_tax      != intraday_sec_trans_tax){
intraday_sec_trans_tax_td.classList     += "updated"
}

if(api_response.fees.sec_trans_tax      != swing_sec_trans_tax){
swing_sec_trans_tax_td.classList        += "updated"
}

if(api_response.fees.sebi_turn_fees     != intraday_sebi_turn_fees){
intraday_sebi_turn_fees_td.classList    += "updated"
}

if(api_response.fees.sebi_turn_fees     != swing_sebi_turn_fees){
swing_sebi_turn_fees_td.classList       += "updated"
}

if(intraday_auto_square_off_charges && intraday_auto_square_off_charges != 0 && intraday_auto_square_off_charges != null){
intraday_auto_square_off_charges_td.classList     += "updated"
api_response.fees.intraday_auto_square_off_charges = intraday_auto_square_off_charges
}
else{
api_response.fees.intraday_auto_square_off_charges = 0
}

if(swing_auto_square_off_charges && swing_auto_square_off_charges != 0 && swing_auto_square_off_charges != null){
swing_auto_square_off_charges_td.classList        += "updated"
api_response.fees.swing_auto_square_off_charges    = swing_auto_square_off_charges
}
else{
api_response.fees.swing_auto_square_off_charges    = 0

}

if(intraday_depository_charges && intraday_depository_charges != 0 && intraday_depository_charges != null){
intraday_depository_charges_td.classList          += "updated"
api_response.fees.intraday_depository_charges      = intraday_depository_charges
}
else{
api_response.fees.intraday_depository_charges      = 0
}

if(swing_depository_charges && swing_depository_charges != 0 && swing_depository_charges != null){
swing_depository_charges_td.classList             += "updated"
api_response.fees.swing_depository_charges         = swing_depository_charges
}
else{
api_response.fees.swing_depository_charges         = 0
}

api_response.fees.trade_types                      = trade_types

api_response.fees.intraday_net_obligation          = intraday_net_obligation // Update the api value with html value
api_response.fees.swing_net_obligation             = swing_net_obligation

api_response.fees.intraday_brokerage               = intraday_brokerage
api_response.fees.swing_brokerage                  = swing_brokerage

api_response.fees.intraday_exc_trans_charges       = intraday_exc_trans_charges
api_response.fees.swing_exc_trans_charges          = swing_exc_trans_charges

api_response.fees.intraday_igst                    = intraday_igst
api_response.fees.swing_igst                       = swing_igst

api_response.fees.intraday_sec_trans_tax           = intraday_sec_trans_tax
api_response.fees.swing_sec_trans_tax              = swing_sec_trans_tax

api_response.fees.intraday_sebi_turn_fees          = intraday_sebi_turn_fees
api_response.fees.swing_sebi_turn_fees             = swing_sebi_turn_fees

const update_form_data = new FormData()
update_form_data.append('trade_data',JSON.stringify(api_response.data))
update_form_data.append('fee_data',JSON.stringify(api_response.fees))

const update_response = await fetch(`/api/stock_pdf/`, {
method: 'PUT',
body: update_form_data
})

const update_data = await update_response.json();

create_notification(update_data.message, update_data.status)
} )

// Add Fee Component

document.getElementById("add_fee_button").addEventListener('click', async function(e) {
e.preventDefault();
let intraday_trading_present_class = ""
let swing_trading_present_class = ""
if (! trade_types.includes("Intraday Trading")){
trade_types.push("Intraday Trading")
} else if (! trade_types.includes("Swing Trading")){
trade_types.push("Swing Trading")
}

const fee_table = document.getElementById('fee_table')

fee_table.innerHTML = `
    <tr id = "fee_header">
    <th class="color-accent">Fee Type</th>
        <td class = '${intraday_trading_present_class}'>Intraday Trading Fees</td>
        <td class = '${swing_trading_present_class}'>Swing Trading Fees</td>
    </tr>
    <tr id = "net_obligation">
    <th class="color-accent">Net Obligation</th>
        <td class = '${intraday_trading_present_class}' contenteditable='true'>${global_data.fees.net_obligation}</td>
        <td class = '${swing_trading_present_class}' contenteditable='true'>${global_data.fees.net_obligation}</td>
    </tr>
    <tr id = "brokerage">
    <th class="color-accent">Brokerage</th>
        <td class = '${intraday_trading_present_class}' contenteditable='true'>${global_data.fees.brokerage}</td>
        <td class = '${swing_trading_present_class}' contenteditable='true'>${global_data.fees.brokerage}</td>
    </tr>
    <tr id = "exc_trans_charges">
    <th class="color-accent">Exchange Transaction Charges</th>
        <td class = '${intraday_trading_present_class}' contenteditable='true'>${global_data.fees.exc_trans_charges}</td>
        <td class = '${swing_trading_present_class}' contenteditable='true'>${global_data.fees.exc_trans_charges}</td>
    </tr>
    <tr id = "igst">
    <th class="color-accent">IGST</th>
        <td class = '${intraday_trading_present_class}' contenteditable='true'>${global_data.fees.igst}</td>
        <td class = '${swing_trading_present_class}' contenteditable='true'>${global_data.fees.igst}</td>
    </tr>
    <tr id = "sec_trans_tax">
    <th class="color-accent">Securities Transaction Tax</th>
        <td class = '${intraday_trading_present_class}' contenteditable='true'>${global_data.fees.sec_trans_tax}</td>
        <td class = '${swing_trading_present_class}' contenteditable='true'>${global_data.fees.sec_trans_tax}</td>
    </tr>
    <tr id = "sebi_turn_fees">
    <th class="color-accent">SEBI Turnover Fees</th>
        <td class = '${intraday_trading_present_class}' contenteditable='true'>${global_data.fees.sebi_turn_fees}</td>
        <td class = '${swing_trading_present_class}' contenteditable='true'>${global_data.fees.sebi_turn_fees}</td>
    </tr>
    <tr id = "auto_square_off_charges_row">
    <th class="color-accent">Auto Square Off Charges(If Any)</th>
        <td id = "intraday_auto_square_off_charges" class = '${intraday_trading_present_class}' contenteditable='true'></td>
        <td id = "swing_auto_square_off_charges" class = '${swing_trading_present_class}' contenteditable='true'></td>
    </tr>
    <tr id = "depository_charges_row">
    <th class="color-accent">Depository Charges(If Any)</th>
        <td id = "intraday_depository_charges" class = '${intraday_trading_present_class}' contenteditable='true'></td>
        <td id = "swing_depository_charges" class = '${swing_trading_present_class}' contenteditable='true'></td>
    </tr>`

})