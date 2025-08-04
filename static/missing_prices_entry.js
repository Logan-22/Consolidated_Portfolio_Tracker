import { create_notification } from './create_notification.js'

// Metadata Entry into Metadata Table

// Method : POST
// URL    : /api/missing_prices/

let missing_prices_data;

document.addEventListener('DOMContentLoaded', () => {
  if (document.getElementById('missing_entry_list_div')) {
    init_missing_prices_form();
  }
});

async function init_missing_prices_form(){
const missing_entry_list_div = document.getElementById('missing_entry_list_div')

let missing_entry_list_div_inner_html = ""
const missing_prices_response = await fetch('/api/missing_prices/', {
method: "GET"
})

missing_prices_data = await missing_prices_response.json()

missing_prices_data.missing_price_data.forEach((missing_price,index) => {
if (missing_price['ALT_SYMBOL']){
missing_entry_list_div_inner_html += 
`
<div class = "field">
    <label for = "${missing_price['ALT_SYMBOL']}_${index}">
    <b>Missing Portfolio Name</b></label>
    <input type = "text" id = "${missing_price['ALT_SYMBOL']}_${index}" value = "${missing_price['ALT_SYMBOL']}" readonly/>
</div>
<div class = "field">
    <label for = "${missing_price['VALUE_DATE']}_${index}">
    <b>Missing Date</b></label>
    <input type = "date" id = "${missing_price['VALUE_DATE']}_${index}" value = "${missing_price['VALUE_DATE']}" readonly/>
</div>
<div class = "field">
    <label for = "${missing_price['ALT_SYMBOL']}_${missing_price['VALUE_DATE']}">
    <b>Missing Price</b></label>
    <input type = "number" id = "${missing_price['ALT_SYMBOL']}_${missing_price['VALUE_DATE']}" step="any"/>
</div>
`
}
else{
  missing_entry_list_div_inner_html = "<h3>No Missing Prices</h3>"
}
})

missing_entry_list_div.innerHTML = missing_entry_list_div_inner_html
}

///////////////////////////////////////////////////////////////////////////
document.getElementById('missing_prices_entry_form').addEventListener('submit', async function (e) {
e.preventDefault();
const missing_price_payloads = []

missing_prices_data.missing_price_data.forEach(missing_price_data => {
const missing_price = document.getElementById(`${missing_price_data['ALT_SYMBOL']}_${missing_price_data['VALUE_DATE']}`)
if (missing_price.value){
const payload = {
    'ALT_SYMBOL'     : missing_price_data['ALT_SYMBOL']
    ,'PORTFOLIO_TYPE': missing_price_data['PORTFOLIO_TYPE']
    ,'VALUE_DATE'    : missing_price_data['VALUE_DATE']
    ,'VALUE_TIME'    : '15:30:00'
    ,'PRICE'         : missing_price.value
    ,'PRICE_TYP_CD'  : 'CLOSE_PRICE'
}
missing_price_payloads.push(payload)
}
})

const form_data = new FormData()
form_data.append('missing_price_payload', JSON.stringify(missing_price_payloads))

const missing_price_post_response = await fetch(`/api/missing_prices/`, {
method: 'POST',
body: form_data
})

const missing_price_post_data = await missing_price_post_response.json();

create_notification(missing_price_post_data.message, missing_price_post_data.status)

init_missing_prices_form()
})
