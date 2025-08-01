import { create_notification } from './create_notification.js'

// Metadata Entry into Metadata Table

// Method : POST
// URL    : /api/metadata/

// Hide Options if not Mutual Fund

toggle_hide_in_metadata_entry_form();

async function toggle_hide_in_metadata_entry_form() {
const portfolio_type = document.getElementById('portfolio_type');

if(portfolio_type){
let portfolio_type_value = portfolio_type.value
const mf_optional_form = document.getElementById('mf_optional_form')
const stock_optional_form = document.getElementById('stock_optional_form')
if(portfolio_type_value === 'Mutual Fund'){
mf_optional_form.classList.remove('hidden')
stock_optional_form.classList.add('hidden')
}
else if (portfolio_type_value === 'Stock'){
stock_optional_form.classList.remove('hidden')
mf_optional_form.classList.add('hidden')
} else {
stock_optional_form.classList.add('hidden')
mf_optional_form.classList.add('hidden')
}
}

if (portfolio_type) {
    portfolio_type.addEventListener('click', () => {
let portfolio_type_value = portfolio_type.value
const mf_optional_form = document.getElementById('mf_optional_form')
const stock_optional_form = document.getElementById('stock_optional_form')
if(portfolio_type_value === 'Mutual Fund'){
mf_optional_form.classList.remove('hidden')
stock_optional_form.classList.add('hidden')
}
else if (portfolio_type_value === 'Stock'){
stock_optional_form.classList.remove('hidden')
mf_optional_form.classList.add('hidden')
} else {
stock_optional_form.classList.add('hidden')
mf_optional_form.classList.add('hidden')
}
});
}
}

document.getElementById('exchange_symbol').addEventListener('input', function (e){
e.preventDefault();
let exchange_symbol   = document.getElementById('exchange_symbol').value;
const alt_symbol      = document.getElementById('alt_symbol');
alt_symbol.value      = exchange_symbol.replaceAll(" ", "_").replaceAll(".", "_").toLowerCase()
})


document.getElementById('metadata_entry_form').addEventListener('submit', async function (e) {
e.preventDefault();
const exchange_symbol         = document.getElementById('exchange_symbol').value;
const yahoo_symbol            = document.getElementById('yahoo_symbol').value;
const alt_symbol              = document.getElementById('alt_symbol').value;
const allocation_category     = document.getElementById('allocation_category').value;
const portfolio_type          = document.getElementById('portfolio_type').value;
const amc                     = document.getElementById('amc').value
const type                    = document.getElementById('type').value
const fund_category           = document.getElementById('fund_category').value
const launched_on             = document.getElementById('launched_on').value
const exit_load               = document.getElementById('exit_load').value
const expense_ratio           = document.getElementById('expense_ratio').value
const fund_manager            = document.getElementById('fund_manager').value
const fund_manager_started_on = document.getElementById('fund_manager_started_on').value
const isin                    = document.getElementById('isin').value
const process_flag            = document.getElementById('process_flag').checked
const consider_for_returns    = document.getElementById('consider_for_returns').checked

const metadata_payload = {
'EXCHANGE_SYMBOL'            : exchange_symbol
,'YAHOO_SYMBOL'              : yahoo_symbol
,'ALT_SYMBOL'                : alt_symbol
,'ALLOCATION_CATEGORY'       : allocation_category
,'PORTFOLIO_TYPE'            : portfolio_type
,'AMC'                       : amc
,'MF_TYPE'                   : type
,'FUND_CATEGORY'             : fund_category
,'LAUNCHED_ON'               : launched_on
,'EXIT_LOAD'                 : exit_load
,'EXPENSE_RATIO'             : expense_ratio
,'FUND_MANAGER'              : fund_manager
,'FUND_MANAGER_STARTED_ON'   : fund_manager_started_on
,'ISIN'                      : isin
,'PROCESS_FLAG'              : process_flag == true ? 1 : 0
,'CONSIDER_FOR_RETURNS'      : consider_for_returns == true ? 1 : 0
}

const formData = new FormData();
formData.append('metadata_payload', JSON.stringify(metadata_payload));

const metadata_post_response = await fetch(`/api/metadata_store/`, {
method: 'POST',
body: formData
})

const metadata_post_data = await metadata_post_response.json();

create_notification(metadata_post_data.message, metadata_post_data.status)

if(metadata_post_data.status == "Success")
document.getElementById('metadata_entry_form').reset()
})