import { create_notification } from './create_notification.js'

// Metadata Entry into Metadata Table

// Method : POST
// URL    : /api/metadata/

// Hide Options if not Mutual Fund


toggle_hide_in_metadata_entry_form();

async function toggle_hide_in_metadata_entry_form() {
const portfolio_type = document.getElementById('portfolio_type');

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

const formData = new FormData();
formData.append('exchange_symbol', exchange_symbol);
formData.append('yahoo_symbol', yahoo_symbol);
formData.append('alt_symbol', alt_symbol);
formData.append('portfolio_type', portfolio_type);
formData.append('amc', amc);
formData.append('type', type);
formData.append('fund_category', fund_category);
formData.append('launched_on', launched_on);
formData.append('exit_load', exit_load);
formData.append('expense_ratio', expense_ratio);
formData.append('fund_manager', fund_manager);
formData.append('fund_manager_started_on', fund_manager_started_on);
formData.append('isin', isin);

const metadata_post_response = await fetch(`/api/metadata_store/`, {
method: 'POST',
body: formData
})

const metadata_post_data = await metadata_post_response.json();

create_notification(metadata_post_data.message, metadata_post_data.status)
})