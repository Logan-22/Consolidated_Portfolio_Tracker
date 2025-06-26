// MF Order Entry into MF Order Table

// Method : POST
// URL    : /api/mf_order/

document.addEventListener('DOMContentLoaded', () => {
  if (document.getElementById('exchange_symbol')) {
    init_alt_symbol_dropdown();
  }
});

let mutual_fund_symbol_data = {}

async function init_alt_symbol_dropdown(){
const mutual_fund_symbol_response = await fetch ('/api/metadata_store/symbols?portfolio_type=Mutual+Fund', {
  method: 'GET'
})

mutual_fund_symbol_data = await mutual_fund_symbol_response.json();

const exchange_symbol = document.getElementById('exchange_symbol')
const alt_symbol      = document.getElementById('alt_symbol')
mutual_fund_symbol_data.all_symbols_list.forEach((element, index) => {
exchange_symbol.innerHTML += `<option id = "options">${element.exchange_symbol}</option>`
if(index == 0){
alt_symbol.value = element.alt_symbol
  }
});
}

document.getElementById('exchange_symbol').addEventListener('change', e => {
const exchange_symbol = document.getElementById('exchange_symbol').value
const alt_symbol      = document.getElementById('alt_symbol')
mutual_fund_symbol_data.all_symbols_list.forEach(element => {
if(exchange_symbol == element.exchange_symbol){
alt_symbol.value = element.alt_symbol
  }
});
})

/////////////////////////////////////////////////////////////////////////////////////////

document.getElementById('mf_order_entry_form').addEventListener('submit', async function (e) {
e.preventDefault();

const exchange_symbol = document.getElementById('exchange_symbol').value;
const alt_symbol      = document.getElementById('alt_symbol').value;
const purchase_date   = document.getElementById('purchase_date').value;
const invested_amount = document.getElementById('invested_amount').value;
const amc_amount      = document.getElementById('amc_amount').value;

const price_lookup_response = await fetch(`/api/price_table/close_price/?alt_symbol=${alt_symbol}&purchase_date=${purchase_date}`, {
method: 'GET'
})

const price_lookup_data = await price_lookup_response.json();
const resultDiv = document.getElementById('result')
if (price_lookup_data.price_data.length > 1){
resultDiv.innerHTML = `<strong>Duplicate Price fetched for ${alt_symbol} on ${purchase_date} from PRICE_TABLE</strong>`
}else if (! price_lookup_data.price_data[0].price){
resultDiv.innerHTML = `<strong>No Price fetched for ${alt_symbol} on ${purchase_date} from PRICE_TABLE</strong>`
}
else{
const price_during_purchase = price_lookup_data.price_data[0].price

const units = amc_amount/price_during_purchase

const formData = new FormData();
formData.append('exchange_symbol', exchange_symbol);
formData.append('purchase_date', purchase_date);
formData.append('invested_amount', invested_amount);
formData.append('amc_amount', amc_amount);
formData.append('price_during_purchase', price_during_purchase);
formData.append('units', units);

const mf_order_response = await fetch(`/api/mf_order/`, {
method: 'POST',
body: formData
})

const mf_order_data = await mf_order_response.json();

if(mf_order_data.status === "Success"){
    resultDiv.innerHTML = `<strong>${mf_order_data.message}</strong>`
    document.getElementById("mf_order_entry_form").reset();
}
else{
    resultDiv.innerHTML = `<strong>${mf_order_data.message}</strong>`
}
}
})

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
