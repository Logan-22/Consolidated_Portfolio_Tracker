// MF Order Entry into MF Order Table

// Method : POST
// URL    : /api/mf_order/

document.addEventListener('DOMContentLoaded', () => {
  if (document.getElementById('alt_symbol')) {
    init_alt_symbol_dropdown();
  }
});

async function init_alt_symbol_dropdown(){
const response = await fetch ('/api/mf_name_list/', {
  method: 'GET'
})

const data = await response.json();
const name_input = document.getElementById('alt_symbol')
data.name_list.forEach(element => {
  name_input.innerHTML += `<option id = "options">${element}</option>`
});
}

/////////////////////////////////////////////////////////////////////////////////////////

document.getElementById('mf_order_entry_form').addEventListener('submit', async function (e) {
e.preventDefault();


const alt_symbol = document.getElementById('alt_symbol').value;
const purchase_date = document.getElementById('purchase_date').value;
const invested_amount = document.getElementById('invested_amount').value;
const amc_amount = document.getElementById('amc_amount').value;

const table_name = alt_symbol.replace(/ /g,"_");


const nav_lookup_response = await fetch(`/api/nav_lookup/${table_name}/${purchase_date}`, {
method: 'GET'
})

const nav_lookup_data = await nav_lookup_response.json();
const nav_during_purchase = nav_lookup_data.nav

const units = amc_amount/nav_during_purchase

const formData = new FormData();
formData.append('alt_symbol', alt_symbol);
formData.append('purchase_date', purchase_date);
formData.append('invested_amount', invested_amount);
formData.append('amc_amount', amc_amount);
formData.append('nav_during_purchase', nav_during_purchase);
formData.append('units', units);

const response = await fetch(`/api/mf_order/`, {
method: 'POST',
body: formData
})

const data = await response.json();
const resultDiv = document.getElementById('result')

if(data.status === "Success"){
    resultDiv.innerHTML = `<strong>${data.message}</strong>`
    document.getElementById("mf_order_entry_form").reset();
}
else{
    resultDiv.innerHTML = `<strong>${data.message}</strong>`
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
