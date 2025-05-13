// Historical data fetch

// Method : POST
// URL    : /get_hist_price/${symbol}

document.addEventListener('DOMContentLoaded', () => {
  if (document.getElementById('alt_symbol')) {
    init_alt_symbol_dropdown();
  }
});

async function init_alt_symbol_dropdown(){
const response = await fetch ('/api/name_list/', {
  method: 'GET'
})

const data = await response.json();
const name_input = document.getElementById('alt_symbol')
data.name_list.forEach(element => {
  name_input.innerHTML += `<option id = "options">${element}</option>`
});
}

/////////////////////////////////////////////////////////////////////////////////////////

document.getElementById('hist_price_form').addEventListener('submit', async function (e) {
e.preventDefault();
const alt_symbol = document.getElementById('alt_symbol').value;
const symbol_response = await fetch(`/api/symbol/${alt_symbol}`, {
method: 'GET'
})

const symbol_data = await symbol_response.json();
const symbol = symbol_data.symbol_list[0]

const start_date = document.getElementById('start_date').value;
const end_date = document.getElementById('end_date').value;
const formData = new FormData();
formData.append('symbol', symbol);
formData.append('alt_symbol', alt_symbol);
formData.append('start_date', start_date);
formData.append('end_date', end_date);

const response = await fetch(`/api/hist_price/`, {
method: 'POST',
body: formData
})

const data = await response.json();
const resultDiv = document.getElementById('result')

if(data.status === "Success"){
    resultDiv.innerHTML = `<strong>${data.message}</strong>`
    document.getElementById("hist_price_form").reset();
}
else{
    resultDiv.innerHTML = `<strong>${data.message}</strong>`
}
})

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
