// Insert into corresponding NAV Table when the site loads

// Method : GET
// URL    : /api/hist_price/

document.addEventListener('DOMContentLoaded', () => {
  if (document.getElementById('index_nav')) {
    upsert_nav_tables();
  }
});

async function upsert_nav_tables(){

const max_date_response = await fetch ('/api/max_date/', {
  method: 'GET'
})

const max_date_data = await max_date_response.json();

for (table_name in max_date_data.max_date_from_tables){
const max_start_date = max_date_data.max_date_from_tables[table_name]
const alt_symbol = String(table_name).replaceAll('_', ' ').replaceAll("'", "")

const symbol_response = await fetch(`/api/symbol/${alt_symbol}/`, {
method: 'GET'
})

const symbol_data = await symbol_response.json();
const symbol = symbol_data.symbol_list[0]

const start_date = max_start_date

const today = new Date();
const year = today.getFullYear();
const month = String(today.getMonth() + 1).padStart(2, '0');
const day = String(today.getDate()).padStart(2, '0');

const end_date = `${year}-${month}-${day}`
const formData = new FormData();
formData.append('alt_symbol', alt_symbol);

await fetch(`/api/hist_price/${symbol}/${start_date}/${end_date}`, {
method: 'POST',
body: formData
})

//end of for loop
}

const dup_check_response = await fetch(`/api/nav/dup_check/`, {
method: 'GET'
})

const dup_check_data = await dup_check_response.json();

const resultDiv = document.getElementById('result')
if(dup_check_data.status === "Success"){
    resultDiv.innerHTML += `<strong>${dup_check_data.message}</strong></br>`
    resultDiv.innerHTML += `<strong>${dup_check_data.status}</strong></br>`
}
else{
    resultDiv.innerHTML += `<strong>${dup_check_data.message}</strong></br>`
    for (dup_table in dup_check_data.dup_tables){
    resultDiv.innerHTML += `<strong>${dup_check_data.status}</strong></br>`
      resultDiv.innerHTML += `<strong>${dup_table} ---> ${dup_check_data.dup_tables[dup_table]} Value Date</strong></br>`
    }
}
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
