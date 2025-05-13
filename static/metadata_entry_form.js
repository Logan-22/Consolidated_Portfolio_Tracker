// Metadata Entry into Metadata Table

// Method : POST
// URL    : /api/metadata/

document.getElementById('metadata_entry_form').addEventListener('submit', async function (e) {
e.preventDefault();
const symbol = document.getElementById('symbol').value;
const alt_symbol = document.getElementById('alt_symbol').value;
const amc = document.getElementById('amc').value;
const type = document.getElementById('type').value;
const fund_category = document.getElementById('fund_category').value;
const launched_on = document.getElementById('launched_on').value;
const exit_load = document.getElementById('exit_load').value;
const expense_ratio = document.getElementById('expense_ratio').value;
const fund_manager = document.getElementById('fund_manager').value;
const fund_manager_started_on = document.getElementById('fund_manager_started_on').value;

const formData = new FormData();
formData.append('symbol', symbol);
formData.append('alt_symbol', alt_symbol);
formData.append('amc', amc);
formData.append('type', type);
formData.append('fund_category', fund_category);
formData.append('launched_on', launched_on);
formData.append('exit_load', exit_load);
formData.append('expense_ratio', expense_ratio);
formData.append('fund_manager', fund_manager);
formData.append('fund_manager_started_on', fund_manager_started_on);

const response = await fetch(`/api/metadata/`, {
method: 'POST',
body: formData
})

const data = await response.json();
const resultDiv = document.getElementById('result')

if(data.status === "Success"){
    resultDiv.innerHTML = `<strong>${data.message}</strong>`
    document.getElementById("metadata_entry_form").reset();
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
