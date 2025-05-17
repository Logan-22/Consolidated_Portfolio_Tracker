// Proc Date Insert/Update into Processing Date Table

// Method : GET
// URL    : /api/processing_date/

document.addEventListener('DOMContentLoaded', () => {
  if (document.getElementById('processing_date_form')) {
    init_processing_date();
  }
});

async function init_processing_date(){
const response = await fetch ('/api/processing_date/', {
  method: 'GET'
})

const data = await response.json();

if (data.status === 'Failed'){
const resultDiv = document.getElementById('result')
resultDiv.innerHTML = `<strong>${data.message}</strong>`
return
}

const mf_proc_date          = document.getElementById('mf_proc_date');
const mf_next_proc_date     = document.getElementById('mf_next_proc_date');
const mf_prev_proc_date     = document.getElementById('mf_prev_proc_date');

const ppf_mf_proc_date      = document.getElementById('ppf_mf_proc_date');
const ppf_mf_next_proc_date = document.getElementById('ppf_mf_next_proc_date');
const ppf_mf_prev_proc_date = document.getElementById('ppf_mf_prev_proc_date');

const stock_proc_date       = document.getElementById('stock_proc_date');
const stock_next_proc_date  = document.getElementById('stock_next_proc_date');
const stock_prev_proc_date  = document.getElementById('stock_prev_proc_date');

mf_proc_date.value      = data.mf_proc_date
mf_next_proc_date.value = data.mf_next_proc_date
mf_prev_proc_date.value = data.mf_prev_proc_date

ppf_mf_proc_date.value      = data.ppf_mf_proc_date
ppf_mf_next_proc_date.value = data.ppf_mf_next_proc_date
ppf_mf_prev_proc_date.value = data.ppf_mf_prev_proc_date

stock_proc_date.value      = data.stock_proc_date
stock_next_proc_date.value = data.stock_next_proc_date
stock_prev_proc_date.value = data.stock_prev_proc_date

}

/////////////////////////////////////////////////////////////////////////////////////////

document.getElementById('processing_date_form').addEventListener('submit', async function (e) {
e.preventDefault();

const mf_proc_date          = document.getElementById('mf_proc_date').value;
const mf_next_proc_date     = document.getElementById('mf_next_proc_date').value;
const mf_prev_proc_date     = document.getElementById('mf_prev_proc_date').value;

const ppf_mf_proc_date      = document.getElementById('ppf_mf_proc_date').value;
const ppf_mf_next_proc_date = document.getElementById('ppf_mf_next_proc_date').value;
const ppf_mf_prev_proc_date = document.getElementById('ppf_mf_prev_proc_date').value;

const stock_proc_date       = document.getElementById('stock_proc_date').value;
const stock_next_proc_date  = document.getElementById('stock_next_proc_date').value;
const stock_prev_proc_date  = document.getElementById('stock_prev_proc_date').value;

const formData = new FormData();

formData.append('mf_proc_date', mf_proc_date);
formData.append('mf_next_proc_date', mf_next_proc_date);
formData.append('mf_prev_proc_date', mf_prev_proc_date);

formData.append('ppf_mf_proc_date', ppf_mf_proc_date);
formData.append('ppf_mf_next_proc_date', ppf_mf_next_proc_date);
formData.append('ppf_mf_prev_proc_date', ppf_mf_prev_proc_date);

formData.append('stock_proc_date', stock_proc_date);
formData.append('stock_next_proc_date', stock_next_proc_date);
formData.append('stock_prev_proc_date', stock_prev_proc_date);

const response = await fetch(`/api/processing_date/`, {
method: 'POST',
body: formData
})

const data = await response.json();
const resultDiv = document.getElementById('result')

if(data.status === "Success"){
    resultDiv.innerHTML = `<strong>${data.message}</strong>`
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
