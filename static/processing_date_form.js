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

const current_date      = document.getElementById('current_date')
const mf_proc_date      = document.getElementById('mf_proc_date')
const ppfs_mf_proc_date = document.getElementById('ppfs_mf_proc_date')
const stock_proc_date   = document.getElementById('stock_proc_date')

current_date.value      = data.current_date
mf_proc_date.value      = data.mf_proc_date
ppfs_mf_proc_date.value = data.ppfs_mf_proc_date
stock_proc_date.value   = data.stock_proc_date
}

/////////////////////////////////////////////////////////////////////////////////////////

document.getElementById('processing_date_form').addEventListener('submit', async function (e) {
e.preventDefault();


const current_date      = document.getElementById('current_date').value;
const mf_proc_date      = document.getElementById('mf_proc_date').value;
const ppfs_mf_proc_date = document.getElementById('ppfs_mf_proc_date').value;
const stock_proc_date   = document.getElementById('stock_proc_date').value;

const formData = new FormData();
formData.append('current_date', current_date);
formData.append('mf_proc_date', mf_proc_date);
formData.append('ppfs_mf_proc_date', ppfs_mf_proc_date);
formData.append('stock_proc_date', stock_proc_date);

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
