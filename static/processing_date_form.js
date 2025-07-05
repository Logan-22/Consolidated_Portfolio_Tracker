import { create_notification } from './create_notification.js'

// Proc Date Insert/Update into Processing Date Table

// Method : GET
// URL    : /api/processing_date/

document.addEventListener('DOMContentLoaded', () => {
  if (document.getElementById('processing_date_form')) {
    init_processing_date();
  }
});

async function init_processing_date(){
const processing_date_response = await fetch ('/api/processing_date/', {
  method: 'GET'
})

const processing_date_data = await processing_date_response.json();

create_notification(processing_date_data.message, processing_date_data.status)

const mf_proc_date          = document.getElementById('mf_proc_date');
const mf_next_proc_date     = document.getElementById('mf_next_proc_date');
const mf_prev_proc_date     = document.getElementById('mf_prev_proc_date');

const ppf_mf_proc_date      = document.getElementById('ppf_mf_proc_date');
const ppf_mf_next_proc_date = document.getElementById('ppf_mf_next_proc_date');
const ppf_mf_prev_proc_date = document.getElementById('ppf_mf_prev_proc_date');

const stock_proc_date       = document.getElementById('stock_proc_date');
const stock_next_proc_date  = document.getElementById('stock_next_proc_date');
const stock_prev_proc_date  = document.getElementById('stock_prev_proc_date');

mf_proc_date.value          = processing_date_data.mf_proc_date
mf_next_proc_date.value     = processing_date_data.mf_next_proc_date
mf_prev_proc_date.value     = processing_date_data.mf_prev_proc_date

ppf_mf_proc_date.value      = processing_date_data.ppf_mf_proc_date
ppf_mf_next_proc_date.value = processing_date_data.ppf_mf_next_proc_date
ppf_mf_prev_proc_date.value = processing_date_data.ppf_mf_prev_proc_date

stock_proc_date.value       = processing_date_data.stock_proc_date
stock_next_proc_date.value  = processing_date_data.stock_next_proc_date
stock_prev_proc_date.value  = processing_date_data.stock_prev_proc_date

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

const post_processing_date_response = await fetch(`/api/processing_date/`, {
method: 'POST',
body: formData
})

const post_processing_date_data = await post_processing_date_response.json();

create_notification(post_processing_date_data.message, post_processing_date_data.status)
})