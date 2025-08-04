import { create_notification } from './create_notification.js'

// Proc Date Insert/Update into Processing Date Table

// Method : GET
// URL    : /api/processing_date/

document.addEventListener('DOMContentLoaded', () => {
  if (document.getElementById('processing_date_form')) {
    init_processing_date()
  }
})

async function init_processing_date(){
const processing_date_response = await fetch ('/api/processing_date/', {
  method: 'GET'
})

const processing_date_data = await processing_date_response.json()

const mf_proc_date             = document.getElementById('mf_proc_date')
const mf_next_proc_date        = document.getElementById('mf_next_proc_date')
const mf_prev_proc_date        = document.getElementById('mf_prev_proc_date')

const ppf_mf_proc_date         = document.getElementById('ppf_mf_proc_date')
const ppf_mf_next_proc_date    = document.getElementById('ppf_mf_next_proc_date')
const ppf_mf_prev_proc_date    = document.getElementById('ppf_mf_prev_proc_date')

const stock_proc_date          = document.getElementById('stock_proc_date')
const stock_next_proc_date     = document.getElementById('stock_next_proc_date')
const stock_prev_proc_date     = document.getElementById('stock_prev_proc_date')

const sim_mf_proc_date         = document.getElementById('sim_mf_proc_date')
const sim_mf_next_proc_date    = document.getElementById('sim_mf_next_proc_date')
const sim_mf_prev_proc_date    = document.getElementById('sim_mf_prev_proc_date')

const sim_stock_proc_date      = document.getElementById('sim_stock_proc_date')
const sim_stock_next_proc_date = document.getElementById('sim_stock_next_proc_date')
const sim_stock_prev_proc_date = document.getElementById('sim_stock_prev_proc_date')

processing_date_data.proc_date_data.forEach(proc_date_type => {
if(proc_date_type['PROC_TYP_CD'] == 'MF_PROC'){
mf_proc_date.value             = proc_date_type['PROC_DATE']
mf_next_proc_date.value        = proc_date_type['PREV_PROC_DATE']
mf_prev_proc_date.value        = proc_date_type['NEXT_PROC_DATE']
}
if(proc_date_type['PROC_TYP_CD'] == 'PPF_MF_PROC'){
ppf_mf_proc_date.value         = proc_date_type['PROC_DATE']
ppf_mf_next_proc_date.value    = proc_date_type['PREV_PROC_DATE']
ppf_mf_prev_proc_date.value    = proc_date_type['NEXT_PROC_DATE']
}
if(proc_date_type['PROC_TYP_CD'] == 'STOCK_PROC'){
stock_proc_date.value          = proc_date_type['PROC_DATE']
stock_next_proc_date.value     = proc_date_type['PREV_PROC_DATE']
stock_prev_proc_date.value     = proc_date_type['NEXT_PROC_DATE']
}
if(proc_date_type['PROC_TYP_CD'] == 'SIM_MF_PROC'){
sim_mf_proc_date.value         = proc_date_type['PROC_DATE']
sim_mf_next_proc_date.value    = proc_date_type['PREV_PROC_DATE']
sim_mf_prev_proc_date.value    = proc_date_type['NEXT_PROC_DATE']
}
if(proc_date_type['PROC_TYP_CD'] == 'SIM_STOCK_PROC'){
sim_stock_proc_date.value      = proc_date_type['PROC_DATE']
sim_stock_prev_proc_date.value = proc_date_type['PREV_PROC_DATE']
sim_stock_next_proc_date.value = proc_date_type['NEXT_PROC_DATE']
}
})
create_notification(processing_date_data.message, processing_date_data.status)
}

/////////////////////////////////////////////////////////////////////////////////////////

document.getElementById('processing_date_form').addEventListener('submit', async function (e) {
e.preventDefault()

const processing_date_payload = {}

processing_date_payload['mf_proc_date']             = document.getElementById('mf_proc_date').value
processing_date_payload['mf_next_proc_date']        = document.getElementById('mf_next_proc_date').value
processing_date_payload['mf_prev_proc_date']        = document.getElementById('mf_prev_proc_date').value

processing_date_payload['ppf_mf_proc_date']         = document.getElementById('ppf_mf_proc_date').value
processing_date_payload['ppf_mf_next_proc_date']    = document.getElementById('ppf_mf_next_proc_date').value
processing_date_payload['ppf_mf_prev_proc_date']    = document.getElementById('ppf_mf_prev_proc_date').value

processing_date_payload['stock_proc_date']          = document.getElementById('stock_proc_date').value
processing_date_payload['stock_next_proc_date']     = document.getElementById('stock_next_proc_date').value
processing_date_payload['stock_prev_proc_date']     = document.getElementById('stock_prev_proc_date').value

processing_date_payload['sim_mf_proc_date']         = document.getElementById('sim_mf_proc_date').value
processing_date_payload['sim_mf_next_proc_date']    = document.getElementById('sim_mf_next_proc_date').value
processing_date_payload['sim_mf_prev_proc_date']    = document.getElementById('sim_mf_prev_proc_date').value

processing_date_payload['sim_stock_proc_date']      = document.getElementById('sim_stock_proc_date').value
processing_date_payload['sim_stock_next_proc_date'] = document.getElementById('sim_stock_next_proc_date').value
processing_date_payload['sim_stock_prev_proc_date'] = document.getElementById('sim_stock_prev_proc_date').value

const formData = new FormData()

formData.append('processing_date_payload', JSON.stringify(processing_date_payload))

const post_processing_date_response = await fetch(`/api/processing_date/`, {
method: 'POST',
body: formData
})

const post_processing_date_data = await post_processing_date_response.json()

create_notification(post_processing_date_data.message, post_processing_date_data.status)

init_processing_date()
})