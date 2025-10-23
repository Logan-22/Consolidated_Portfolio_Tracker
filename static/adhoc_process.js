import { create_notification } from './create_notification.js'

// Ad hoc Processing

// Method : POST
// URL    : /api/process/

document.getElementById('adhoc_process_form').addEventListener('submit', async function (e) {
e.preventDefault();
const user_id       = document.getElementById('user_id').value
const instrument_id = document.getElementById('instrument_id').value
const process_type  = document.getElementById('process_type').value
const start_date    = document.getElementById('start_date').value
const end_date      = document.getElementById('end_date').value

const process_payload = {
'user_id'        : user_id
,'instrument_id' : instrument_id
,'process_type'  : process_type
,'start_date'    : start_date
,'end_date'      : end_date
}

const formData = new FormData();
formData.append('process_payload', JSON.stringify(process_payload));

const process_post_response = await fetch(`/api/process/`, {
method: 'POST',
body: formData
})

const process_post_data = await process_post_response.json();

create_notification(process_post_data.message, process_post_data.status)

if(process_post_data.status == "Success")
document.getElementById('adhoc_process_form').reset()
})