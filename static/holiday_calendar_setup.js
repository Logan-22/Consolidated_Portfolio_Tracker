import { create_notification } from './create_notification.js'

// Holiday Entry into Holiday Date Table

// Method : POST
// URL    : /api/holiday_date/

document.addEventListener('DOMContentLoaded', () => {
  if (document.getElementById('holiday_date_setup_form')) {
    init_holiday_table()
  }
});

async function init_holiday_table(){

const today = new Date()
const current_year = today.getFullYear()

const get_current_year_holiday_response = await fetch (`/api/holiday_date?current_year=${current_year}`, {
  method: 'GET'
})

const get_current_year_holiday_data = await get_current_year_holiday_response.json()

if(get_current_year_holiday_data.status === "Success"){
    const holiday_table = document.getElementById('holiday_table')
    holiday_table.innerHTML = "<tr><th class='color-accent'>Holiday Date</th><th class='color-accent'>Holiday Name</th><th class='color-accent'>Holiday Day</th></tr>"

    get_current_year_holiday_data.data.forEach(element => {
    holiday_table.innerHTML += `<tr><td>${element['HOLIDAY_DATE']}</td><td>${element['HOLIDAY_NAME']}</td><td>${element['HOLIDAY_DAY']}</td></tr>`
})
holiday_table.innerHTML += "</table>"
}

create_notification(get_current_year_holiday_data.message, get_current_year_holiday_data.status)
}

/////////////////////////////////////////////////////////////////////////////////////////

document.getElementById('holiday_date_setup_form').addEventListener('submit', async function (e) {
e.preventDefault();


const holiday_date = document.getElementById('holiday_date').value
const holiday_name = document.getElementById('holiday_name').value

const holiday = new Date(holiday_date)
const day = holiday.getDay()
let holiday_day

switch (day) {
  case 0: holiday_day = "Sunday";    break;
  case 1: holiday_day = "Monday";    break;
  case 2: holiday_day = "Tuesday";   break;
  case 3: holiday_day = "Wednesday"; break;
  case 4: holiday_day = "Thursday";  break;
  case 5: holiday_day = "Friday";    break;
  case 6: holiday_day = "Saturday";  break;
}

const holiday_date_payload = {
'HOLIDAY_DATE'  : holiday_date
,'HOLIDAY_NAME' : holiday_name
,'HOLIDAY_DAY'  : holiday_day
}

const formData = new FormData()
formData.append('holiday_date_payload', JSON.stringify(holiday_date_payload))

const post_holiday_response = await fetch(`/api/holiday_date/`, {
method: 'POST',
body: formData
})

const post_holiday_data = await post_holiday_response.json()

if(post_holiday_data.status === "Success"){
    document.getElementById("holiday_date_setup_form").reset()
    init_holiday_table()
}

create_notification(post_holiday_data.message, post_holiday_data.status)
})

/////////////////////////////////////////////////////////////////////////////////////////

document.getElementById('working_date_setup_form').addEventListener('submit', async function (e) {
e.preventDefault();

const working_date     = document.getElementById('working_date').value
const working_day_name = document.getElementById('working_day_name').value

const working = new Date(working_date)
const day = working.getDay()

let working_day

switch (day) {
  case 0: working_day = "Sunday";    break;
  case 1: working_day = "Monday";    break;
  case 2: working_day = "Tuesday";   break;
  case 3: working_day = "Wednesday"; break;
  case 4: working_day = "Thursday";  break;
  case 5: working_day = "Friday";    break;
  case 6: working_day = "Saturday";  break;
}

const working_date_payload = {
'WORKING_DATE'  : working_date
,'WORKING_DAY_NAME' : working_day_name
,'WORKING_DAY'  : working_day
}

const formData = new FormData();
formData.append('working_date_payload', JSON.stringify(working_date_payload))

const post_working_day_response = await fetch(`/api/working_date/`, {
method: 'POST',
body: formData
})

const post_working_day_data = await post_working_day_response.json();

create_notification(post_working_day_data.message, post_working_day_data.status)
})

/////////////////////////////////////////////////////////////////////////////////////////

document.getElementById('holiday_calendar_setup_form').addEventListener('submit', async function (e) {
e.preventDefault();

const holiday_calendar_start_date = document.getElementById('holiday_calendar_start_date').value;
const holiday_calendar_end_date   = document.getElementById('holiday_calendar_end_date').value;

const holiday_date_response = await fetch(`/api/holiday_date/`, {
method: 'GET'
})

const holiday_date_data = await holiday_date_response.json();

create_notification(holiday_date_data.message, holiday_date_data.status)

const holiday_list = holiday_date_data.data

const working_date_response = await fetch(`/api/working_date/`, {
method: 'GET'
})

const working_date_data = await working_date_response.json();

create_notification(working_date_data.message, working_date_data.status)

const working_day_list  = working_date_data.data

const formData = new FormData();
formData.append('holiday_calendar_start_date', holiday_calendar_start_date);
formData.append('holiday_calendar_end_date', holiday_calendar_end_date);

const holiday_data = []
const working_day_data = []

if(holiday_date_data.status == "Success" && working_date_data.status == "Success"){
holiday_list.forEach(holiday => holiday_data.push(holiday.holiday_date))
if(working_day_list){
  working_day_list.forEach(working_day => working_day_data.push(working_day.working_date))
}

formData.append('holiday_data', JSON.stringify(holiday_data));
formData.append('working_day_data', JSON.stringify(working_day_data));

const setup_holiday_calendar_response = await fetch(`/api/holiday_calendar_setup/?start_date=${holiday_calendar_start_date}&end_date=${holiday_calendar_end_date}`, {
method: 'POST',
body: formData
})

const setup_holiday_calendar_data = await setup_holiday_calendar_response.json();

create_notification(setup_holiday_calendar_data.message, setup_holiday_calendar_data.status)
}
})