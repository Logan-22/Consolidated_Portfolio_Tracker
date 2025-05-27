// Holiday Entry into Holiday Date Table

// Method : POST
// URL    : /api/holiday_date/

document.addEventListener('DOMContentLoaded', () => {
  if (document.getElementById('holiday_date_setup_form')) {
    init_holiday_table();
  }
});

async function init_holiday_table(){

const today = new Date();
const current_year = today.getFullYear();

const response = await fetch (`/api/holiday_date/${current_year}`, {
  method: 'GET'
})

const get_data = await response.json();
const resultDiv = document.getElementById('result')

if(get_data.status === "Success"){
    const holiday_table = document.getElementById('holiday_table')
    holiday_table.innerHTML = "<tr><th>Holiday Date</th><th>Holiday Name</th><th>Holiday Day</th></tr>"

    get_data.data.forEach(element => {
    holiday_table.innerHTML += `<tr><td>${element.holiday_date}</td><td>${element.holiday_name}</td><td>${element.holiday_day}</td></tr>`
})

holiday_table.innerHTML += "</table>"
}
else{
    resultDiv.innerHTML = `<strong>${data.message}</strong>`
}
}

/////////////////////////////////////////////////////////////////////////////////////////

document.getElementById('holiday_date_setup_form').addEventListener('submit', async function (e) {
e.preventDefault();


const holiday_date = document.getElementById('holiday_date').value;
const holiday_name = document.getElementById('holiday_name').value;

holiday = new Date(holiday_date)
day = holiday.getDay()

switch (day) {
  case 0: holiday_day = "Sunday";    break;
  case 1: holiday_day = "Monday";    break;
  case 2: holiday_day = "Tuesday";   break;
  case 3: holiday_day = "Wednesday"; break;
  case 4: holiday_day = "Thursday";  break;
  case 5: holiday_day = "Friday";    break;
  case 6: holiday_day = "Saturday";  break;
}

const formData = new FormData();
formData.append('holiday_date', holiday_date);
formData.append('holiday_name', holiday_name);
formData.append('holiday_day', holiday_day);

const response = await fetch(`/api/holiday_date/`, {
method: 'POST',
body: formData
})

const data = await response.json();
const resultDiv = document.getElementById('result')

if(data.status === "Success"){
    resultDiv.innerHTML = `<strong>${data.message}</strong>`
    document.getElementById("holiday_date_setup_form").reset();
    init_holiday_table()
}
else{
    resultDiv.innerHTML = `<strong>${data.message}</strong>`
}
})

/////////////////////////////////////////////////////////////////////////////////////////

document.getElementById('working_date_setup_form').addEventListener('submit', async function (e) {
e.preventDefault();

const working_date = document.getElementById('working_date').value;
const working_day_name   = document.getElementById('working_day_name').value;

working_day = new Date(working_date)
day = working_day.getDay()

switch (day) {
  case 0: working_day = "Sunday";    break;
  case 1: working_day = "Monday";    break;
  case 2: working_day = "Tuesday";   break;
  case 3: working_day = "Wednesday"; break;
  case 4: working_day = "Thursday";  break;
  case 5: working_day = "Friday";    break;
  case 6: working_day = "Saturday";  break;
}

const formData = new FormData();
formData.append('working_date', working_date);
formData.append('working_day_name', working_day_name);
formData.append('working_day', working_day);

const response = await fetch(`/api/working_date/`, {
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

/////////////////////////////////////////////////////////////////////////////////////////

document.getElementById('holiday_calendar_setup_form').addEventListener('submit', async function (e) {
e.preventDefault();

const holiday_calendar_start_date = document.getElementById('holiday_calendar_start_date').value;
const holiday_calendar_end_date   = document.getElementById('holiday_calendar_end_date').value;

const holiday_date_response = await fetch(`/api/holiday_date/`, {
method: 'GET'
})

const holiday_date_data = await holiday_date_response.json();
const holiday_list = holiday_date_data.data

const working_date_response = await fetch(`/api/working_date/`, {
method: 'GET'
})

const working_date_data = await working_date_response.json();
const working_day_list  = working_date_data.data

const formData = new FormData();
formData.append('holiday_calendar_start_date', holiday_calendar_start_date);
formData.append('holiday_calendar_end_date', holiday_calendar_end_date);

holiday_data = []
working_day_data = []

holiday_list.forEach(holiday => holiday_data.push(holiday.holiday_date))
working_day_list.forEach(working_day => working_day_data.push(working_day.working_date))

formData.append('holiday_data', JSON.stringify(holiday_data));
formData.append('working_day_data', JSON.stringify(working_day_data));

const response = await fetch(`/api/holiday_calendar_setup/`, {
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
