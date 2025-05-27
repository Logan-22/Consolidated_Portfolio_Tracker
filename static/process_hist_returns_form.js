// GET /api/process_hist_returns/
// Inserts data into MF_HIST_RETURNS table

document.addEventListener('DOMContentLoaded', () => {
  if (document.getElementById('navChart')) {
    init_returns_chart();
  }
});

async function init_returns_chart(){

const response = await fetch (`/api/mf_hist_returns/`, {
  method: 'GET'
})

const data = await response.json();
const resultDiv = document.getElementById('result')

const processing_date_array = []
const perc_total_p_l_array = []
const perc_day_p_l_array = []

data.data.forEach(mf_hist_return => {
  processing_date_array.push(mf_hist_return.processing_date)
  perc_total_p_l_array.push(mf_hist_return.perc_total_p_l)
  perc_day_p_l_array.push(mf_hist_return.perc_day_p_l)
});

const ctx = document.getElementById('navChart').getContext('2d');

new Chart(ctx, {type: 'line',
        data: {labels: processing_date_array,  // Dates on the X-axis
        datasets: [
          {
            label: 'Total Profit And Loss Percentage',
            data: perc_total_p_l_array,  // %_total_p/l values on the Y-axis
            borderColor: 'rgba(75, 192, 192, 1)',
            //backgroundColor: 'rgba(75, 192, 192, 0.2)',
            //fill: true,
            tension: 0.4
          },
          {
            label: 'Day Profit And Loss Percentage',
            data: perc_day_p_l_array,  // %_day_p/l values on the Y-axis
            borderColor: 'rgba(12, 176, 6, 0.82)',
            // backgroundColor: 'rgba(75, 192, 192, 0.2)',
            // fill: true,
            tension: 0.4
          }]
        },
        options: {
          responsive: true,
          plugins: {
            title: {
              display: true,
              text: 'Profit/Loss Percentage Over Time'
            },
            tooltip: {
              mode: 'index',
              intersect: false
            }
          },
          scales: {
            x: {
              type: 'category',
              title: {
                display: true,
                text: 'Processing Date'
              }
            },
            y: {
              title: {
                display: true,
                text: 'Total Profit And Loss Percentage'
              },
              beginAtZero: false
            }
          }
        }
      });


if(data.status === "Success"){
    resultDiv.innerHTML = `<strong>${data.message}</strong>`
}
else{
    resultDiv.innerHTML = `<strong>${data.message}</strong>`
}
}


/////////////////////////////////////////////////////////////////////////////////////////

document.getElementById('process_hist_returns_form').addEventListener('submit', async function (e) {
e.preventDefault();

const response = await fetch(`/api/process_hist_returns/`, {
method: 'GET'
})

const data = await response.json();
const resultDiv = document.getElementById('result')

if(data.status === "Success"){
    resultDiv.innerHTML = `<strong>${data.message}</strong>`
    init_returns_chart()
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
