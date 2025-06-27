// GET /api/process_unrealised_stock_hist_returns/
// Inserts data into UNREALISED_STOCK_HIST_RETURNS table

document.addEventListener('DOMContentLoaded', () => {
  if (document.getElementById('navChart')) {
    init_returns_chart();
  }
});

let chart;

async function init_returns_chart(){

if (chart){
  chart.destroy()
}

const unrealised_stock_hist_returns_response = await fetch ('/api/unrealised_stock_hist_returns/', {
  method: 'GET'
})

const unrealised_stock_hist_returns_data = await unrealised_stock_hist_returns_response.json();
const resultDiv = document.getElementById('result')

const processing_date_array = []
const perc_net_p_l_array = []
const perc_day_p_l_array = []

unrealised_stock_hist_returns_data.data.forEach(hist_return => {
  processing_date_array.push(hist_return.processing_date)
  perc_net_p_l_array.push(hist_return.perc_net_p_l)
  perc_day_p_l_array.push(hist_return.perc_day_p_l)
});

const ctx = document.getElementById('navChart').getContext('2d');

chart = new Chart(ctx, {type: 'line',
        data: {labels: processing_date_array,  // Dates on the X-axis
        datasets: [
          {
            label: 'Total Profit And Loss Percentage',
            data: perc_net_p_l_array,  // %_total_p/l values on the Y-axis
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


if(unrealised_stock_hist_returns_data.status === "Success"){
    resultDiv.innerHTML = `<strong>${unrealised_stock_hist_returns_data.message}</strong>`
}
else{
    resultDiv.innerHTML = `<strong>${unrealised_stock_hist_returns_data.message}</strong>`
}
}


/////////////////////////////////////////////////////////////////////////////////////////

document.getElementById('process_unrealised_stock_hist_returns_form').addEventListener('submit', async function (e) {
e.preventDefault();

const unrealised_returns_process_response = await fetch(`/api/process_unrealised_stock_hist_returns/`, {
method: 'GET'
})

const unrealised_returns_process_data = await unrealised_returns_process_response.json();
const resultDiv = document.getElementById('result')

if(unrealised_returns_process_data.status === "Success"){
    resultDiv.innerHTML = `<strong>${unrealised_returns_process_data.message}</strong>`
    init_returns_chart()
}
else{
    resultDiv.innerHTML = `<strong>${unrealised_returns_process_data.message}</strong>`
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
