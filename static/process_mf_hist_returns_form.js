import { create_notification } from './create_notification.js'

// GET /api/process_hist_returns/
// Inserts data into MF_HIST_RETURNS table

let chart;

document.addEventListener('DOMContentLoaded', () => {
  if (document.getElementById('navChart')) {
    init_mf_hist_returns_chart();
  }
});

async function init_mf_hist_returns_chart(){

if(chart){
  chart.destroy();
}

const get_mf_hist_returns_response = await fetch (`/api/mf_hist_returns/`, {
  method: 'GET'
})

const get_mf_hist_returns_data = await get_mf_hist_returns_response.json();

const processing_date_array = []
const perc_total_p_l_array = []
const perc_day_p_l_array = []

get_mf_hist_returns_data.data.forEach(mf_hist_return => {
  processing_date_array.push(mf_hist_return.processing_date)
  perc_total_p_l_array.push(mf_hist_return.perc_total_p_l)
  perc_day_p_l_array.push(mf_hist_return.perc_day_p_l)
});

const ctx = document.getElementById('navChart').getContext('2d');

chart = new Chart(ctx, {type: 'line',
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


create_notification(get_mf_hist_returns_data.message, get_mf_hist_returns_data.status)
}


/////////////////////////////////////////////////////////////////////////////////////////

document.getElementById('process_mf_hist_returns_form').addEventListener('submit', async function (e) {
e.preventDefault();

const mf_hist_returns_response = await fetch(`/api/process_mf_hist_returns/`, {
method: 'GET'
})

const mf_hist_returns_data = await mf_hist_returns_response.json();

create_notification(mf_hist_returns_data.message, mf_hist_returns_data.status)

if(mf_hist_returns_data.status === "Success"){
    init_mf_hist_returns_chart()
}
})