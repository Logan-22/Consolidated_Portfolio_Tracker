import { create_notification } from './create_notification.js'

// GET /api/simulate_returns/
// Inserts data into SIMULATE_RETUNS Table

let simulated_returns_chart;

document.addEventListener('DOMContentLoaded', () => {
  if (document.getElementById('simulated_returns_chart')) {
    init_simulated_returns_chart();
  }
});

async function init_simulated_returns_chart(){

if(simulated_returns_chart){
  simulated_returns_chart.destroy();
}

const get_simulated_returns_response = await fetch (`/api/simulated_returns/`, {
  method: 'GET'
})

const simulated_returns_data = await get_simulated_returns_response.json();

const processing_date_array = []
const perc_total_p_l_array = []
const perc_day_p_l_array = []
const sim_perc_total_p_l_array = []
const sim_perc_day_p_l_array = []

simulated_returns_data.data.forEach(simulated_return => {
  processing_date_array.push(simulated_return['PROCESSING_DATE'])
  perc_total_p_l_array.push(simulated_return['%_FIN_TOTAL_P/L'])
  perc_day_p_l_array.push(simulated_return['%_FIN_DAY_P/L'])
  sim_perc_total_p_l_array.push(simulated_return['FIN_%_SIM_P/L'])
  sim_perc_day_p_l_array.push(simulated_return['FIN_%_SIM_DAY_P/L'])
});

const ctx = document.getElementById('simulated_returns_chart').getContext('2d');

simulated_returns_chart = new Chart(ctx, {type: 'line',
        data: {labels: processing_date_array,  // Dates on the X-axis
        datasets: [
          {
            label: 'Consolidated Profit or Loss Percentage',
            data: perc_total_p_l_array,  // %_total_p/l values on the Y-axis
            borderColor: 'rgba(75, 192, 192, 1)',
            tension: 0.4
          },
          {
            label: 'Consolidated Day Profit or Loss Percentage',
            data: perc_day_p_l_array,  // %_day_p/l values on the Y-axis
            borderColor: 'rgba(12, 176, 6, 0.82)',
            tension: 0.4
          },          {
            label: 'Simulated Profit or Loss Percentage',
            data: sim_perc_total_p_l_array,  // %_total_p/l values on the Y-axis
            borderColor: 'rgba(255, 193, 7, 0.8)',
            tension: 0.4
          },
          {
            label: 'Simulated Day Profit or Loss Percentage',
            data: sim_perc_day_p_l_array,  // %_day_p/l values on the Y-axis
            borderColor: 'rgba(255, 87, 34, 0.9)',
            tension: 0.4
          }]
        },
        options: {
          responsive: true,
          plugins: {
            title: {
              display: true,
              text: 'Simulated Returns'
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
                text: 'Simulated Returns Percentage'
              },
              beginAtZero: false
            }
          }
        }
      });


create_notification(simulated_returns_data.message, simulated_returns_data.status)
}

/////////////////////////////////////////////////////////////////////////////////////////

document.getElementById('simulate_returns_form').addEventListener('submit', async function (e) {
e.preventDefault();

const process_simulate_returns_response = await fetch(`/api/process_simulate_returns/`, {
method: 'GET'
})

const process_simulate_returns_data = await process_simulate_returns_response.json();

create_notification(process_simulate_returns_data.message, process_simulate_returns_data.status)

if(process_simulate_returns_data.status === "Success"){
    init_simulated_returns_chart()
}
})