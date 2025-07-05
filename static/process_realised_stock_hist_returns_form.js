import { create_notification } from './create_notification.js'

// GET /api/process_realised_intraday_stock_hist_returns/
// Inserts data into REALISED_INTRADAY_STOCK_HIST_RETURNS table

let chart;

document.addEventListener('DOMContentLoaded', () => {
  if (document.getElementById('navChart')) {
    init_realised_intraday_and_swing_stock_hist_returns_chart();
  }
});

async function init_realised_intraday_and_swing_stock_hist_returns_chart(){

if (chart){
  chart.destroy()
}

const get_realised_intraday_and_swing_hist_returns_response = await fetch (`/api/realised_intraday_and_swing_stock_hist_returns/`, {
  method: 'GET'
})

const get_realised_intraday_and_swing_hist_returns_data = await get_realised_intraday_and_swing_hist_returns_response.json();

const trade_date_array = []
const perc_p_l_with_leverage_array = []

get_realised_intraday_and_swing_hist_returns_data.data.forEach(realised_stock_hist_return => {
  trade_date_array.push(realised_stock_hist_return.trade_date)
  perc_p_l_with_leverage_array.push(realised_stock_hist_return.perc_p_l_with_leverage)
});

const ctx = document.getElementById('navChart').getContext('2d');

chart = new Chart(ctx, {
  type: 'bar',
  data: {
    labels: trade_date_array,
    datasets: [
      {
        label: 'Total Profit And Loss %',
        data: perc_p_l_with_leverage_array,
        backgroundColor: perc_p_l_with_leverage_array.map(value =>
          value >= 0 ? 'rgba(0, 200, 0, 0.7)' : 'rgba(200, 0, 0, 0.7)'
        ),
        borderColor: perc_p_l_with_leverage_array.map(value =>
          value >= 0 ? 'rgba(0, 150, 0, 1)' : 'rgba(150, 0, 0, 1)'
        ),
        borderWidth: 1
      }
    ]
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
      },
      legend: {
        position: 'top'
      }
    },
    scales: {
      x: {
        title: {
          display: true,
          text: 'Trade Date'
        },
        ticks: {
          autoSkip: true,
          maxRotation: 45,
          minRotation: 0
        }
      },
      y: {
        title: {
          display: true,
          text: 'Profit And Loss Percentage'
        },
        beginAtZero: false
      }
    }
  }
})

create_notification(get_realised_intraday_and_swing_hist_returns_data.message, get_realised_intraday_and_swing_hist_returns_data.status)
}

/////////////////////////////////////////////////////////////////////////////////////////

document.getElementById('process_realised_stock_hist_returns_form').addEventListener('submit', async function (e) {
e.preventDefault();

const process_realised_intraday_hist_returns_response = await fetch(`/api/process_realised_intraday_stock_hist_returns/`, {
method: 'GET'
})

const process_realised_intraday_hist_returns_data = await process_realised_intraday_hist_returns_response.json();

create_notification(process_realised_intraday_hist_returns_data.message, process_realised_intraday_hist_returns_data.status)

const process_realised_swing_hist_returns_response = await fetch(`/api/process_realised_swing_stock_hist_returns/`, {
method: 'GET'
})

const process_realised_swing_hist_returns_data = await process_realised_swing_hist_returns_response.json();

create_notification(process_realised_swing_hist_returns_data.message, process_realised_swing_hist_returns_data.status)

if(process_realised_intraday_hist_returns_data.status === "Success" && process_realised_swing_hist_returns_data.status === "Success"){
  init_realised_intraday_and_swing_stock_hist_returns_chart()
}
})