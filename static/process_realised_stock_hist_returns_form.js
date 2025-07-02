// GET /api/process_realised_intraday_stock_hist_returns/
// Inserts data into REALISED_INTRADAY_STOCK_HIST_RETURNS table

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

const response = await fetch (`/api/realised_intraday_stock_hist_returns/`, {
  method: 'GET'
})

const data = await response.json();
const resultDiv = document.getElementById('result')

const trade_date_array = []
const perc_p_l_with_leverage_array = []

data.data.forEach(realised_stock_hist_return => {
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

if(data.status === "Success"){
    resultDiv.innerHTML = `<strong>${data.message}</strong>`
}
else{
    resultDiv.innerHTML = `<strong>${data.message}</strong>`
}
}

/////////////////////////////////////////////////////////////////////////////////////////

document.getElementById('process_realised_stock_hist_returns_form').addEventListener('submit', async function (e) {
e.preventDefault();

const response = await fetch(`/api/process_realised_intraday_stock_hist_returns/`, {
method: 'GET'
})


const data = await response.json();
const resultDiv = document.getElementById('result')

if(data.status === "Success"){
    resultDiv.innerHTML = `<strong>${data.message}</strong>`
}
else{
    resultDiv.innerHTML = `<strong>${data.message}</strong>`
}

const swing_response = await fetch(`/api/process_realised_swing_stock_hist_returns/`, {
method: 'GET'
})

const swing_data = await swing_response.json();

if(swing_data.status === "Success"){
    resultDiv.innerHTML += `<strong>${data.message}</strong>`
}
else{
    resultDiv.innerHTML = `<strong>${data.message}</strong>`
}

if(swing_data.status === "Success" && data.status === "Success"){
  init_returns_chart()
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
