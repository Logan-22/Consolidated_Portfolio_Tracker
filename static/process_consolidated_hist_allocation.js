import { create_notification } from './create_notification.js'

// GET /api/process_consolidated_hist_allocation/
// Inserts data into CONSOLIDATED_HIST_ALLOCATION table

let chart;

document.addEventListener('DOMContentLoaded', () => {
  if (document.getElementById('allocation_chart')) {
    init_consolidated_hist_allocation_chart();
  }
});

async function init_consolidated_hist_allocation_chart(){

if (chart){
  chart.destroy()
}

const get_consolidated_hist_allocation_portfolio_response = await fetch (`/api/consolidated_hist_allocation_portfolio/`, {
  method: 'GET'
})

const get_consolidated_hist_allocation_portfolio_data = await get_consolidated_hist_allocation_portfolio_response.json();
const processing_date_array        = []
const mf_allocation_perc_array     = []
const equity_allocation_perc_array = []
const gold_allocation_perc_array   = []

let mf_allocation_present_flag     = 0
let equity_allocation_present_flag = 0
let gold_allocation_present_flag   = 0

if(get_consolidated_hist_allocation_portfolio_data.data){
get_consolidated_hist_allocation_portfolio_data.data.forEach(consolidated_hist_allocation_portfolio => {
processing_date_array.push(consolidated_hist_allocation_portfolio.processing_date)
})
}

if (processing_date_array.length != 0){
processing_date_array.forEach(proc_date => {
mf_allocation_present_flag     = 0
equity_allocation_present_flag = 0
gold_allocation_present_flag   = 0
get_consolidated_hist_allocation_portfolio_data.data.forEach(consolidated_hist_allocation_portfolio => {
if(proc_date == consolidated_hist_allocation_portfolio.processing_date){
  if(consolidated_hist_allocation_portfolio.portfolio_type == 'Mutual Fund'){
    mf_allocation_perc_array.push(consolidated_hist_allocation_portfolio.fin_alloc_perc_portfolio_invested_amount)
    mf_allocation_present_flag = 1
  }
  if(consolidated_hist_allocation_portfolio.portfolio_type == 'Equity'){
    equity_allocation_perc_array.push(consolidated_hist_allocation_portfolio.fin_alloc_perc_portfolio_invested_amount)
    equity_allocation_present_flag = 1
  }
  if(consolidated_hist_allocation_portfolio.portfolio_type == 'Gold'){
    gold_allocation_perc_array.push(consolidated_hist_allocation_portfolio.fin_alloc_perc_portfolio_invested_amount)
    gold_allocation_present_flag = 1
  }
}
})
// Insert 0 into the allocation array if there is no records for that type for that processing date
if (mf_allocation_present_flag == 0){
  mf_allocation_perc_array.push(0)
}
if (equity_allocation_present_flag == 0){
  equity_allocation_perc_array.push(0)
}
if (gold_allocation_present_flag == 0){
  gold_allocation_perc_array.push(0)
}
})
}

if(get_consolidated_hist_allocation_portfolio_data.data)
{
const ctx = document.getElementById('allocation_chart').getContext('2d');

chart = new Chart(ctx, {type: 'line',
        data: {labels: processing_date_array,  // Dates on the X-axis
        datasets: [
          {
            label: 'Mutal Fund Allocation Percentage',
            data: mf_allocation_perc_array,
            borderColor: 'rgba(75, 192, 192, 1)',
            //backgroundColor: 'rgba(75, 192, 192, 0.2)',
            //fill: true,
            tension: 0.4
          },
          {
            label: 'Equity Allocation Percentage',
            data: equity_allocation_perc_array,
            borderColor: 'rgba(12, 176, 6, 0.82)',
            // backgroundColor: 'rgba(75, 192, 192, 0.2)',
            // fill: true,
            tension: 0.4
          },
          {
            label: 'Gold Allocation Percentage',
            data: gold_allocation_perc_array,
            borderColor: 'rgba(255, 99, 132, 1)',
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
              text: 'Consolidated Allocation Over Time'
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
                text: 'Allocation Percentage'
              },
              beginAtZero: false
            }
          }
        }
      });
}

create_notification(get_consolidated_hist_allocation_portfolio_data.message, get_consolidated_hist_allocation_portfolio_data.status)
}

/////////////////////////////////////////////////////////////////////////////////////////

document.getElementById('process_consolidated_hist_allocation_form').addEventListener('submit', async function (e) {
e.preventDefault();

const process_consolidated_hist_allocation_response = await fetch(`/api/process_consolidated_hist_allocation/`, {
method: 'GET'
})

const process_consolidated_hist_allocation_data = await process_consolidated_hist_allocation_response.json();

if(process_consolidated_hist_allocation_data.status === "Success"){
    init_consolidated_hist_allocation_chart()
}

create_notification(process_consolidated_hist_allocation_data.message, process_consolidated_hist_allocation_data.status)
})