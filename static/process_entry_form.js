import { create_notification } from './create_notification.js'

// Process Entry into Process Group, Process and Keycolumn Tables

document.addEventListener('DOMContentLoaded', () => {
  if (document.getElementById('process_entry_form_container')) {
    initialize_process_entry_form();
  }
});

let process_count = 0
let table_and_view_data = {}
const view_list = []
const table_list = []
const frequency_list = ['Ad hoc','On Start']
const default_start_date_typ_cd_list = ['ALL','MUTUAL_FUND','STOCK']

async function initialize_process_entry_form(){
const table_and_view_info_response = await fetch('/api/table_and_view_info/',{
method: 'GET'
})

table_and_view_data = await table_and_view_info_response.json()

add_process_entry_block()
}

document.getElementById("add_process_button").addEventListener("click" , add_process_entry_block)

async function add_process_entry_block(e){
  if (process_count != 0) {
  e.preventDefault()
  }
  const process_entry_form_container = document.getElementById("process_entry_form_container")
  const process_entry_block_div = document.createElement("div")
  process_entry_block_div.className = 'process-entry'

  view_list.push("None")
  for (let view in table_and_view_data.view_info){
    view_list.push(view)
  }

  for (let table in table_and_view_data.table_info){
    table_list.push(table)
  }

  process_entry_block_div.innerHTML = `
  <div class="field-grid">
    ${create_input_element("Process Group", "text", "process_group")}
    ${create_input_element("Process Name", "text", "process_name")}
    ${create_input_element("Process Type", "select", "process_type", ["SCD1", "SCD2", "SCD3"])}
    ${create_input_element("Process Type Codes", "text", "process_type_codes")}
    ${create_input_element("Input View", "select", "process_input_view", view_list)}
    ${create_input_element("Target Table", "select", "process_target_table", table_list)}
    ${create_input_element("Process Description", "text", "process_description")}
    ${create_input_element("Process Frequency", "select", "process_frequency", frequency_list)}
    ${create_input_element("Default Start Date Type Code", "select", "process_default_start_date_type_code", ["NONE","ALL", "MUTUAL_FUND", "STOCK"])}
    ${create_input_element("Execution Order", "number", "execution_order")}
  </div>
  
  <div class="checkbox-row">
    ${create_input_element("Auto Trigger On Launch?", "checkbox", "process_auto_trigger_on_launch")}
    ${create_input_element("Process Decommissioned?", "checkbox", "process_decommissioned")}
    ${create_input_element("Consider for Processing?", "checkbox", "consider_for_processing")}
  </div>
  <div class="key-columns">
  <label for ="key-column-list-${process_count}">Key Columns</label>
  <select id = "key-column-list-${process_count}" name = "key-column-list" multiple size = 6>
  </select>
  <div class = "selected-values" id = "selected-values-${process_count}">
  </div>
  `

process_entry_form_container.appendChild(process_entry_block_div);

const select_input_element = document.querySelector(`#key-column-list-${process_count}`)
const select_input_display = document.querySelector(`#selected-values-${process_count}`)

select_input_element.addEventListener('change', ()=>{
  const selected_options = Array.from(select_input_element.selectedOptions).map(opt => opt.value)
  select_input_display.innerHTML = selected_options.map(opt => `<span>${opt}</span>`).join('')
})

const target_table_field = document.querySelector(`#process_target_table-${process_count}`)
target_table_field.addEventListener('change', (e) => {
  let process_count_of_the_selected_field = e.target.id.split("-")[1]
  get_column_list_options(process_count_of_the_selected_field)
})

get_column_list_options(process_count)

process_count += 1
}

function create_input_element(input_label, input_type, input_name, input_options = []){
  if(input_type == 'select'){
    return `
    <div class="field">
      <label for = "${input_name}-${process_count}">${input_label}</label>
      <select name = "${input_name}" id = "${input_name}-${process_count}">
        ${input_options.map(opt => `<option value = "${opt}">${opt}</option>`).join('')}
      </select>
    </div>
    `
  }
  else if (input_type == 'checkbox'){
    return `
    <div class="field">
    <label for = "${input_name}-${process_count}">${input_label}</label>
    <input type = "${input_type}" name = "${input_name}" id = "${input_name}-${process_count}">
    </div>
    `
  }
  else{
    return `
    <div class="field">
      <label for = "${input_name}-${process_count}">${input_label}</label>
      <input type = "${input_type}" name = "${input_name}" id = "${input_name}-${process_count}" placeholder="Enter the ${input_label}" autocomplete="off" ${input_name != "process_type_codes" ? "required" : ""}>
    </div>
    `
  }
}

function get_column_list_options(process_count_of_the_selected_field){
const key_column_select_list = document.getElementById(`key-column-list-${process_count_of_the_selected_field}`)
const selected_values_list = document.getElementById(`selected-values-${process_count_of_the_selected_field}`)
selected_values_list.innerHTML = ""
const process_target_table_name = document.getElementById(`process_target_table-${process_count_of_the_selected_field}`)
let column_option_list = ""
  for (let table_name in table_and_view_data.table_info){
    if(table_name == process_target_table_name.value){
      column_option_list = table_and_view_data.table_info[table_name].map(column => `<option value = "${column.name}">${column.name}</option>`).join(" ")
      }
    }
key_column_select_list.innerHTML = column_option_list
}

document.getElementById("process_entry_form").addEventListener("submit", async (e) => {
e.preventDefault()
const process_entries = document.querySelectorAll('.process-entry')
const process_entry_payloads = Array.from(process_entries).map(entry => {
const key_columns = Array.from(entry.querySelector('[name="key-column-list"]').selectedOptions).map(opt => opt.value)
return {
  PROCESS_GROUP              : entry.querySelector('[name="process_group"]').value.trim(),
  OUT_PROCESS_NAME           : entry.querySelector('[name="process_name"]').value.trim(),
  PROCESS_TYPE               : entry.querySelector('[name="process_type"]').value.trim(),
  PROC_TYP_CD_LIST           : entry.querySelector('[name="process_type_codes"]').value.trim(),
  INPUT_VIEW                 : entry.querySelector('[name="process_input_view"]').value.trim() != "None" ? entry.querySelector('[name="process_input_view"]').value.trim() : "",
  TARGET_TABLE               : entry.querySelector('[name="process_target_table"]').value.trim(),
  PROCESS_DESCRIPTION        : entry.querySelector('[name="process_description"]').value.trim(),
  AUTO_TRIGGER_ON_LAUNCH     : entry.querySelector('[name="process_auto_trigger_on_launch"]').checked ? 1 : 0,
  PROCESS_DECOMMISSIONED     : entry.querySelector('[name="process_decommissioned"]').checked ? 1 : 0,
  CONSIDER_FOR_PROCESSING    : entry.querySelector('[name="consider_for_processing"]').checked ? 1 : 0,
  FREQUENCY                  : entry.querySelector('[name="process_frequency"]').value.trim(),
  DEFAULT_START_DATE_TYPE_CD : entry.querySelector('[name="process_default_start_date_type_code"]').value.trim(),
  EXECUTION_ORDER            : entry.querySelector('[name="execution_order"]').value,
  PROCESS_KEYCOLUMNS         : key_columns
}
})

const formData = new FormData();
formData.append('process_entry_values', JSON.stringify(process_entry_payloads));

const process_entry_response = await fetch(`/api/process_entry/`, {
method: 'POST',
body: formData
})

const process_entry_data = await process_entry_response.json();

create_notification(process_entry_data.message, process_entry_data.status)

})