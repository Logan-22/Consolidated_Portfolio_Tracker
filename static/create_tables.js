import { create_notification } from './create_notification.js'

document.getElementById("create_metadata_tables").addEventListener("submit", async (e) => {
e.preventDefault()
const metadata_schema = document.getElementById("metadata_schema").value
const create_metadata_tables_response = await fetch(`/api/create_metadata_tables?metadata_schema=${metadata_schema}`, {
method: 'GET'
})

const create_metadata_tables_data = await create_metadata_tables_response.json();

create_notification(create_metadata_tables_data.message, create_metadata_tables_data.status)
})

document.getElementById("create_utility_tables").addEventListener("submit", async (e) => {
e.preventDefault()
const utility_schema = document.getElementById("utility_schema").value
const create_utility_tables_response = await fetch(`/api/create_utility_tables?utility_schema=${utility_schema}`, {
method: 'GET'
})

const create_utility_tables_data = await create_utility_tables_response.json();

create_notification(create_utility_tables_data.message, create_utility_tables_data.status)
})

document.getElementById("create_auth_tables").addEventListener("submit", async (e) => {
e.preventDefault()
const auth_schema = document.getElementById("auth_schema").value
const create_auth_tables_response = await fetch(`/api/create_auth_tables?auth_schema=${auth_schema}`, {
method: 'GET'
})

const create_auth_tables_data = await create_auth_tables_response.json();

create_notification(create_auth_tables_data.message, create_auth_tables_data.status)
})

document.getElementById("create_user_transaction_tables").addEventListener("submit", async (e) => {
e.preventDefault()
const txn_schema = document.getElementById("txn_schema").value
const create_txn_tables_response = await fetch(`/api/create_txn_tables?txn_schema=${txn_schema}`, {
method: 'GET'
})

const create_txn_tables_data = await create_txn_tables_response.json();

create_notification(create_txn_tables_data.message, create_txn_tables_data.status)
})

document.getElementById("create_tier0_metrics_tables").addEventListener("submit", async (e) => {
e.preventDefault()
const tier0_metrics_schema = document.getElementById("tier0_metrics_schema").value
const create_tier0_metrics_tables_response = await fetch(`/api/create_tier0_metrics_tables?tier0_metrics_schema=${tier0_metrics_schema}`, {
method: 'GET'
})

const create_tier0_metrics_tables_data = await create_tier0_metrics_tables_response.json();

create_notification(create_tier0_metrics_tables_data.message, create_tier0_metrics_tables_data.status)
})

document.getElementById("create_tier0_inp_view").addEventListener("submit", async (e) => {
e.preventDefault()
const tier0_inp_view_schema = document.getElementById("tier0_inp_view_schema").value
const create_tier0_input_view_response = await fetch(`/api/create_tier0_inp_view?tier0_inp_view_schema=${tier0_inp_view_schema}`, {
method: 'GET'
})

const create_tier0_input_view_data = await create_tier0_input_view_response.json();

create_notification(create_tier0_input_view_data.message, create_tier0_input_view_data.status)
})

document.getElementById("create_tier1_inp_view").addEventListener("submit", async (e) => {
e.preventDefault()
const tier1_inp_view_schema = document.getElementById("tier1_inp_view_schema").value
const create_tier1_input_view_response = await fetch(`/api/create_tier1_inp_view?tier1_inp_view_schema=${tier1_inp_view_schema}`, {
method: 'GET'
})

const create_tier1_input_view_data = await create_tier1_input_view_response.json();

create_notification(create_tier1_input_view_data.message, create_tier1_input_view_data.status)
})

document.getElementById("migrate_data_to_aws").addEventListener("submit", async (e) => {
e.preventDefault()
const schema = document.getElementById("schema").value
const sqlite_table_name = document.getElementById("sqlite_table_name").value
const aws_table_name = document.getElementById("aws_table_name").value
const migrate_data_to_aws_response = await fetch(`/api/migrate_data_to_aws?schema=${schema}&sqlite_table_name=${sqlite_table_name}&aws_table_name=${aws_table_name}`, {
method: 'GET'
})

const migrate_data_to_aws_data = await migrate_data_to_aws_response.json();

create_notification(migrate_data_to_aws_data.message, migrate_data_to_aws_data.status)
})

