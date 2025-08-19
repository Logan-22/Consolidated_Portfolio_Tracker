import { create_notification } from './create_notification.js'

document.getElementById("create_metadata_tables").addEventListener("submit", async (e) => {
e.preventDefault()
const metadata_schema = document.getElementById("metadata_schema").value
const create_metadata_tables_response = await fetch(`/api/create_metadata_tables/?metadata_schema=${metadata_schema}`, {
method: 'GET'
})

const create_metadata_tables_data = await create_metadata_tables_response.json();

create_notification(create_metadata_tables_data.message, create_metadata_tables_data.status)
})

document.getElementById("create_utility_tables").addEventListener("submit", async (e) => {
e.preventDefault()
const utility_schema = document.getElementById("utility_schema").value
const create_utility_tables_response = await fetch(`/api/create_utility_tables/?utility_schema=${utility_schema}`, {
method: 'GET'
})

const create_utility_tables_data = await create_utility_tables_response.json();

create_notification(create_utility_tables_data.message, create_utility_tables_data.status)
})

document.getElementById("create_auth_tables").addEventListener("submit", async (e) => {
e.preventDefault()
const auth_schema = document.getElementById("auth_schema").value
const create_auth_tables_response = await fetch(`/api/create_auth_tables/?auth_schema=${auth_schema}`, {
method: 'GET'
})

const create_auth_tables_data = await create_auth_tables_response.json();

create_notification(create_auth_tables_data.message, create_auth_tables_data.status)
})

document.getElementById("migrate_data_to_aws").addEventListener("submit", async (e) => {
e.preventDefault()
const schema = document.getElementById("schema").value
const sqlite_table_name = document.getElementById("sqlite_table_name").value
const aws_table_name = document.getElementById("aws_table_name").value
const migrate_data_to_aws_response = await fetch(`/api/migrate_data_to_aws/?schema=${schema}&sqlite_table_name=${sqlite_table_name}&aws_table_name=${aws_table_name}`, {
method: 'GET'
})

const migrate_data_to_aws_data = await migrate_data_to_aws_response.json();

create_notification(migrate_data_to_aws_data.message, migrate_data_to_aws_data.status)
})

