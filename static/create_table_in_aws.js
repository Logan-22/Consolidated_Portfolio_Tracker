import { create_notification } from './create_notification.js'

document.getElementById("create_table_in_aws").addEventListener("submit", async (e) => {
e.preventDefault()
const schema_id = document.getElementById("schema_id").value
const create_table_in_aws_response = await fetch(`/api/create_metadata_tables_in_aws/${schema_id}/`, {
method: 'GET'
})

const create_table_in_aws_data = await create_table_in_aws_response.json();

create_notification(create_table_in_aws_data.message, create_table_in_aws_data.status)

})