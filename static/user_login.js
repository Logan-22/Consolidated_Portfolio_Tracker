import { create_notification } from './create_notification.js'

document.addEventListener('DOMContentLoaded', async () => {
if (document.getElementById('welcome_text')) {
    init_test()
}
});

async function init_test(){
const test_response = await fetch("/auth/test/", {
method: 'GET'
})

const test_data = await test_response.json()

const welcome_text = document.getElementById("welcome_text")
welcome_text.innerHTML = `Hello ${test_data.message}`

create_notification(test_data.message, test_data.status)
}