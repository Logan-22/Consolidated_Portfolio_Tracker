import { create_notification } from './create_notification.js'

document.getElementById('user_register_form').addEventListener('submit', async (e) => {
    e.preventDefault()
    try {
        const first_name = document.getElementById('first_name').value
        const last_name  = document.getElementById('last_name').value
        const email_id   = document.getElementById('email_id').value
        const password   = document.getElementById('password').value

        const user_register_form_data = {
            'first_name': first_name
            ,'last_name': last_name
            ,'email_id' : email_id
            ,'password' : password
        }

        const formData = new FormData()

        formData.append('user_register_form_data', JSON.stringify(user_register_form_data))

        const user_regiser_response = await fetch('/auth/register/', {
            method: 'POST'
            , body: formData
        })

        const user_regiser_data = await user_regiser_response.json()

        create_notification(user_regiser_data.message, user_regiser_data.status)

        if (user_regiser_data.redirect_url) {
            window.location.href = user_regiser_data.redirect_url
        } else {
            create_notification('Invalid Redirect', 'Failed')
        }
    }
    catch (err) {
        console.error("Error while registering:", err)
    }
})