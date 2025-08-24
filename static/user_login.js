import { create_notification } from './create_notification.js'

document.getElementById('user_login_form').addEventListener('submit', async (e) => {
e.preventDefault()
try {
const email_id = document.getElementById('email_id').value
const password = document.getElementById('password').value

const formData = new FormData()

formData.append('email_id', email_id)
formData.append('password', password)

const user_login_response = await fetch ('/auth/login/', {
method : 'POST'
,body  : formData
})

const user_login_data = await user_login_response.json()

create_notification(user_login_data.message, user_login_data.status)
if(user_login_data.redirect_url){
    window.location.href = user_login_data.redirect_url
}
else{
create_notification('Invalid Redirect during Login', 'Failed')
}
}
catch(err){
    console.error("Error while logging in:", err)
}
})