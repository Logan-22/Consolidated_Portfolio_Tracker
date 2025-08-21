import { create_notification } from './create_notification.js'

function handleGoogleResponse(response) {
  const data = parseJwt(response.credential);
  alert(`Google Login Successful! Welcome ${data.name} 😎`);
}

window.handleGoogleResponse = handleGoogleResponse