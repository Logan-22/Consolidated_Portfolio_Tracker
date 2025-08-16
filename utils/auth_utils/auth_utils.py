from flask import request
from flask_limiter.util import get_remote_address
from hmac import new
from hashlib import sha256
from os import getenv
from base64 import urlsafe_b64encode
from secrets import token_bytes

def login_limit_based_on_email_or_ip():
    'To limit login attempts on the basis of email or IP'
    try:
        email = (request.form.get('email_id') or "").strip().lower()
    except Exception:
        email = ""
    if email:
        return {'EMAIL_ID': email, 'DEVICE_IP': None}
    else:
        device_ip = get_remote_address()
    return {'EMAIL_ID': None, 'DEVICE_IP': device_ip}
    
def hash_input_token(input_token):
    APP_SECRET = getenv("APP_SECRET")
    hashed_token = new(APP_SECRET.encode(), input_token.encode(), sha256).hexdigest()
    return hashed_token

def generate_session_id():
    rand = token_bytes(32)
    session_id = urlsafe_b64encode(rand).decode()
    return session_id

def generate_csrf_token():
    csrf_token = urlsafe_b64encode(token_bytes(16)).decode()
    return csrf_token