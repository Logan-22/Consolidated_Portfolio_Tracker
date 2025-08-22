from flask import request, jsonify, make_response, current_app
from flask_limiter.util import get_remote_address
from hmac import new
from hashlib import sha256
from os import getenv
from base64 import urlsafe_b64encode
from secrets import token_bytes
from functools import wraps

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

def generate_user_id(email_id):
    email_id = email_id.lower().strip()
    hashed_email = sha256(email_id.encode()).hexdigest()
    numeric_value_of_hash = int(hashed_email, 16)
    user_id = numeric_value_of_hash % (10 ** 15) # To return 15 digit number
    return user_id

def require_csrf_token(function):
    @wraps(function)
    def decorated(*args, **kwargs):
        SESSION_COOKIE_NAME = current_app.config['SESSION_COOKIE_NAME']
        redirect_url        = current_app.config['REDIRECT_URL']
        header = request.headers.get("X-XSRF-TOKEN")
        cookie = request.cookies.get("XSRF-TOKEN")
        if not header or not cookie or header != cookie:
            response = make_response("", 302)
            response.headers["Location"] = f"{redirect_url}/login"
            response.delete_cookie(SESSION_COOKIE_NAME)
            response.delete_cookie('XSRF-TOKEN')
            return response
        return function(*args, **kwargs)
    return decorated
