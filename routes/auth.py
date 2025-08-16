from flask import Blueprint, jsonify, request
from os import getenv
from routes.limiter import limiter
from argon2 import PasswordHasher
from utils.sql_utils.process.fetch_queries import fetch_queries_as_dictionaries
from utils.auth_utils.auth_utils import\
hash_input_token,\
generate_session_id,\
generate_csrf_token

auth = Blueprint('auth', __name__)

env = getenv('ENVIRONMENT')

ph = PasswordHasher(time_cost = 2, memory_cost = 102400, parallelism = 4)

@auth.route('/auth/register')
@limiter.limit('5 per minute')
def user_register():
    email_id = (request.form.get('email_id') or "").strip().lower()
    password = request.form.get('password') or ""
    if not email_id or not password:
        return jsonify({'message': 'Missing Email ID/Password', 'status': 'Failed'}), 400
    if len(password) < 12:
        return jsonify({'message': 'Weak Password', 'status': 'Failed'}), 400
    
    email_id_exists = fetch_queries_as_dictionaries(f"""
SELECT
    EMAIL_ID
FROM
    {env}T_AUTH
WHERE
    EMAIL_ID = '{email_id}';
    """, 'return_none')
    if email_id_exists:
        return jsonify({'message': 'Email Already Registered! Please Login'})
    
    password_hash = ph.hash(password)

@auth.route('/auth/test/')
def test_functions():
    try:
        variable = generate_session_id()
        return jsonify({'message': variable, 'status': 'Success'})
    except Exception as e:
        return jsonify({'message': repr(e), 'status': 'Failed'})