from flask import Blueprint, jsonify, redirect, request, current_app, make_response, url_for
from google.oauth2 import id_token
from google.auth.transport import requests
from argon2 import PasswordHasher
from hmac import compare_digest
from secrets import token_urlsafe
from argon2.exceptions import VerifyMismatchError
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from utils.sql_utils.process.fetch_queries import fetch_queries_as_dictionaries
from utils.sql_utils.process.execute_process_group import execute_process_group_using_metadata
from utils.sql_utils.query_db.update_in_db import update_table_with_payload
from utils.auth_utils.auth_utils import\
generate_user_id,\
hash_input_token,\
generate_session_id,\
generate_csrf_token,\
login_limit_based_on_email_or_ip,\
require_csrf_token
from utils.log_utils.auth_audit_log import auth_audit_log_entry
from extensions import oauth, limiter

auth = Blueprint('auth', __name__)

ph = PasswordHasher(time_cost = 2, memory_cost = 102400, parallelism = 4)

@auth.route('/auth/register/', methods = ['POST'])
@limiter.limit('5 per minute')
def user_register():
    try:
        env = current_app.config['ENVIRONMENT']
        email_id = (request.form.get('email_id') or "").strip().lower()
        password = request.form.get('password') or ""
        first_name = request.form.get('first_name') or ""
        last_name = request.form.get('last_name') or ""
        phone_number = request.form.get('phone_number') or ""
        date_of_birth = request.form.get('date_of_birth') or ""
        if not email_id or not password:
            return jsonify({'message': 'Missing Email ID or Password', 'status': 'Failed'}), 400
        if len(password) < 12:
            return jsonify({'message': 'Weak Password', 'status': 'Failed'}), 400
        
        email_id_exists = fetch_queries_as_dictionaries(f"""
SELECT
    EMAIL_ID
FROM
    {env}T_AUTH.USERS
WHERE
    EMAIL_ID = '{email_id}'
    AND RECORD_DELETED_FLAG = 0;
        """, 'return_none', fetch = 'One')
        if email_id_exists:
            auth_audit_log_entry(None, None, 'Register', 'Failed', f'Provided Email ID {email_id} is already registered')
            return jsonify({'message': 'Given Email is Already Registered! Please Login', 'status': 'Failed'}), 409

        password_hash = ph.hash(password)
        user_id = generate_user_id(email_id)

        user_load_payload = {
            'USER_ID'           : user_id
            ,'EMAIL_ID'         : email_id
            ,'PASSWORD_HASH'    : password_hash
            ,'ROLE'             : 'User'
            ,'PERMISSIONS_JSON' : None
        }

        user_info_load_payload = {
            'USER_ID'              : user_id
            ,'FIRST_NAME'          : first_name
            ,'LAST_NAME'           : last_name
            ,'USER_NAME'           : first_name + ' ' + last_name
            ,'PHONE_NUMBER'        : phone_number
            ,'DATE_OF_BIRTH'       : date_of_birth
            ,'PROFILE_PICTURE_URL' : None
        }

        user_security_payload = {
            'USER_ID'                  : user_id
            ,'LAST_LOGIN_AT'           : None
            ,'CURRENT_LOGIN_AT'        : None
            ,'LOGIN_COUNT'             : 0
            ,'LAST_PASSWORD_CHANGE_AT' : datetime.now()
            ,'PASSWORD_EXPIRES_AT'     : datetime.now() + relativedelta(months = 3)
            ,'TWO_FACTOR_ENABLED'      : 0
            ,'TWO_FACTOR_SECRET'       : None
            ,'LAST_LOGIN_IP'           : None
            ,'LAST_LOGIN_DEVICE'       : None
            ,'CURRENT_LOGIN_IP'        : None
            ,'CURRENT_LOGIN_DEVICE'    : None
            ,'FAILED_LOGIN_AT'         : None
            ,'FAILED_LOGIN_COUNT'      : 0
            ,'EMAIL_VERIFIED'          : 0
            ,'LOCKED_UNTIL'            : None
        }

        user_register_final_payloads = {
            'PR_USERS_LOAD'          : user_load_payload
            ,'PR_USER_INFO_LOAD'     : user_info_load_payload
            ,'PR_USER_SECURITY_LOAD' : user_security_payload
        }

        user_registry_logs = execute_process_group_using_metadata('PG_USERS_REGISTER', payloads = user_register_final_payloads)
        if user_registry_logs['status'] == 'Success':
            auth_audit_log_entry(None, None, 'Register', 'Success', f'Registered with Email ID {email_id}')
            return jsonify({'message': 'Registration Completed Successfully', 'status': 'Success'}), 201
        else:
            auth_audit_log_entry(None, None, 'Register', 'Failed', f"Registration failed for {email_id} - Internal Error: {user_registry_logs['message']}")
            return jsonify({'message': 'Error while registering! Please try again after some time.', 'status': 'Failed'}), 500
    except Exception as e:
        return jsonify({'message': repr(e), 'status': "Failed"}), 500

@auth.route('/auth/login/', methods = ['POST'])
@limiter.limit("10 per 10 minutes", key_func = login_limit_based_on_email_or_ip)
def user_login():
    try:
        env = current_app.config['ENVIRONMENT']
        SESSION_COOKIE_NAME = current_app.config['SESSION_COOKIE_NAME']
        SESSION_COOKIE_AGE  = current_app.config['SESSION_COOKIE_AGE']
        email_id = (request.form.get('email_id') or "").strip().lower()
        password = request.form.get('password') or ""
        if not email_id or not password:
            return jsonify({'message': 'Missing Email ID or Password', 'status': 'Failed'}), 400
        
        user_data = fetch_queries_as_dictionaries(f"""
SELECT
    USER_ID
    ,EMAIL_ID
    ,PASSWORD_HASH
FROM
    {env}T_AUTH.USERS
WHERE
    EMAIL_ID = '{email_id}'
    AND RECORD_DELETED_FLAG = 0;
    """, 'return_none', fetch = 'One')
        if not user_data:
            auth_audit_log_entry(None, None, 'Login', 'Failed', f'Unknown Email {email_id} Login Attempt')
            return jsonify({'message': 'Invalid Credentials', 'status': 'Failed'}), 401

        user_security_data = fetch_queries_as_dictionaries(f"""
SELECT
    USER_ID
    ,FAILED_LOGIN_COUNT
    ,LOCKED_UNTIL
FROM
    {env}T_AUTH.USER_SECURITY
WHERE
    USER_ID = {user_data['USER_ID']}
    AND RECORD_DELETED_FLAG = 0;
    """, 'return_none', fetch = 'One')

        if user_security_data.get('LOCKED_UNTIL') and user_security_data['LOCKED_UNTIL'] > datetime.now():
            auth_audit_log_entry(user_data['USER_ID'], None, 'Login', 'Locked', f"User is locked until {user_security_data['LOCKED_UNTIL']}")
            return jsonify({'message': f"Due to mutliple unsuccessful login attempts, user is locked until {user_security_data['LOCKED_UNTIL']}", 'status': 'Failed'}), 403
        
        try:
            ph.verify(user_data['PASSWORD_HASH'], password)
            if ph.check_needs_rehash(user_data['PASSWORD_HASH']):
                new_hash_with_entered_password = ph.hash(password)
                update_user_payload = {
                    'data' : {
                        'PASSWORD_HASH' : new_hash_with_entered_password
                    },
                    'conditions' : {
                        'USER_ID': user_data['USER_ID']
                    }
                }
                update_table_with_payload(f"{env}T_AUTH", "USERS", update_user_payload)
            update_user_security_payload = {
                'data': {
                    'LAST_LOGIN_AT'         : {'raw': 'CURRENT_LOGIN_AT'}
                    ,'CURRENT_LOGIN_AT'     : datetime.now()
                    ,'LOGIN_COUNT'          : ('increment', 1)
                    ,'LAST_LOGIN_IP'        : {'raw': 'CURRENT_LOGIN_IP'}
                    ,'LAST_LOGIN_DEVICE'    : {'raw': 'CURRENT_LOGIN_DEVICE'}
                    ,'CURRENT_LOGIN_IP'     : request.remote_addr
                    ,'CURRENT_LOGIN_DEVICE' : request.headers.get("User-Agent")
                    ,'FAILED_LOGIN_COUNT'   : 0
                    ,'LOCKED_UNTIL'         : None
                }
                ,'conditions': {
                    'USER_ID' : user_data['USER_ID']
                }
            }
            update_table_with_payload(f"{env}T_AUTH", "USER_SECURITY", update_user_security_payload)
        except VerifyMismatchError:
            failed_login_count = (user_security_data.get('FAILED_LOGIN_COUNT') or 0) + 1
            locked_until = None
            if failed_login_count >= 5:
                locked_until = datetime.now() + timedelta(minutes = 15)
            update_user_security_fail_login_payload = {
                'data': {
                    'FAILED_LOGIN_AT'    : datetime.now()
                    ,'FAILED_LOGIN_COUNT' : failed_login_count
                    ,'LOCKED_UNTIL'      : locked_until
                }
                ,'conditions': {
                    'USER_ID' : user_data['USER_ID']
                }
            }
            update_table_with_payload(f"{env}T_AUTH", "USER_SECURITY", update_user_security_fail_login_payload)
            auth_audit_log_entry(user_data['USER_ID'], None, 'Login', 'Failed', 'Login Attempt using Bad Password')
            return jsonify({'message': 'Invalid Credentials', 'status': 'Failed'}), 401
        
        # Create Cookies
        session_id = generate_session_id()
        csrf_token = generate_csrf_token()
        session_expires_at = datetime.now() + timedelta(seconds = SESSION_COOKIE_AGE)
        
        user_sessions_payload = {
            'USER_ID'         : user_data['USER_ID']
            ,'SESSION_ID'     : session_id
            ,'CSRF_TOKEN'     : csrf_token
            ,'LOGIN_DEVICE'   : request.headers.get("User-Agent")
            ,'LOGIN_IP'       : request.remote_addr
            ,'ISSUED_AT'      : datetime.now()
            ,'LAST_ACTIVE_AT' : datetime.now()
            ,'EXPIRES_AT'     : session_expires_at
            ,'REVOKED'        : 0
        }

        user_sessions_entry_final_payload = {
            'PR_USER_SESSIONS_LOAD' : user_sessions_payload
        }

        user_sessions_logs = execute_process_group_using_metadata('PG_USERS_SESSIONS_ENTRY', payloads = user_sessions_entry_final_payload)
        if user_sessions_logs['status'] == "Success":
            response = make_response(jsonify({'message': 'Login Completed Successfully', 'status': 'Success'}))
            response.set_cookie(SESSION_COOKIE_NAME, session_id, httponly = True, secure = True, samesite = "Strict", max_age = SESSION_COOKIE_AGE)
            response.set_cookie('XSRF-TOKEN', csrf_token, httponly = False, secure = True, samesite = "Strict", max_age = SESSION_COOKIE_AGE)
            auth_audit_log_entry(user_data['USER_ID'], session_id, 'Login', 'Success', 'Logged in using Email ID and Password')
            return response, 200
        else:
            auth_audit_log_entry(user_data['USER_ID'], session_id, 'Login', 'Failed', f"Internal Error: {user_sessions_logs['message']}")
            return jsonify({'message': 'Internal Server Error', 'status': 'Failed'}), 500
    except Exception as e:
        return jsonify({'message': repr(e), 'status': "Failed"}), 500

@auth.route('/auth/logout/', methods = ['GET'])
@require_csrf_token
def user_logout():
    try:
        env = current_app.config['ENVIRONMENT']
        SESSION_COOKIE_NAME = current_app.config['SESSION_COOKIE_NAME']
        session_id = request.cookies.get(SESSION_COOKIE_NAME)
        if session_id:
            update_user_sessions_payload = {
                'data': {
                    'REVOKED' : 1
                },
                'conditions': {
                    'SESSION_ID': session_id
                }
            }
            update_table_with_payload(f"{env}T_AUTH", "USER_SESSIONS", update_user_sessions_payload)
        auth_audit_log_entry(None, session_id, 'Logout', 'Success', 'Successfully Logged out')
        response = make_response(jsonify({'message': 'Logout Completed Successfully', 'status': 'Success'}))
        response.delete_cookie(SESSION_COOKIE_NAME)
        response.delete_cookie('XSRF-TOKEN')
        return response, 200
    except Exception as e:
        return jsonify({'message': repr(e), 'status': "Failed"}), 500

@auth.route('/auth/me/', methods = ['GET'])
@require_csrf_token
def authenticate_user():
    try:
        env = current_app.config['ENVIRONMENT']
        SESSION_COOKIE_NAME = current_app.config['SESSION_COOKIE_NAME']
        SESSION_IDLE_TIMEOUT = current_app.config['SESSION_IDLE_TIMEOUT']
        session_id = request.cookies.get(SESSION_COOKIE_NAME)
        if not session_id:
            auth_audit_log_entry(None, session_id, 'Authentication', 'Failed', 'Invalid Session ID')
            return jsonify({'message': 'Unauthenticated. Please Login again', 'status': 'Failed'}), 401
        session_data = fetch_queries_as_dictionaries(f"""
SELECT
    USER_ID
    ,SESSION_ID
    ,EXPIRES_AT
    ,LAST_ACTIVE_AT
FROM
    {env}T_AUTH.USER_SESSIONS
WHERE
    SESSION_ID  = '{session_id}'
    AND REVOKED = 0;
    """, 'return_none', fetch = 'One')

        if not session_data:
            auth_audit_log_entry(None, session_id, 'Authentication', 'Failed', 'Invalid Session ID')
            return jsonify({'message': 'Unauthenticated. Please Login again', 'status': 'Failed'}), 401
        if session_data['EXPIRES_AT'] and session_data['EXPIRES_AT'] < datetime.now():
            auth_audit_log_entry(None, session_id, 'Authentication', 'Failed', 'Session ID Expired')
            return jsonify({'message': 'Unauthenticated. Please Login again', 'status': 'Failed'}), 401
        if session_data['LAST_ACTIVE_AT'] and (datetime.now() - session_data['LAST_ACTIVE_AT']).total_seconds() > SESSION_IDLE_TIMEOUT:
            auth_audit_log_entry(None, session_id, 'Authentication', 'Failed', 'Session ID Idle Status')
            return jsonify({'message': 'Unauthenticated. Please Login again', 'status': 'Failed'}), 401

        update_user_sessions_payload = {
            'data': {
                'LAST_ACTIVE_AT' : datetime.now()
            },
            'conditions': {
                'SESSION_ID': session_id
            }
        }
        update_table_with_payload(f"{env}T_AUTH", "USER_SESSIONS", update_user_sessions_payload)
        user_info_data = fetch_queries_as_dictionaries(f"""
SELECT
    USER_ID
    ,USER_NAME
FROM
    {env}T_AUTH.USER_INFO
WHERE
    USER_ID = {session_data['USER_ID']};
    """, 'return_none', fetch = 'One')

        if not user_info_data or not user_info_data['USER_ID']:
            auth_audit_log_entry(None, session_id, 'Authentication', 'Failed', 'Invalid Session ID')
            return jsonify({'message': 'Unauthenticated. Please Login again', 'status': 'Failed'}), 401
        auth_audit_log_entry(user_info_data['USER_ID'], session_id, 'Authentication', 'Success', 'User Authenticated')
        return jsonify({'message': f"Authenicated as {'User' if not user_info_data['USER_NAME'] else user_info_data['USER_NAME']}", 'status' : 'Success'})
    except Exception as e:
        return jsonify({'message': repr(e), 'status': "Failed"}), 500

@auth.route('/auth/password_reset/request/', methods = ['POST'])
@limiter.limit('5 per hour')
def password_reset_request():
    try:
        env = current_app.config['ENVIRONMENT']
        PASSWORD_RESET_EXP_HOURS = current_app.config['PASSWORD_RESET_EXP_HOURS']
        email_id = (request.form.get('email_id') or "").strip().lower()
        if not email_id:
            auth_audit_log_entry(None, None, 'Password Reset', 'Failed', f'Email ID {email_id} is Empty')
            return jsonify({'message': 'Password reset instructions will be sent if the email is registered.', 'status': 'Success'}), 200
        user_data = fetch_queries_as_dictionaries(f"""
SELECT
    USER_ID
FROM
    {env}T_AUTH.USERS
WHERE
    EMAIL_ID = '{email_id}';
    """, 'return_none', fetch = "One")
        if not user_data:
            auth_audit_log_entry(None, None, 'Password Reset', 'Failed', f'Email ID {email_id} is not registered')
            return jsonify({'message': 'Password reset instructions will be sent if the email is registered.', 'status': 'Success'}), 200
        
        password_reset_token = token_urlsafe(32)
        password_reset_token_hash = hash_input_token(password_reset_token)
        expires_at = datetime.now() + timedelta(hours = PASSWORD_RESET_EXP_HOURS)

        password_reset_payload = {
            'USER_ID'     : user_data['USER_ID']
            ,'TOKEN_HASH' : password_reset_token_hash
            ,'EXPIRES_AT' : expires_at
            ,'USED_AT'    : None
            ,'CREATED_AT' : datetime.now()
            ,'REVOKED'    : 0
        }

        password_reset_final_payload = {
            'PR_PASSWORD_RESETS_LOAD' : password_reset_payload
        }

        password_reset_entry_logs = execute_process_group_using_metadata('PG_PASSWORD_RESETS_ENTRY', payloads = password_reset_final_payload)
        if password_reset_entry_logs['status'] == 'Success':
            auth_audit_log_entry(user_data['USER_ID'], None, 'Password Reset', 'Success', f'Password Reset Instructions sent to {email_id}')
            reset_link = f"http://127.0.0.1:5000/password_reset/confirm?token={password_reset_token}&user_id={user_data['USER_ID']}"
            print(f"DEV Link -> {reset_link}")
            return jsonify({'message': 'Password reset instructions will be sent if the email is registered.', 'status': 'Success'}), 200
        else:
            auth_audit_log_entry(None, None, 'Password Reset', 'Failed', f"Failed to send Password Reset Instructions to {email_id} - Internal Error: {password_reset_entry_logs['message']}'")
            return jsonify({'message': 'Internal Server Error. Please try again after some time', 'status': 'Failed'}), 500
    except Exception as e:
        return jsonify({'message': repr(e), 'status': "Failed"}), 500

@auth.route('/auth/password_reset/confirm/', methods = ['POST'])
def password_reset_confirm():
    try:
        env = current_app.config['ENVIRONMENT']
        password_reset_token = request.form.get('token')
        user_id              = request.form.get('user_id')
        new_password         = request.form.get('new_password') or ""
        if not password_reset_token or not user_id or not new_password:
            auth_audit_log_entry(None, None, 'Password Reset Confirm', 'Failed', "Missing Token or User Id or New Password in Request")
            return jsonify({'message': 'Invalid or Expired Password Reset Request', 'status': 'Failed'}), 400
        if len(new_password) < 12:
            auth_audit_log_entry(None, None, 'Password Reset Confirm', 'Failed', "Weak New Password")
            return jsonify({'message': 'Weak New Password', 'status': 'Failed'}), 400
        
        password_reset_data = fetch_queries_as_dictionaries(f"""
SELECT
    USER_ID
    ,TOKEN_HASH
    ,EXPIRES_AT
FROM
    {env}T_AUTH.PASSWORD_RESETS
WHERE
    USED_AT IS NULL
    AND USER_ID = {user_id}
ORDER BY CREATED_AT DESC
LIMIT 1;
    """, 'return_none', fetch = 'One')
        if not password_reset_data or password_reset_data['EXPIRES_AT'] < datetime.now():
            auth_audit_log_entry(None, None, 'Password Reset Confirm', 'Failed', "Token has expired")
            return jsonify({'message': 'Invalid or Expired Password Reset Request', 'status': 'Failed'}), 400
        if not compare_digest(password_reset_data['TOKEN_HASH'], hash_input_token(password_reset_token)):
            auth_audit_log_entry(None, None, 'Password Reset Confirm', 'Failed', "Invalid Token")
            return jsonify({'message': 'Invalid or Expired Password Reset Request', 'status': 'Failed'}), 400

        update_password_resets_payload = {
            'data' : {
                'USED_AT' : datetime.now()
            }
            ,'conditions' : {
                'USER_ID' : user_id
            }
        }
        update_table_with_payload(f'{env}T_AUTH','PASSWORD_RESETS', update_password_resets_payload)

        new_password_hash = ph.hash(new_password)

        update_users_payload = {
            'data' : {
                'PASSWORD_HASH' : new_password_hash
            }
            ,'conditions' : {
                'USER_ID' : user_id
            }
        }
        update_table_with_payload(f'{env}T_AUTH', 'USERS', update_users_payload)

        update_user_sessions_payload = {
            'data' : {
                'REVOKED' : 1
            }
            ,'conditions' : {
                'USER_ID' : user_id
            }
        }
        update_table_with_payload(f'{env}T_AUTH', 'USER_SESSIONS', update_user_sessions_payload)

        update_user_security_payload = {
            'data' : {
                'LAST_PASSWORD_CHANGE_AT' : datetime.now()
                ,'PASSWORD_EXPIRES_AT'    : datetime.now() + relativedelta(months = 3)
            }
            ,'conditions' : {
                'USER_ID' : user_id
            }
        }
        update_table_with_payload(f'{env}T_AUTH', 'USER_SECURITY', update_user_security_payload)
        auth_audit_log_entry(user_id, None, 'Password Reset Confirm', 'Success', f'Password has been reset using Password Reset Token')
        return jsonify({'message': 'Password Reset completed Successfully. Please login using the new Password', 'status': 'Success'}), 200
    except Exception as e:
        return jsonify({'message': repr(e), 'status': "Failed"}), 500

@auth.route('/auth/login/google/', methods = ['GET'])
def login_using_google():
    redirect_uri = url_for('auth.auth_google_callback', _external = True)
    return oauth.google.authorize_redirect(redirect_uri)

@auth.route('/auth/google/callback', methods = ['POST'])
def auth_google_callback():
    try:
        env                 = current_app.config['ENVIRONMENT']
        SESSION_COOKIE_NAME = current_app.config['SESSION_COOKIE_NAME']
        SESSION_COOKIE_AGE  = current_app.config['SESSION_COOKIE_AGE']
        redirect_url        = current_app.config['REDIRECT_URL']
        token = request.form.get('credential')
        if not token:
            return redirect(f'{redirect_url}/login?error=invalid_oauth_token')

        user_info = id_token.verify_oauth2_token(
            token,
            requests.Request(),
            current_app.config['GOOGLE_CLIENT_ID']
        )
        email_id        = (user_info.get('email')          or "").strip().lower()
        email_verified  =  user_info.get('email_verified')
        user_name       = (user_info.get('name')           or "").strip().lower()
        profile_picture = (user_info.get('picture')        or "").strip().lower()
        first_name      = (user_info.get('given_name')     or "").strip().lower()
        last_name       = (user_info.get('family_name')    or "").strip().lower()
        if not email_id:
            auth_audit_log_entry(None, None, 'Login Using Google', 'Failed', 'Email not present')
            return redirect(f'{redirect_url}/login?error=gmail_not_found')
        auth_audit_log_entry(None, None, 'OAuth Email', 'Entry', f'Email:{email_id}')
        user_data = fetch_queries_as_dictionaries(f"""
SELECT
    USER_ID
FROM
    {env}T_AUTH.USERS
WHERE
    EMAIL_ID = '{email_id}'
    """, 'return_none', fetch = 'One')
        auth_audit_log_entry(None, None, 'OAuth User Payload', 'Entry', f'{str(user_data)}')
        user_id = ""
        if not user_data:
            user_id = generate_user_id(email_id)
            auth_audit_log_entry(None, None, 'OAuth User ID Generated', 'Entry', f'{user_id}')
            user_load_payload = {
                'USER_ID'           : user_id
                ,'EMAIL_ID'         : email_id
                ,'PASSWORD_HASH'    : None
                ,'ROLE'             : 'User'
                ,'PERMISSIONS_JSON' : None
            }

            user_info_load_payload = {
                'USER_ID'              : user_id
                ,'FIRST_NAME'          : first_name
                ,'LAST_NAME'           : last_name
                ,'USER_NAME'           : user_name
                ,'PHONE_NUMBER'        : None
                ,'DATE_OF_BIRTH'       : None
                ,'PROFILE_PICTURE_URL' : profile_picture
            }

            user_security_payload = {
                'USER_ID'                  : user_id
                ,'LAST_LOGIN_AT'           : None
                ,'CURRENT_LOGIN_AT'        : None
                ,'LOGIN_COUNT'             : 0
                ,'LAST_PASSWORD_CHANGE_AT' : None
                ,'PASSWORD_EXPIRES_AT'     : None
                ,'TWO_FACTOR_ENABLED'      : 0
                ,'TWO_FACTOR_SECRET'       : None
                ,'LAST_LOGIN_IP'           : None
                ,'LAST_LOGIN_DEVICE'       : None
                ,'CURRENT_LOGIN_IP'        : None
                ,'CURRENT_LOGIN_DEVICE'    : None
                ,'FAILED_LOGIN_AT'         : None
                ,'FAILED_LOGIN_COUNT'      : 0
                ,'EMAIL_VERIFIED'          : 1 if email_verified else 0
                ,'LOCKED_UNTIL'            : None
            }

            user_register_final_payloads = {
                'PR_USERS_LOAD'          : user_load_payload
                ,'PR_USER_INFO_LOAD'     : user_info_load_payload
                ,'PR_USER_SECURITY_LOAD' : user_security_payload
            }

            user_registry_logs = execute_process_group_using_metadata('PG_USERS_REGISTER', payloads = user_register_final_payloads)
            if user_registry_logs['status'] == 'Success':
                auth_audit_log_entry(None, None, 'Oauth Register', 'Success', f'Registered using Google OAuth with Email ID {email_id}')
            else:
                auth_audit_log_entry(None, None, 'Oauth Register', 'Failed', f"Registration failed for {email_id} - Internal Error: {user_registry_logs['message']}")
                return redirect(f'{redirect_url}/login?error=internal_server_error'), 500
        else: # if user is present
            user_security_data = fetch_queries_as_dictionaries(f"""
SELECT
    USER_ID
    ,EMAIL_VERIFIED
FROM
    {env}T_AUTH.USER_SECURITY
WHERE
    USER_ID = (SELECT DISTINCT USER_ID FROM {env}T_AUTH.USERS WHERE EMAIL_ID = '{email_id}');
    """, 'return_none', fetch = 'One')
            user_id = user_security_data['USER_ID']
            if user_security_data and user_security_data.get('EMAIL_VERIFIED') and user_security_data['EMAIL_VERIFIED'] != 1:
                update_user_security_payload = {
                    'data': {
                        'EMAIL_VERIFIED' : 1 if email_verified else 0
                    }
                    ,'conditions' : {
                        'USER_ID' : user_security_data['USER_ID']
                    }
                }
                update_table_with_payload(f'{env}T_AUTH', 'USER_SECURITY', update_user_security_payload)
        # Create session as usual
        session_id = generate_session_id()
        csrf_token = generate_csrf_token()
        session_expires_at = datetime.now() + timedelta(seconds = SESSION_COOKIE_AGE)
        user_sessions_payload = {
            'USER_ID'         : user_id
            ,'SESSION_ID'     : session_id
            ,'CSRF_TOKEN'     : csrf_token
            ,'LOGIN_DEVICE'   : request.headers.get("User-Agent")
            ,'LOGIN_IP'       : request.remote_addr
            ,'ISSUED_AT'      : datetime.now()
            ,'LAST_ACTIVE_AT' : datetime.now()
            ,'EXPIRES_AT'     : session_expires_at
            ,'REVOKED'        : 0
        }

        user_sessions_entry_final_payload = {
            'PR_USER_SESSIONS_LOAD' : user_sessions_payload
        }

        user_sessions_logs = execute_process_group_using_metadata('PG_USERS_SESSIONS_ENTRY', payloads = user_sessions_entry_final_payload)
        if user_sessions_logs['status'] == "Success":
            response = make_response("", 302)
            response.headers["Location"] = f"{redirect_url}/process_entry"
            response.set_cookie(SESSION_COOKIE_NAME, session_id, httponly = True, secure = True, samesite = "Strict", max_age = SESSION_COOKIE_AGE)
            response.set_cookie('XSRF-TOKEN', csrf_token, httponly = False, secure = True, samesite = "Strict", max_age = SESSION_COOKIE_AGE)
            auth_audit_log_entry(user_id, session_id, 'Login', 'Success', 'Logged in using Google OAuth')
            return response
            
    except Exception as e:
        return redirect(f'{redirect_url}/login?error={repr(e)}'), 500