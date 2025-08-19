from flask import current_app, request
from mysql.connector import Error
from utils.connection_utils.connection_pool_config import connection_pool

def auth_audit_log_entry(user_id, session_id, event_type, event_status, details, expires_at = None, used_at = None, revoked = None):
    try:
        env = current_app.config['ENVIRONMENT']
        conn = connection_pool.get_connection()
        cursor = conn.cursor()
        insert_query = f"""
INSERT INTO {env}T_LOG.AUTH_AUDIT
(
USER_ID
,SESSION_ID
,EVENT_TYPE
,EVENT_STATUS
,EVENT_IP
,EVENT_AGENT
,DETAILS
,EXPIRES_AT
,USED_AT
,REVOKED
)
VALUES
(
%s, %s, %s, %s, %s, %s, %s, %s, %s, %s 
)
    """
        insert_params = (
user_id
,session_id
,event_type
,event_status
,request.remote_addr
,request.headers.get("User-Agent")
,details
,expires_at
,used_at
,revoked
)
        cursor.execute(insert_query, insert_params)
        conn.commit()
    except Error as e:
        print(f"MySQL error: {e}")
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()