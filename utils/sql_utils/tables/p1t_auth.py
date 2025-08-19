from os import getenv
from utils.connection_utils.connection_pool_config import connection_pool

env = getenv('ENVIRONMENT')

def create_authorization_schema(auth_schema = f"{env}T_AUTH"):
    conn = connection_pool.get_connection()
    cursor = conn.cursor()
    cursor.execute(f"""
CREATE DATABASE IF NOT EXISTS {auth_schema}
CHARACTER SET utf8mb4
COLLATE utf8mb4_unicode_ci;
    """)
    cursor.close()
    conn.close()

def create_users_table(auth_schema = f"{env}T_AUTH"):
    conn = connection_pool.get_connection()
    cursor = conn.cursor()
    cursor.execute(f"""
CREATE TABLE IF NOT EXISTS {auth_schema}.USERS
(
    ID                       INT            AUTO_INCREMENT PRIMARY KEY,
    USER_ID                  INT            UNIQUE NOT NULL,
    EMAIL_ID                 VARCHAR(255),
    PASSWORD_HASH            VARCHAR(1024),
    ROLE                     VARCHAR(20),
    PERMISSIONS_JSON         JSON,
    IS_ACTIVE                TINYINT        DEFAULT 1,
    CREATED_AT               DATETIME       DEFAULT CURRENT_TIMESTAMP,
    UPDATED_AT               DATETIME       DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UPDATE_PROCESS_NAME      VARCHAR(100),
    UPDATE_PROCESS_ID        INT,
    PROCESS_NAME             VARCHAR(100),
    PROCESS_ID               INT,
    START_DATE               DATE,
    END_DATE                 DATE,
    RECORD_DELETED_FLAG      INT            DEFAULT 0,
    INDEX idx_users (USER_ID)
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_unicode_ci;
    """)
    cursor.close()
    conn.close()

def create_user_profile_table(auth_schema = f"{env}T_AUTH"):
    conn = connection_pool.get_connection()
    cursor = conn.cursor()
    cursor.execute(f"""
CREATE TABLE IF NOT EXISTS {auth_schema}.USER_INFO
(
    ID                       INT            AUTO_INCREMENT PRIMARY KEY,
    USER_ID                  INT            NOT NULL,
    FIRST_NAME               VARCHAR(255),
    LAST_NAME                VARCHAR(255),
    USER_NAME                VARCHAR(255),
    PHONE_NUMBER             VARCHAR(20),
    DATE_OF_BIRTH            DATE,
    PROFILE_PICTURE_URL      VARCHAR(1024),
    UPDATE_PROCESS_NAME      VARCHAR(100),
    UPDATE_PROCESS_ID        INT,
    PROCESS_NAME             VARCHAR(100),
    PROCESS_ID               INT,
    START_DATE               DATE,
    END_DATE                 DATE,
    RECORD_DELETED_FLAG      INT            DEFAULT 0,
    INDEX idx_user_info (USER_ID)
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_unicode_ci;
    """)
    cursor.close()
    conn.close()

def create_user_security_table(auth_schema = f"{env}T_AUTH"):
    conn = connection_pool.get_connection()
    cursor = conn.cursor()
    cursor.execute(f"""
CREATE TABLE IF NOT EXISTS {auth_schema}.USER_SECURITY
(
    ID                       INT            AUTO_INCREMENT PRIMARY KEY,
    USER_ID                  INT            NOT NULL,
    LAST_LOGIN_AT            DATETIME,
    CURRENT_LOGIN_AT         DATETIME,
    LOGIN_COUNT              TINYINT        DEFAULT 0,
    LAST_PASSWORD_CHANGE_AT  DATETIME,
    PASSWORD_EXPIRES_AT      DATETIME,
    TWO_FACTOR_ENABLED       TINYINT        DEFAULT 0,
    TWO_FACTOR_SECRET        VARCHAR(255),
    LAST_LOGIN_IP            VARCHAR(45),
    LAST_LOGIN_DEVICE        VARCHAR(255),
    CURRENT_LOGIN_IP         VARCHAR(45),
    CURRENT_LOGIN_DEVICE     VARCHAR(255),
    FAILED_LOGIN_AT          DATETIME,
    FAILED_LOGIN_COUNT       TINYINT        DEFAULT 0,
    EMAIL_VERIFIED           TINYINT        DEFAULT 0,
    LOCKED_UNTIL             DATETIME       NULL,
    UPDATE_PROCESS_NAME      VARCHAR(100),
    UPDATE_PROCESS_ID        INT,
    PROCESS_NAME             VARCHAR(100),
    PROCESS_ID               INT,
    START_DATE               DATE,
    END_DATE                 DATE,
    RECORD_DELETED_FLAG      INT            DEFAULT 0,
    INDEX idx_user_security (USER_ID)
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_unicode_ci;
    """)
    cursor.close()
    conn.close()

def create_user_sessions_table(auth_schema = f"{env}T_AUTH"):
    conn = connection_pool.get_connection()
    cursor = conn.cursor()
    cursor.execute(f"""
CREATE TABLE IF NOT EXISTS {auth_schema}.USER_SESSIONS
(
    ID                       INT            AUTO_INCREMENT PRIMARY KEY,
    USER_ID                  INT            NOT NULL,
    SESSION_ID               VARCHAR(255),
    CSRF_TOKEN               VARCHAR(255),
    LOGIN_DEVICE             VARCHAR(255),
    LOGIN_IP                 VARCHAR(45),
    ISSUED_AT                DATETIME,
    LAST_ACTIVE_AT           DATETIME,
    EXPIRES_AT               DATETIME,
    REVOKED                  TINYINT        DEFAULT 0,
    UPDATE_PROCESS_NAME      VARCHAR(100),
    UPDATE_PROCESS_ID        INT,
    PROCESS_NAME             VARCHAR(100),
    PROCESS_ID               INT,
    START_DATE               DATE,
    END_DATE                 DATE,
    RECORD_DELETED_FLAG      INT            DEFAULT 0,
    INDEX idx_user_sessions(USER_ID, EXPIRES_AT)
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_unicode_ci;
    """)
    cursor.close()
    conn.close()

def create_password_resets_table(auth_schema = f"{env}T_AUTH"):
    conn = connection_pool.get_connection()
    cursor = conn.cursor()
    cursor.execute(f"""
CREATE TABLE IF NOT EXISTS {auth_schema}.PASSWORD_RESETS
(
    ID                       INT            AUTO_INCREMENT PRIMARY KEY,
    USER_ID                  INT            NOT NULL,
    TOKEN_HASH               VARCHAR(512),
    EXPIRES_AT               DATETIME,
    USED_AT                  DATETIME,
    CREATED_AT               DATETIME       DEFAULT CURRENT_TIMESTAMP,
    REVOKED                  TINYINT        DEFAULT 0,
    UPDATE_PROCESS_NAME      VARCHAR(100),
    UPDATE_PROCESS_ID        INT,
    PROCESS_NAME             VARCHAR(100),
    PROCESS_ID               INT,
    START_DATE               DATE,
    END_DATE                 DATE,
    RECORD_DELETED_FLAG      INT            DEFAULT 0,
    INDEX idx_password_resets(USER_ID, EXPIRES_AT)
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_unicode_ci;
    """)
    cursor.close()
    conn.close()
