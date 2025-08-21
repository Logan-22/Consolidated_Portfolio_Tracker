from dotenv import load_dotenv

load_dotenv()

from flask import Flask, g
from frontend.screen import frontend
from routes.api import api
from routes.auth import auth
from datetime import datetime
from os import getenv
from secrets import token_urlsafe
from extensions import oauth, limiter

APP_SECRET           = getenv("FLASK_SECRET_KEY", token_urlsafe(32))
SESSION_COOKIE_NAME  = getenv("FLASK_SESSION_COOKIE_NAME", "session")

# Google OAuth
GOOGLE_CLIENT_ID     = getenv("GOOGLE_CLIENT_ID")
GOOGLE_CLIENT_SECRET = getenv("GOOGLE_CLIENT_SECRET")
GOOGLE_REDIRECT_URI  = getenv("GOOGLE_REDIRECT_URI", "http://localhost:5000/auth/google/callback")

app = Flask(__name__)
app.secret_key = APP_SECRET
app.config.update(SESSION_COOKIE_NAME = SESSION_COOKIE_NAME)

limiter.init_app(app)

oauth.init_app(app)
oauth.register(
    name                 = "google"
    ,client_id           = GOOGLE_CLIENT_ID
    ,client_secret       = GOOGLE_CLIENT_SECRET
    ,server_metadata_url = "https://accounts.google.com/.well-known/openid-configuration"
    ,client_kwargs       = {'scope' : 'openid email profile'}
    ,redirect_uri        = GOOGLE_REDIRECT_URI
)

app.register_blueprint(frontend)
app.register_blueprint(api)
app.register_blueprint(auth)

app.config['ENVIRONMENT']              = getenv('ENVIRONMENT')
app.config['SESSION_COOKIE_AGE']       = int(getenv("SESSION_COOKIE_AGE", 60 * 60 * 24))
app.config['SESSION_COOKIE_NAME']      = getenv("SESSION_COOKIE_NAME", "session")
app.config['SESSION_IDLE_TIMEOUT']     = getenv("SESSION_IDLE_TIMEOUT", 60 * 30)
app.config['PASSWORD_RESET_EXP_HOURS'] = int(getenv('PASSWORD_RESET_EXP_HOURS', 1))
GOOGLE_CLIENT_ID                       = getenv('GOOGLE_CLIENT_ID')

@app.context_processor
def inject_data():
    return {'current_year': datetime.now().year, 'app_name' : 'Consolidated Tracker', 'GOOGLE_CLIENT_ID' : GOOGLE_CLIENT_ID}

@app.teardown_appcontext
def close_db_connection(exc):
    db_connection = g.pop('db_connection', None)
    if db_connection:
        db_connection.close()

if __name__ == '__main__':
    app.run(debug=True)
