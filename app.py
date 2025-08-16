from dotenv import load_dotenv

load_dotenv()

from flask import Flask, g
from frontend.screen import frontend
from routes.limiter import limiter
from routes.api import api
from routes.auth import auth
from datetime import datetime
from os import getenv
from secrets import token_urlsafe

APP_SECRET          = getenv("FLASK_SECRET_KEY", token_urlsafe(32))
SESSION_COOKIE_NAME = getenv("FLASK_SESSION_COOKIE_NAME", "session")

app = Flask(__name__)
app.secret_key = APP_SECRET
app.config.update(SESSION_COOKIE_NAME = SESSION_COOKIE_NAME)

limiter.init_app(app)

app.register_blueprint(frontend)
app.register_blueprint(api)
app.register_blueprint(auth)

app.config['ENVIRONMENT'] = getenv('ENVIRONMENT')

@app.context_processor
def inject_data():
    return {'current_year': datetime.now().year, 'app_name' : 'Consolidated Tracker'}

@app.teardown_appcontext
def close_db_connection(exc):
    db_connection = g.pop('db_connection', None)
    if db_connection:
        db_connection.close()

if __name__ == '__main__':
    app.run(debug=True)
