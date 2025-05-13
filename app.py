from flask import Flask
from frontend.screen import frontend
from routes.api import api

app = Flask(__name__)

app.register_blueprint(frontend)
app.register_blueprint(api)

if __name__ == '__main__':
    app.run(debug=True)
