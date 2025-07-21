from flask import Flask
from frontend.screen import frontend
from routes.api import api
from datetime import datetime

app = Flask(__name__)

app.register_blueprint(frontend)
app.register_blueprint(api)

@app.context_processor
def inject_current_year():
    return {'current_year': datetime.now().year}

if __name__ == '__main__':
    app.run(debug=True)
