from authlib.integrations.flask_client import OAuth
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address

limiter = Limiter(key_func = get_remote_address)

oauth = OAuth()