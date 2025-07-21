from datetime import datetime

def get_current_timestamp(format = None):
    if format:
        return datetime.now().strftime(format)
    return datetime.now()