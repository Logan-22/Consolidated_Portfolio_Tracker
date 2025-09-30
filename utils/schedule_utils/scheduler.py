from apscheduler.schedulers.background import BackgroundScheduler
from utils.schedule_utils.scheduled_refresh import scheduled_refresh

def init_scheduler(app):
    scheduler = BackgroundScheduler()
    scheduler.add_job(func = lambda: scheduled_refresh(app), trigger = 'interval', max_instances = 1, hours = 1)
    scheduler.start()
    return scheduler