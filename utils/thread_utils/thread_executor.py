from concurrent.futures import ThreadPoolExecutor
from os import getenv
from flask import current_app

max_thread_workers = int(getenv('MAX_THREAD_WORKERS', 1))

thread_executor = ThreadPoolExecutor(max_workers = max_thread_workers)

queued_tasks = {}

def submit_threaded_task(function_name, *args, **kwargs):
    app = current_app._get_current_object()
    def wrapper():
        with app.app_context():
            return function_name(*args, **kwargs)
    task_id = str(len(queued_tasks) + 1)
    future = thread_executor.submit(wrapper)
    queued_tasks[task_id] = future
    return task_id