from flask import Blueprint, request, jsonify
from utils.thread_utils.thread_executor import queued_tasks

thread_status_bp = Blueprint('thread_status', __name__)

@thread_status_bp.route('/', methods = ['GET'])
def get_thread_status():
    try:
        task_id = request.args.get('task_id')
        future = queued_tasks.get(task_id)

        if future is None:
            return jsonify({'message': 'Invalid Task ID', 'status' : 'Failed'})
        if future.running():
            return jsonify({'message': f'Task ID: {task_id} is still running', 'status' : 'Success'})
        elif future.done():
            result = future.result()
            return jsonify({'message': f'Task ID: {task_id} has completed Successfully', 'status': 'Success', 'result' : result})
        else:
            return jsonify({'message': f'Task ID: {task_id} is still pending', 'status': 'Success'})
    except Exception as e:
        return jsonify({'message': repr(e), 'status': 'Failed'})

@thread_status_bp.route('/all', methods = ['GET'])
def get_all_queued_process():
    try:
        return jsonify({'message': 'Successfully Retrieved all queued process', 'status': 'Success', 'result' : str(queued_tasks.keys())})
    except Exception as e:
        return jsonify({'message': repr(e), 'status': 'Failed'})