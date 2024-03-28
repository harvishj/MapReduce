import socket
from threading import Thread, Lock
import json
import logging
import time

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')

port = 5000
active_workers = {}
worker_lock = Lock()
tasks = [{"id": 1, "type": "map", "data": "Hello World from MapReduce Hello MapReduce World"}]
map_results = []
reduce_tasks = []
task_status = {task["id"]: "pending" for task in tasks}
results = []

def handle_worker(conn, addr):
    global active_workers, map_results
    try:
        while True:
            data = conn.recv(1024)
            if not data:
                break
            message = json.loads(data.decode('utf-8'))
            if message['type'] == 'register':
                with worker_lock:
                    worker_name = message['name']
                    active_workers[worker_name] = addr[0]
                    logging.info(f"Registered {worker_name} from {addr}")
                    if len(active_workers) == 1:
                        Thread(target=distribute_map_tasks).start()
            elif message['type'] == 'map_result':
                map_results.extend(message['data'])
                if len(map_results) == len(tasks):  # All map tasks have completed
                    prepare_reduce_tasks()
                    Thread(target=distribute_reduce_tasks).start()
            elif message['type'] == 'reduce_result':
                results.append(message['data'])
                if len(results) == len(reduce_tasks):  # All reduce tasks have completed
                    print_final_results()
    except Exception as e:
        logging.error(f"Error handling worker {addr}: {e}")
    finally:
        with worker_lock:
            worker_name = next((name for name, worker_addr in active_workers.items() if worker_addr == addr[0]), None)
            if worker_name:
                del active_workers[worker_name]
                logging.info(f"Worker {worker_name} disconnected.")
        conn.close()

def distribute_map_tasks():
    distribute_tasks(tasks, "map")

def distribute_reduce_tasks():
    distribute_tasks(reduce_tasks, "reduce")

def distribute_tasks(tasks, task_type):
    global active_workers
    for task in tasks:
        task_assigned = False
        while not task_assigned:
            with worker_lock:
                for worker_name, worker_addr in active_workers.items():
                    try:
                        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                            s.connect((worker_addr, port))
                            task_message = json.dumps({"type": task_type, "data": task})
                            s.send(task_message.encode('utf-8'))
                            task_assigned = True
                            break
                    except ConnectionError:
                        logging.warning(f"Failed to connect to {worker_name}")
            if not task_assigned:
                time.sleep(1)

def prepare_reduce_tasks():
    global map_results, reduce_tasks
    # Group map results by key
    grouped_results = {}
    for result in map_results:
        for key, value in result.items():
            if key in grouped_results:
                grouped_results[key].append(value)
            else:
                grouped_results[key] = [value]
    # Prepare reduce tasks
    reduce_tasks = [{"id": i + 1, "data": {key: values}} for i, (key, values) in enumerate(grouped_results.items())]

def print_final_results():
    for result in results:
        logging.info(f"Reduce result: {result}")

def start_server():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(('', port))
        s.listen()
        logging.info("Master listening...")

        while True:
            conn, addr = s.accept()
            Thread(target=handle_worker, args=(conn, addr)).start()

if __name__ == "__main__":
    logging.info("Starting master server...")
    Thread(target=start_server).start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logging.info("Master server shutting down.")
