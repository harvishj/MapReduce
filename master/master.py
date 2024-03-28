import socket
from threading import Thread, Lock
import json
import time
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

port = 5000
active_workers = {}  # Dictionary to keep track of active workers and their addresses
worker_lock = Lock()  # Lock for thread-safe operations on active_workers
tasks = [{"id": 1, "type": "map", "data": "Hello World from MapReduce"}]
task_status = {1: "pending"}
results = []

def handle_worker(conn, addr):
    global active_workers
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
                    # If this is the first worker, start distributing tasks
                    if len(active_workers) == 1:
                        Thread(target=distribute_tasks).start()
            elif message['type'] == 'result':
                # Process result
                results.append(message['data'])
                task_id = message['data']['task_id']
                task_status[task_id] = "completed"
                logging.info(f"Received result for task {task_id} from {addr}")
    except Exception as e:
        logging.error(f"Error handling worker {addr}: {e}")
    finally:
        with worker_lock:
            worker_name = next((name for name, worker_addr in active_workers.items() if worker_addr == addr[0]), None)
            if worker_name:
                del active_workers[worker_name]
                logging.info(f"Worker {worker_name} disconnected.")
        conn.close()

def start_server():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(('', port))
        s.listen()
        logging.info("Master listening...")

        while True:
            conn, addr = s.accept()
            Thread(target=handle_worker, args=(conn, addr)).start()

def distribute_tasks():
    global active_workers, task_status
    logging.info("Distributing tasks...")
    while not all(status == "completed" for status in task_status.values()):

        with worker_lock:

            if active_workers:  # Proceed only if there are active workers
                for task_id, status in task_status.items():
                    if status == "pending":
                        for worker_name, worker_addr in active_workers.items():
                            try:
                                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                                    s.connect((worker_addr, port))
                                    task = next((task for task in tasks if task['id'] == task_id), None)
                                    if task:
                                        s.send(json.dumps(task).encode('utf-8'))
                                        task_status[task_id] = f"assigned_to_{worker_name}"
                                        print(f"Task {task_id} assigned to {worker_name}")
                                        break
                            except ConnectionError:
                                logging.warning(f"Failed to connect to {worker_name}")
                                continue
            time.sleep(1)  # Wait a bit before trying to distribute tasks again

if __name__ == "__main__":
    logging.info("Starting master server...")
    Thread(target=start_server).start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logging.info("Master server shutting down.")