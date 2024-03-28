import socket
from threading import Thread
import json

port = 5000
active_workers = {}  # Dictionary to keep track of active workers and their addresses
tasks = [{"id": 1, "type": "map", "data": "Hello World from MapReduce"}]
task_status = {1: "pending"}
results = []

def handle_worker(conn, addr):
    while True:
        data = conn.recv(1024)
        if not data:
            break
        message = json.loads(data.decode('utf-8'))
        if message['type'] == 'register':
            worker_name = message['name']
            active_workers[worker_name] = addr[0]
            print(f"Registered {worker_name} from {addr}")
        elif message['type'] == 'result':
            # Process result
            results.append(message['data'])
            task_id = message['data']['task_id']
            task_status[task_id] = "completed"
            print(f"Received result for task {task_id} from {addr}")
    conn.close()

def start_server():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(('', port))
        s.listen()
        print("Master listening...")
        
        while True:
            conn, addr = s.accept()
            Thread(target=handle_worker, args=(conn, addr)).start()

def distribute_tasks():
    while not all(status == "completed" for status in task_status.values()):
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
                        print(f"Failed to connect to {worker_name}")
                        continue

if __name__ == "__main__":
    Thread(target=start_server).start()
