import socket
import json
import sys
import time
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def handle_map_task(text):
    result = {}
    for word in text.split():
        result[word] = result.get(word, 0) + 1
    logging.info(f"Processed task: {text}")
    logging.info(f"Result: {result}")
    return result

def main(master_host, port, worker_name):
    connected = False
    for _ in range(5):  # Retry up to 5 times
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((master_host, port))
            connected = True
            break  # Exit loop if connection is successful
        except socket.error as err:
            logging.warning(f"Connection attempt failed: {err}")
            time.sleep(2)  # Wait for 2 seconds before retrying

    if not connected:
        logging.error("Failed to connect to master after several attempts.")
        sys.exit(1)

    # Send registration message
    register_msg = json.dumps({"type": "register", "name": worker_name})
    s.sendall(register_msg.encode('utf-8'))
    
    while True:
        data = s.recv(1024)
        if not data:
            break
        task = json.loads(data)
        if task['type'] == 'map':
            result = handle_map_task(task['data'])
            result_msg = json.dumps({"type": "result", "data": {"task_id": task['id'], "result": result}})
            s.sendall(result_msg.encode('utf-8'))
        elif task['type'] == 'terminate':
            break

if __name__ == "__main__":
    if len(sys.argv) == 4:
        master_host = sys.argv[1]
        port = int(sys.argv[2])
        worker_name = sys.argv[3]
        # Rest of your worker code
        
        logging.info(f"Starting worker {worker_name} connecting to master at {master_host}:{port}")
        main(sys.argv[1], int(sys.argv[2]), sys.argv[3])

    else:
        print("Usage: python worker.py <master_host> <port> <worker_name>")
        sys.exit(1)
