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

def handle_reduce_task(data):
    # Assume data is in the format: {"key": [values]}
    key, values = list(data.items())[0]
    return {key: sum(values)}

def connect_to_master(master_host, port, retry_attempts=5, retry_interval=2):
    for attempt in range(retry_attempts):
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((master_host, port))
            logging.info(f"Connected to master at {master_host}:{port}")
            return sock
        except ConnectionError as e:
            logging.warning(f"Attempt {attempt + 1} failed to connect to master: {e}")
            time.sleep(retry_interval)
    logging.error("Could not connect to the master after several attempts.")
    return None

def main(master_host, port, worker_name):
    sock = connect_to_master(master_host, port)
    if not sock:
        sys.exit(1)
    
    try:
        # Send registration message
        register_msg = json.dumps({"type": "register", "name": worker_name})
        sock.sendall(register_msg.encode('utf-8'))

        while True:
            try:
                data = sock.recv(1024)
                if not data:
                    logging.info("Connection closed by the master.")
                    break

                message = json.loads(data.decode('utf-8'))
                if message['type'] == 'map':
                    result = handle_map_task(message['data'])
                    result_msg = json.dumps({"type": "map_result", "data": result})
                    sock.sendall(result_msg.encode('utf-8'))
                elif message['type'] == 'reduce':
                    result = handle_reduce_task(message['data'])
                    result_msg = json.dumps({"type": "reduce_result", "data": result})
                    sock.sendall(result_msg.encode('utf-8'))
                elif message['type'] == 'terminate':
                    logging.info("Received terminate command. Exiting.")
                    break
                else:
                    logging.error(f"Unknown task type: {message['type']}")

            except json.JSONDecodeError as e:
                logging.error(f"Error decoding JSON: {e}")
            except Exception as e:
                logging.error(f"Unexpected error: {e}")
                break

    finally:
        logging.info("Worker shutting down.")
        sock.close()

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
