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

def main(master_host, port, worker_name):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.connect((master_host, port))
        logging.info(f"{worker_name} connected to master at {master_host}:{port}")

        # Send registration message
        register_msg = json.dumps({"type": "register", "name": worker_name})
        s.sendall(register_msg.encode('utf-8'))

        # Keep alive mechanism - optional
        s.settimeout(10.0)  # Set a timeout for recv operation to prevent blocking indefinitely

        while True:
            try:
                # Listen for tasks or commands from the master
                logging.info("Waiting for tasks...")
                data = s.recv(1024)
                if not data:
                    # If recv returns an empty bytes object, the connection has been closed
                    logging.info("Master closed connection.")
                    break
            except socket.timeout:
                # If recv times out, just try again
                logging.error("Connection timed out. Retrying...")
                continue
            except Exception as e:
                logging.error(f"An error occurred while receiving data: {e}")
                break

            message = json.loads(data.decode('utf-8'))

            # Handle different types of tasks
            logging.info(f"Received task: {message}")
            if message['type'] == 'map':
                result = handle_map_task(message['data'])
                result_msg = json.dumps({"type": "map_result", "data": result})
                s.sendall(result_msg.encode('utf-8'))
            elif message['type'] == 'reduce':
                result = handle_reduce_task(message['data'])
                result_msg = json.dumps({"type": "reduce_result", "data": result})
                s.sendall(result_msg.encode('utf-8'))
            elif message['type'] == 'terminate':
                logging.info("Received terminate command. Exiting.")
                sys.exit(0)
            else:
                logging.error(f"Unknown task type: {message['type']}")
                
    except Exception as e:
        logging.error(f"An error occurred: {e}")
    finally:
        logging.info("Closing connection.")
        s.close()

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
