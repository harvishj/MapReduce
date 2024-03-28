import socket
import json
import sys

def handle_map_task(text):
    result = {}
    for word in text.split():
        result[word] = result.get(word, 0) + 1
    return result

def main(master_host, port, worker_name):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((master_host, port))
        
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
        
        main(sys.argv[1], int(sys.argv[2]), sys.argv[3])

    else:
        print("Usage: python worker.py <master_host> <port> <worker_name>")
        sys.exit(1)
