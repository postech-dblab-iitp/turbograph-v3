from flask import Flask, request, jsonify
import socket
import json
import struct
import time

API_ID_PREPARE_STATEMENT = 0 
API_ID_EXECUTE_STATEMENT = 1 
API_ID_FETCHALL_STATEMENT = 3
S62_PORT = 8080
BUFFER_SIZE = 8192
S62_ADDRESS = "localhost"

app = Flask(__name__)

def create_socket():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((S62_ADDRESS, S62_PORT))
    return s

def send_message_receive_json(sock, message):
    # Send the message
    sock.sendall(message.encode('utf-8'))
    
    # Buffer to store incoming data
    response_data = b""
    
    while True:
        # Read data in chunks
        chunk = sock.recv(BUFFER_SIZE)
        if not chunk:  # Stop if no more data is sent by the server
            break
        response_data += chunk
        
        # Check if we've reached the end of the JSON message
        try:
            # Attempt to parse the current response as JSON
            json.loads(response_data.decode('utf-8'))
            break
        except json.JSONDecodeError:
            # If parsing fails, continue receiving more data
            continue

    # Decode and parse the complete response
    return json.loads(response_data.decode('utf-8'))

def run_prepare_statement(sock, query):
    message = chr(API_ID_PREPARE_STATEMENT) + query
    received_data_prepare = send_message_receive_json(sock, message)
    
    if received_data_prepare["status"] == 1:
        print("Prepare statement failed")
        return -1

    return received_data_prepare["client_id"]

def run_execute_statement(sock, client_id):
    htonl_client_id = socket.htonl(client_id)
    client_id_bytes = struct.pack('I', htonl_client_id)
    message = chr(API_ID_EXECUTE_STATEMENT) + client_id_bytes.decode('latin-1')
    
    start_time = time.time()
    received_data_exec = send_message_receive_json(sock, message)
    end_time = time.time()
    
    elapsed_time = (end_time - start_time) * 1000
    received_data_exec["elapsed_time"] = int(elapsed_time)
    
    if received_data_exec["status"] == 1:
        print("Execute statement failed")
        return {}

    return received_data_exec

@app.route('/execute-s62', methods=['POST'])
def execute_s62():
    query = request.json.get('query')
    sock = create_socket()
    client_id = run_prepare_statement(sock, query)
    if client_id == -1:
        return jsonify({'error': 'Prepare statement failed', 'result_set_size': 0, 'elapsed_time': 0})
    
    # Get execution result, including property names
    execute_result = run_execute_statement(sock, client_id)
    if not execute_result or "property_names" not in execute_result:
        return jsonify({'error': 'Execute statement failed or missing property names', 'result_set_size': 0, 'elapsed_time': 0})

    # Extract property names
    print(execute_result)
    property_names = execute_result.get("property_names", [])
    
    # Fetch all results
    fetchall_message = chr(API_ID_FETCHALL_STATEMENT) + struct.pack('I', socket.htonl(client_id)).decode('latin-1')
    results = send_message_receive_json(sock, fetchall_message)

    # Parse and reformat results
    csv_data = "\n".join(results.get("data", []))

    return jsonify({
        'result_set_size': execute_result['result_set_size'],
        'elapsed_time': execute_result['elapsed_time'],
        'property_names': property_names,
        'results': csv_data
    })
    
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=6543)
