import socket
import bson  # Binary JSON format
import threading  # For handling multiple clients concurrently
import pika  # RabbitMQ client library
from pika.exchange_type import ExchangeType
import logging
import datetime
import json
from concurrent.futures import ThreadPoolExecutor
logging.basicConfig(filename='receiver_log.log', level=logging.INFO)


# Global flag to signal all threads to stop
stop_threads = False

# Function to handle each client connection
def handle_client(conn):
    # Receive data from the client
    data = b""
    while True:
        part = conn.recv(4096)
        if part:
            data += part
        else:
            break

    # Deserialize the BSON data to a Python object
    obj = bson.loads(data)
    print(obj)
    # Process the received BSON object
    parse_bson_obj(obj)
    obj_json = json.dumps(obj)
    logging.info(f"Job ID: {obj['ID']} received at {datetime.datetime.now()}, job: {obj_json}")
    # Close the connection
    conn.close()


# Function to parse BSON object and publish data to RabbitMQ
def parse_bson_obj(obj):
    # Dictionary mapping data types to routing keys
    data_types = {
        'Documents': '.Document.',
        'Images': '.Image.',
        'Audio': '.Audio.',
        'Video': '.Video.'
    }
    
    # Iterate through each data type and publish relevant items
    for data_type, routing_key in data_types.items():
        if obj[data_type]:
            for item in obj[data_type]:
                #publish_to_rabbitmq(routing_key, item)
                pass
        else:
            print(f"No {data_type.lower()} to send")

# Function to publish messages to RabbitMQ
def publish_to_rabbitmq(routing_key, message):
    # Establish a connection to the RabbitMQ server
    connection_parameters = pika.ConnectionParameters('localhost')
    connection = pika.BlockingConnection(connection_parameters)

    # Create a channel for communication with RabbitMQ
    channel = connection.channel()
  
    # Serialize the message to BSON
    message = bson.dumps(message)
    
    # Publish the message to the specified routing key
    channel.basic_publish(
        exchange="Topic",
        routing_key=routing_key,
        body=message
    )

    print(f'Message "{message}" sent on routing key "{routing_key}"')

    # Close the connection to RabbitMQ
    connection.close()

# Function to start a socket server and listen for incoming BSON objects
def receive_bson_obj():
    global stop_threads
    try:
        # Create a ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=4) as executor:
            # Continuously accept new connections
            # Create a TCP/IP socket
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                # Bind the socket to localhost on port 12345
                s.bind(('localhost', 12345))
                # Listen for incoming connections
                s.listen()
                # Set a timeout so that accept() doesn't block indefinitely
                s.settimeout(1)
                while True:
                    try:
                        # Accept a connection
                        conn, addr = s.accept()
                        print('Connected by', addr)
                        # Handle each client connection in a separate thread
                        executor.submit(handle_client, conn)
                    except socket.timeout:
                        # If accept() times out, just ignore it and check if we need to stop
                        if stop_threads:
                            break
    except KeyboardInterrupt:
        print('Server stopping...')
        stop_threads = True

# Main function to start the server
if __name__ == '__main__':
    receive_bson_obj()

