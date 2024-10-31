import pika
from pika.exchange_type import ExchangeType
import bson

def consumer_connection(routing_key):
    # Establish a connection to RabbitMQ server
    connection_parameters = pika.ConnectionParameters('localhost')
    connection = pika.BlockingConnection(connection_parameters)

    # Create a channel
    channel = connection.channel()

    # Declare a queue (queue names are generated based on the routing key)
    queue_name = routing_key

    # Consume messages from the queue
    a=channel.basic_consume(queue=queue_name, auto_ack=True,
        on_message_callback=on_message_received)
    
    print('Preprocess Starting Consuming')
    
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        channel.close()
        connection.close()

def on_message_received(ch, method, properties, body):
    body=bson.loads(body)
    # Strip .pdf from FileName and replace with .txt
    base_file_name = body["FileName"].replace('.pdf', '.txt')
    
    #save the image
    with open(f'{body["FileName"]}', 'wb') as file:
        file.write(body['Payload'])
    # Save the 'Meta' data to a new file with .txt extension
    with open(f'{base_file_name} Meta.txt', 'wb') as file:
        file.write(body['Meta'])

    # Save the 'Summary' data to a new file with .txt extension
    with open(f'{base_file_name} Summary.txt', 'wb') as file:
        file.write(body['Summary'])

    # Save the 'Keywords' data to a new file with .txt extension
    with open(f'{base_file_name} Keywords.txt', 'wb') as file:
        file.write(body['Keywords'])
    

consumer_connection('Store')