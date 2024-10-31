import pika
from pika.exchange_type import ExchangeType
import bson

def consumer_connection(queue_name):
    # Establish a connection to RabbitMQ server
    connection_parameters = pika.ConnectionParameters('localhost')
    connection = pika.BlockingConnection(connection_parameters)

    # Create a channel
    channel = connection.channel()

    # Declare a queue (queue names are generated based on the routing key)
    queue_name = queue_name

    # Consume messages from the queue
    a=channel.basic_consume(queue=queue_name, auto_ack=True,
        on_message_callback=on_message_received)
    
    print('Starting Consuming')
    
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        channel.close()
        connection.close()

def on_message_received(ch, method, properties, body):
    body=bson.loads(body)
    print(f"""
            received new message: {body}
            routing key: {method.routing_key}
            """)
    

consumer_connection('Dashboard') # This is the routing key for the dashboard