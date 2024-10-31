import pika
import bson

def decode_message(ch, method, properties, body):
    # Decode BSON message
    message = bson.loads(body)
    print("Decoded Message:")
    for key, value in message.items():
        print(f"{key}: {value}")
    print("\n--- End of Message ---\n")

def read_from_queue(queue_name="ClassifiedImages"):
    # Connect to RabbitMQ
    connection_parameters = pika.ConnectionParameters('localhost')
    connection = pika.BlockingConnection(connection_parameters)
    channel = connection.channel()

    # Declare the queue to ensure it exists
    channel.queue_declare(queue=queue_name, durable=True)

    # Consume messages from the queue
    channel.basic_consume(queue=queue_name, on_message_callback=decode_message, auto_ack=True)
    print(f"Listening for messages on '{queue_name}' queue...")
    channel.start_consuming()

# Run the function to start consuming messages
read_from_queue("ClassifiedImages")
