import pika
import bson
import os

def send_image_to_queue(image_path, processing_type="thumbnail"):
    # Read the image file in binary mode
    with open(image_path, "rb") as image_file:
        image_data = image_file.read()
    
    # Prepare the message
    message = {
        "ID": "test_id_001",
        "FileName": os.path.basename(image_path),
        "Payload": image_data,
        "ProcessingType": processing_type  # Options: "thumbnail", "resize", "grayscale"
    }
    
    # Connect to RabbitMQ
    connection_parameters = pika.ConnectionParameters('localhost')
    connection = pika.BlockingConnection(connection_parameters)
    channel = connection.channel()
    
    # Send the message to the 'Images' queue
    channel.basic_publish(
        exchange='',
        routing_key='Image',  # Queue name
        body=bson.dumps(message)
    )
    
    print(f"Sent image {image_path} to Images queue with processing type '{processing_type}'.")
    connection.close()

# Send an image to be processed as a thumbnail
send_image_to_queue("sample.jpg", processing_type="thumbnail")
