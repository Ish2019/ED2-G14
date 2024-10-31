import pika
import bson
import os

file_path = "NavyImages.pdf"  # Replace with your test PDF path

# Read the PDF file content
with open(file_path, "rb") as f:
    pdf_content = f.read()

# Prepare the message data
message = {
    "ID": "test_id_001",
    "DocumentId": "test_document_001",
    "DocumentType": "PDF",
    "FileName": os.path.basename(file_path),
    "Payload": pdf_content
}

# Connect to RabbitMQ
connection_parameters = pika.ConnectionParameters('localhost')
connection = pika.BlockingConnection(connection_parameters)
channel = connection.channel()

# Publish the message to the 'Document' queue
channel.basic_publish(
    exchange='',
    routing_key='Document',
    body=bson.dumps(message)
)

print("Message sent to Document queue.")
connection.close()
