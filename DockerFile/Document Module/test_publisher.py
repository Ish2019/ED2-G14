import pika
import bson

# Sample test message structure
test_message = {
    "ID": "TestID",
    "DocumentId": "TestDocID",
    "DocumentType": "pdf",
    "FileName": "test_document.pdf",
    "Payload": b"Test binary content for the document"  # Replace with actual binary content if needed
}

def publish_test_message():
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    channel.queue_declare(queue='Document')  # Ensure the queue name matches the consumer setup
    channel.basic_publish(exchange='', routing_key='Document', body=bson.dumps(test_message))
    print(" [x] Sent test message")
    connection.close()

publish_test_message()
