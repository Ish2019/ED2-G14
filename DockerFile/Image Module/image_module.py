import os
import io
import torch
import torch.nn.functional as F
from PIL import Image
import pika
import bson
from transformers import AutoModelForImageClassification, AutoImageProcessor

# Load the pre-trained model and preprocessor
model_name = "microsoft/resnet-50"
model = AutoModelForImageClassification.from_pretrained(model_name)
preprocessor = AutoImageProcessor.from_pretrained(model_name)

FilePath = os.path.dirname(__file__)

def classify_image(image_data, file_name):
    # Open image from bytes
    image = Image.open(io.BytesIO(image_data))

    # Preprocess and classify
    inputs = preprocessor(images=image, return_tensors="pt")
    with torch.no_grad():
        outputs = model(**inputs)

    # Get predicted class index and confidence score
    logits = outputs.logits
    predicted_class_indices = torch.argmax(logits, dim=-1).tolist()
    predicted_class = model.config.id2label[predicted_class_indices[0]]

    # Calculate probabilities using softmax and confidence score
    probabilities = F.softmax(logits, dim=-1)
    confidence_score = probabilities[0, predicted_class_indices].item()

    # Print and return classification results
    print(f"Predicted class: {predicted_class}")
    print(f"Confidence score: {confidence_score:.2f}")
    return predicted_class, confidence_score

def publish_to_rabbitmq(routing_key, message):
    connection_parameters = pika.ConnectionParameters('localhost')
    connection = pika.BlockingConnection(connection_parameters)
    channel = connection.channel()

    # Declare the queue to ensure it exists
    channel.queue_declare(queue=routing_key, durable=True)

    message = bson.dumps(message)
    channel.basic_publish(exchange="", routing_key=routing_key, body=message)
    connection.close()

def on_message_received(ch, method, properties, body):
    try:
        body = bson.loads(body)
        
        # Classify the image
        image_data = body['Payload']
        file_name = body['FileName']
        predicted_class, confidence_score = classify_image(image_data, file_name)

        # Prepare the response message with classification data
        response_message = {
            "ID": body["ID"],
            "FileName": file_name,
            "PredictedClass": predicted_class,
            "ConfidenceScore": confidence_score,
            "Status": "Classification Successful",
            "Message": f"Image {file_name} classified as {predicted_class} with confidence {confidence_score:.2f}"
        }
        
        # Publish classification results to RabbitMQ
        publish_to_rabbitmq('ClassifiedImages', response_message)

    except Exception as e:
        error_message = str(e)
        print("Classification failed:", error_message)
        status_message = body.copy()
        del status_message['Payload']
        status_message['Status'] = 'Classification Failed'
        status_message['Message'] = error_message
        publish_to_rabbitmq(".Status.", bson.dumps(status_message))

def consumer_connection(routing_key):
    connection_parameters = pika.ConnectionParameters('localhost')
    connection = pika.BlockingConnection(connection_parameters)
    channel = connection.channel()
    
    # Ensure queue exists
    channel.queue_declare(queue=routing_key, durable=True)
    
    channel.basic_consume(queue=routing_key, auto_ack=True, on_message_callback=on_message_received)
    print('Image Classification Module Starting Consuming')
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        channel.close()
        connection.close()

if __name__ == "__main__":
    consumer_connection('Image')
