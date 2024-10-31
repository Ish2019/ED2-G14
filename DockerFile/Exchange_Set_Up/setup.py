import pika
from pika.exchange_type import ExchangeType

# Establish a connection to RabbitMQ server
connection_parameters = pika.ConnectionParameters('rabbitmq_tshark')
connection = pika.BlockingConnection(connection_parameters)

# Create a channel
channel = connection.channel()

routing_keys = [
    "#.Document.#",
    "#.Store.#",
    "#.Image.#",
    "#.Audio.#",
    "#.Video.#",
]
channel.exchange_declare(exchange='Topic', exchange_type=ExchangeType.topic, durable=True)

channel.queue_declare(queue='Dashboard', durable=True)

# Declare queues
for key in routing_keys:
    if key not in ["#.Audio.#", "#.Video.#"]:
        channel.queue_declare(queue=key[2:-2], durable=True)
    else:
        # make it a lazy queue
        channel.queue_declare(queue=key[2:-2], durable=True, arguments={"x-queue-mode": "lazy"})
    #bind queues to exchange
    channel.queue_bind(exchange='Topic', queue=key[2:-2], routing_key=key)

channel.queue_bind(exchange='Topic', queue='Dashboard', routing_key='#.Status.#')

# Close the connection
connection.close()
