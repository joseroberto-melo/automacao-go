import pika
import json
from config import config, secrets
from utils.logger import setup_logger
from config.config import LOG_OK

logger = setup_logger('operation', LOG_OK)

def publish_message(message):
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=secrets.RABBITMQ_HOST, port=secrets.RABBITMQ_PORT,
                                  credentials=pika.PlainCredentials(secrets.RABBITMQ_USER, secrets.RABBITMQ_PASSWORD)))
    channel = connection.channel()
    channel.queue_declare(queue=config.RABBITMQ_QUEUE_OUT, durable=True)

    channel.basic_publish(
        exchange='',
        routing_key=config.RABBITMQ_QUEUE_OUT,
        body=json.dumps(message),
        properties=pika.BasicProperties(delivery_mode=2)
    )

    logger.info(f"Mensagem enviada: {message}")
    connection.close()