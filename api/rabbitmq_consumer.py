import pika
import json
import threading
from config import config, secrets
from automation.message_processor import process_message
from config.config import LOG_OK
from utils.logger import log_monitoramento

def consume_messages():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(
            host=secrets.RABBITMQ_HOST,
            port=secrets.RABBITMQ_PORT,
            credentials=pika.PlainCredentials(secrets.RABBITMQ_USER, secrets.RABBITMQ_PASSWORD)
        )
    )
    channel = connection.channel()
    channel.queue_declare(queue=config.RABBITMQ_QUEUE_IN, durable=True)

    def callback(ch, method, properties, body):
        def worker():
            try:
                message = json.loads(body)
                process_message(message, properties)
            except Exception as e:
                pass
        threading.Thread(target=worker).start()

    channel.basic_consume(
        queue=config.RABBITMQ_QUEUE_IN,
        on_message_callback=callback,
        auto_ack=True
    )

    channel.start_consuming()
