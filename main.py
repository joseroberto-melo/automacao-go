# main.py
from api.rabbitmq_consumer import consume_messages
from utils.resource_monitor import monitor_resources
import threading

if __name__ == "__main__":
    threading.Thread(target=consume_messages, daemon=True).start()
    threading.Thread(target=monitor_resources, daemon=True).start()

    while True:
        pass
