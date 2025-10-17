import json
import uuid

from confluent_kafka import Producer

producer_configs = {
    "bootstrap.servers": "localhost:9092",
}

producer = Producer(producer_configs)


def order_delivery_callback(error, msg):
    """Order delivery callback"""
    msg_str = msg.value().decode("utf-8")

    topic = msg.topic()
    partition = msg.partition()

    if error:
        print(
            (
                f"Error occurred during delivery of order: {msg_str} "
                f"Topic: {topic} "
                f"Partition: {partition} "
                f"Error: {error}"
            )
        )
    else:
        print(
            (
                f"{msg_str} was delivered successfully "
                f"Topic: {topic} "
                f"Partition: {partition} "
            )
        )


order = {
    "service": "order",
    "order_id": str(uuid.uuid4()),
    "user": 1,
    "item": "laptop",
    "quantity": 1,
}

order_event = json.dumps(order).encode("utf-8")


producer.produce(
    topic="order",
    value=order_event,  # Events should always be in bytes
    callback=order_delivery_callback,
)

# Ensures the buffered events are pushed to Kafka in-case of any issues.
producer.flush()
