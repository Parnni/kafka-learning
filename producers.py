import json
import uuid
from random import choice

from confluent_kafka import Producer

from topics import Topics

producer_configs = {
    "bootstrap.servers": "localhost:9092",
}

producer = Producer(producer_configs)


def order_delivery_callback(error, msg):
    """Order delivery callback"""
    if error:
        print(f"❌ Delivery failed: {error}")
    else:
        print(f"✅ Delivered {msg.value().decode("utf-8")}")
        print(
            (
                "✅ Delivered to "
                f"{msg.topic()} : partition {msg.partition()} : "
                f"at offset {msg.offset()}"
            )
        )


users = [1, 2, 3, 4, 5, 6]
items = ["laptop", "book", "mobile", "mouse", "gifts"]
quantities = [1, 2, 3, 4, 5]

if __name__ == "__main__":
    for _ in range(20):
        order = {
            "service": "order",
            "order_id": str(uuid.uuid4()),
            "user": choice(users),
            "item": choice(items),
            "quantity": choice(quantities),
        }

        order_event = json.dumps(order).encode("utf-8")

        producer.produce(
            topic=Topics.ORDER.value,
            value=order_event,  # Events should always be in bytes
            callback=order_delivery_callback,
        )

    # Ensures the buffered events are pushed to Kafka in-case of any issues.
    producer.flush()
