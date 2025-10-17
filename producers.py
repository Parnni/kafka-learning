import json
import uuid

from confluent_kafka import Producer

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
