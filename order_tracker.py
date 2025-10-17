import json

from confluent_kafka import Consumer

consumer_configs = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "order-tracker-group",
    "auto.offset.reset": "earliest",
}

consumer = Consumer(consumer_configs)
consumer.subscribe(
    [
        "order",
    ]
)

try:
    while True:
        msg = consumer.poll(5.0)
        if msg is None:
            print("No messages found")
            continue
        if msg.error():
            print(f"Error occurred: {msg.error()}")
            continue

        topic = msg.topic()
        partition = msg.partition()

        order_event = json.loads(msg.value().decode("utf-8"))
        print(
            f"Consuming msg from topic: {topic} and partition: {partition}", order_event
        )
except (Exception, KeyboardInterrupt) as e:
    print(f"Error: {e}")
finally:
    consumer.close()
