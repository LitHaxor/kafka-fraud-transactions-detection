import json

from confluent_kafka import Consumer, KafkaError, KafkaException

from config import settings


def main():
    consumer = Consumer({
        'bootstrap.servers': settings.KAFKA_BROKER,
        'group.id': settings.KAFKA_GROUP_ID,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False,
    })

    consumer.subscribe([settings.KAFKA_TOPIC])
    print(f"listening on {settings.KAFKA_TOPIC}...")

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue

            if msg.error():
                code = msg.error().code()
                if code == KafkaError._PARTITION_EOF:
                    continue
                if code == KafkaError.UNKNOWN_TOPIC_OR_PART:
                    # Topic may not exist yet; keep polling instead of dying.
                    print(f"waiting for topic {settings.KAFKA_TOPIC}...")
                    continue
                raise KafkaException(msg.error())

            try:
                value = json.loads(msg.value())
            except json.JSONDecodeError:
                value = msg.value().decode('utf-8', errors='replace')

            key = msg.key().decode('utf-8') if msg.key() else None
            print(f"[{msg.partition()}] @ {msg.offset()} key={key} value={value}")

            # Commit only after the message has been handled.
            consumer.commit(msg, asynchronous=False)
    except KeyboardInterrupt:
        print("stopping...")
    finally:
        consumer.close()


if __name__ == '__main__':
    main()
