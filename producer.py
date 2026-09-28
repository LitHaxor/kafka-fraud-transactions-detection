import json
import signal
import sys
import time

from confluent_kafka import Producer

from config import settings


def delivery_report(err, msg):
    """Called once per message to report delivery success or failure."""
    if err is not None:
        print(f"delivery failed: {err}")
    else:
        print(f"delivered to {msg.topic()} [{msg.partition()}] @ {msg.offset()}")


def main():
    producer = Producer({
        'bootstrap.servers': settings.KAFKA_BROKER,
    })

    running = True

    def shutdown(signum, frame):
        nonlocal running
        running = False

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    i = 0
    while running:
        value = {'id': i, 'message': f'hello {i}', 'ts': time.time()}

        # Serve delivery callbacks from previous produce() calls.
        producer.poll(0)

        producer.produce(
            settings.KAFKA_TOPIC,
            key=str(i),
            value=json.dumps(value),
            on_delivery=delivery_report,
        )

        i += 1
        time.sleep(1)

    print("flushing...")
    remaining = producer.flush(10)
    if remaining:
        print(f"{remaining} message(s) not delivered")
        sys.exit(1)


if __name__ == '__main__':
    main()
