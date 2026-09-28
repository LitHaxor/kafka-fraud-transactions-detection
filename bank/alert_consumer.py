"""Print the fraud alerts Flink writes to bank.alerts.

    python -m bank.alert_consumer
    python -m bank.alert_consumer --from-start
"""

import argparse
import json

from confluent_kafka import Consumer, KafkaError, KafkaException

from config import settings

COLOR = {"CRITICAL": "\033[91m", "HIGH": "\033[93m", "MEDIUM": "\033[95m"}
RESET = "\033[0m"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--from-start", action="store_true",
                    help="replay every alert instead of only new ones")
    ap.add_argument("--group", default=None)
    args = ap.parse_args()

    consumer = Consumer({
        'bootstrap.servers': settings.KAFKA_BROKER,
        'group.id': args.group or f"{settings.BANK_GROUP_ID}-cli",
        'auto.offset.reset': 'earliest' if args.from_start else 'latest',
        'enable.auto.commit': False,
    })
    consumer.subscribe([settings.BANK_ALERTS_TOPIC])
    print(f"watching {settings.BANK_ALERTS_TOPIC}...\n")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                code = msg.error().code()
                if code in (KafkaError._PARTITION_EOF, KafkaError.UNKNOWN_TOPIC_OR_PART):
                    continue
                raise KafkaException(msg.error())

            a = json.loads(msg.value())
            c = COLOR.get(a.get("severity"), "")
            print(f"{c}{a.get('severity','?'):<9}{RESET} {a.get('rule_name','?'):<18} "
                  f"{a.get('account_id','?'):<9} {a.get('detected_at','')[11:19]}  "
                  f"{a.get('detail','')}")
            consumer.commit(msg, asynchronous=False)
    except KeyboardInterrupt:
        print("\nstopping...")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
