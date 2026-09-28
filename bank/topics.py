"""Create the bank topics. Auto-create is off on this cluster, so run this first.

    python -m bank.topics
"""

from confluent_kafka.admin import AdminClient, NewTopic

from config import settings

TOPICS = [
    (settings.BANK_TXN_TOPIC, 3),
    (settings.BANK_ALERTS_TOPIC, 3),
]
REPLICATION = 3  # the cluster runs 3 brokers


def main():
    admin = AdminClient({'bootstrap.servers': settings.KAFKA_BROKER})
    existing = admin.list_topics(timeout=10).topics

    wanted = [
        NewTopic(name, num_partitions=parts, replication_factor=REPLICATION)
        for name, parts in TOPICS if name not in existing
    ]

    for name, _ in TOPICS:
        if name in existing:
            print(f"exists  {name}")

    if not wanted:
        return

    for name, fut in admin.create_topics(wanted).items():
        try:
            fut.result()
            print(f"created {name}")
        except Exception as exc:
            print(f"failed  {name}: {exc}")


if __name__ == "__main__":
    main()
