"""Stream dummy bank transactions into Kafka.

Most traffic is ordinary spending. Every so often the generator injects one of
the three patterns the Flink job looks for, so the alert stream is never empty.

    python -m bank.txn_producer                 # run until Ctrl-C
    python -m bank.txn_producer --rate 5        # 5 txns/sec
    python -m bank.txn_producer --fraud-rate 0  # clean traffic only
"""

import argparse
import json
import random
import signal
import time
import uuid

from confluent_kafka import Producer

from bank.models import Transaction, now_iso
from bank.seed import load
from config import settings

MERCHANTS = [
    ("Blue Bottle Coffee", "FOOD"), ("Whole Foods", "GROCERIES"),
    ("Shell", "FUEL"), ("Amazon", "RETAIL"), ("Uber", "TRANSPORT"),
    ("Netflix", "ENTERTAINMENT"), ("Apple Store", "ELECTRONICS"),
    ("IKEA", "HOME"), ("Delta Airlines", "TRAVEL"), ("CVS Pharmacy", "HEALTH"),
]
FOREIGN = [("BR", "Sao Paulo"), ("NG", "Lagos"), ("RU", "Moscow"),
           ("VN", "Hanoi"), ("MX", "Mexico City")]

# Matches the thresholds in flink/sql/fraud_detection.sql.
LARGE_TXN_FLOOR = 10_000


class Bank:
    def __init__(self, rng):
        self.rng = rng
        customers, accounts = load()
        self.customers = {c["customer_id"]: c for c in customers}
        self.accounts = accounts

    def _base(self, account, amount, country, city, merchant, category, txn_type):
        return Transaction(
            txn_id=str(uuid.uuid4()),
            account_id=account["account_id"],
            customer_id=account["customer_id"],
            amount=amount,
            currency=account["currency"],
            merchant=merchant,
            category=category,
            country=country,
            city=city,
            txn_type=txn_type,
        )

    def normal(self):
        acct = self.rng.choice(self.accounts)
        cust = self.customers[acct["customer_id"]]
        merchant, category = self.rng.choice(MERCHANTS)
        return [self._base(
            acct,
            round(self.rng.lognormvariate(3.2, 0.9), 2),  # mostly small, occasional spike
            cust["home_country"], cust["home_city"],
            merchant, category,
            self.rng.choices(["PURCHASE", "WITHDRAWAL", "TRANSFER", "DEPOSIT"],
                             weights=[70, 12, 10, 8])[0],
        )]

    def large_withdrawal(self):
        """Rule 1: one transaction above the reporting threshold."""
        acct = self.rng.choice(self.accounts)
        cust = self.customers[acct["customer_id"]]
        return [self._base(
            acct, round(self.rng.uniform(LARGE_TXN_FLOOR + 500, 85_000), 2),
            cust["home_country"], cust["home_city"],
            "Wire Transfer", "TRANSFER", "WITHDRAWAL",
        )]

    def velocity_burst(self):
        """Rule 2: a rapid run of charges on one account."""
        acct = self.rng.choice(self.accounts)
        cust = self.customers[acct["customer_id"]]
        merchant, category = self.rng.choice(MERCHANTS)
        return [
            self._base(acct, round(self.rng.uniform(20, 300), 2),
                       cust["home_country"], cust["home_city"],
                       merchant, category, "PURCHASE")
            for _ in range(self.rng.randint(7, 11))
        ]

    def impossible_travel(self):
        """Rule 3: same account charged in two countries moments apart."""
        acct = self.rng.choice(self.accounts)
        cust = self.customers[acct["customer_id"]]
        country, city = self.rng.choice(FOREIGN)
        merchant, category = self.rng.choice(MERCHANTS)
        return [
            self._base(acct, round(self.rng.uniform(15, 200), 2),
                       cust["home_country"], cust["home_city"],
                       merchant, category, "PURCHASE"),
            self._base(acct, round(self.rng.uniform(200, 3000), 2),
                       country, city, "Unknown Merchant", "RETAIL", "PURCHASE"),
        ]


def delivery_report(err, msg):
    if err is not None:
        print(f"delivery failed: {err}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rate", type=float, default=1.0, help="transactions per second")
    ap.add_argument("--fraud-rate", type=float, default=0.10,
                    help="probability each tick emits a fraud pattern instead")
    ap.add_argument("--seed", type=int, default=None)
    args = ap.parse_args()

    rng = random.Random(args.seed)
    bank = Bank(rng)
    producer = Producer({
        'bootstrap.servers': settings.KAFKA_BROKER,
        'linger.ms': 50,
    })

    running = True

    def stop(signum, frame):
        nonlocal running
        running = False

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    patterns = [bank.large_withdrawal, bank.velocity_burst, bank.impossible_travel]
    sent = 0
    interval = 1.0 / args.rate if args.rate > 0 else 0.1

    print(f"producing to {settings.BANK_TXN_TOPIC} at ~{args.rate}/s "
          f"({len(bank.accounts)} accounts, fraud rate {args.fraud_rate})")

    while running:
        if rng.random() < args.fraud_rate:
            batch = rng.choice(patterns)()
            tag = "FRAUD"
        else:
            batch = bank.normal()
            tag = "     "

        for txn in batch:
            producer.poll(0)
            producer.produce(
                settings.BANK_TXN_TOPIC,
                key=txn.account_id,
                value=json.dumps(txn.to_dict()),
                on_delivery=delivery_report,
            )
            sent += 1

        print(f"{tag} {batch[0].account_id} x{len(batch):<2} "
              f"{batch[0].merchant[:20]:<20} {batch[0].amount:>10,.2f} "
              f"{batch[0].country}  (total {sent})")

        time.sleep(interval)

    print(f"\nflushing {sent} transactions...")
    producer.flush(15)


if __name__ == "__main__":
    main()
