"""Generate the dummy customers and accounts the demo runs on.

    python -m bank.seed            # writes bank/seed_data.json
    python -m bank.seed --show     # print what was generated
"""

import argparse
import json
import random

from faker import Faker

from bank.models import Account, Customer
from config import settings

SEED = 42  # fixed so every run of the demo uses the same people

HOME_LOCATIONS = [
    ("US", "New York"), ("US", "Chicago"), ("GB", "London"),
    ("DE", "Berlin"), ("FR", "Paris"), ("SG", "Singapore"),
]
CURRENCY_BY_COUNTRY = {"US": "USD", "GB": "GBP", "DE": "EUR", "FR": "EUR", "SG": "SGD"}


def generate(n_customers: int = 12):
    fake = Faker()
    Faker.seed(SEED)
    rng = random.Random(SEED)

    customers, accounts = [], []

    for i in range(n_customers):
        country, city = HOME_LOCATIONS[i % len(HOME_LOCATIONS)]
        cust = Customer(
            customer_id=f"CUST-{1000 + i}",
            name=fake.name(),
            email=fake.email(),
            home_country=country,
            home_city=city,
            # A few high-risk customers make the fraud rules more interesting.
            risk_band=rng.choices(["LOW", "MEDIUM", "HIGH"], weights=[6, 3, 1])[0],
        )
        customers.append(cust)

        for j in range(rng.randint(1, 2)):
            accounts.append(Account(
                account_id=f"ACC-{7000 + len(accounts)}",
                customer_id=cust.customer_id,
                account_type="CHECKING" if j == 0 else "SAVINGS",
                currency=CURRENCY_BY_COUNTRY[country],
                opening_balance=round(rng.uniform(500, 25000), 2),
            ))

    return customers, accounts


def load():
    """Read the seed file; used by the producer and the dashboard."""
    with open(settings.BANK_SEED_FILE) as fh:
        data = json.load(fh)
    return data["customers"], data["accounts"]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--customers", type=int, default=12)
    ap.add_argument("--show", action="store_true")
    args = ap.parse_args()

    customers, accounts = generate(args.customers)
    payload = {
        "customers": [c.to_dict() for c in customers],
        "accounts": [a.to_dict() for a in accounts],
    }
    with open(settings.BANK_SEED_FILE, "w") as fh:
        json.dump(payload, fh, indent=2)

    print(f"wrote {settings.BANK_SEED_FILE}: "
          f"{len(customers)} customers, {len(accounts)} accounts")

    if args.show:
        for c in customers:
            owned = [a for a in accounts if a.customer_id == c.customer_id]
            print(f"  {c.customer_id}  {c.name:<24} {c.home_city:<10} risk={c.risk_band:<6} "
                  f"accounts={[a.account_id for a in owned]}")


if __name__ == "__main__":
    main()
