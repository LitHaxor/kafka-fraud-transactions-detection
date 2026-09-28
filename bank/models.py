"""Domain objects for the demo bank."""

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from decimal import Decimal


def now_iso() -> str:
    """Flink's JSON format reads TIMESTAMP(3) as 'yyyy-MM-ddTHH:mm:ss.SSS'."""
    return datetime.now(timezone.utc).replace(tzinfo=None).isoformat(timespec='milliseconds')


@dataclass
class Customer:
    customer_id: str
    name: str
    email: str
    home_country: str
    home_city: str
    risk_band: str  # LOW | MEDIUM | HIGH

    def to_dict(self):
        return asdict(self)


@dataclass
class Account:
    account_id: str
    customer_id: str
    account_type: str  # CHECKING | SAVINGS
    currency: str
    opening_balance: float

    def to_dict(self):
        return asdict(self)


@dataclass
class Transaction:
    txn_id: str
    account_id: str
    customer_id: str
    amount: float
    currency: str
    merchant: str
    category: str
    country: str
    city: str
    txn_type: str  # PURCHASE | WITHDRAWAL | TRANSFER | DEPOSIT
    event_time: str = field(default_factory=now_iso)

    def to_dict(self):
        return asdict(self)
