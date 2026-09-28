from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    KAFKA_BROKER: str

    # Original demo producer/consumer.
    KAFKA_TOPIC: str
    KAFKA_GROUP_ID: str

    # Bank application.
    BANK_TXN_TOPIC: str = "bank.transactions"
    BANK_ALERTS_TOPIC: str = "bank.alerts"
    BANK_GROUP_ID: str = "bank-dashboard"

    # Seeded customers/accounts, shared by the producer and the dashboard.
    BANK_SEED_FILE: str = "bank/seed_data.json"

    DASHBOARD_HOST: str = "127.0.0.1"
    DASHBOARD_PORT: int = 8000


settings = Settings()
