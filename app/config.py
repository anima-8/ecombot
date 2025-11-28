from pydantic import BaseModel
from functools import lru_cache

class Settings(BaseModel):
    API_PREFIX: str = "/XXXXX"

    # Telegram Bot Tokens
    TELEGRAM_DELIVERY_TOKEN: str = "XXXXX"
    TELEGRAM_FULFILMENT_TOKEN: str = "XXXXX"
    TELEGRAM_DRIVER_TOKEN: str = "XXXXX"

    # Bitrix24
    BITRIX_WEBHOOK_URL: str = "https://XXXXX.bitrix24.ru/rest/1/XXXXX/"

    # MongoDB
    MONGODB_URI: str = "mongodb://XXXXX/?authSource=admin"

    # Dadata
    DADATA_TOKEN: str = "XXXXX"

    # Payments
    PAYKEEPER_TOKEN_URL: str = "https://ecomdelivery.server.paykeeper.ru/info/settings/token/"
    PAYKEEPER_INVOICE_URL: str = "https://ecomdelivery.server.paykeeper.ru/change/invoice/preview/"
    PAYKEEPER_USER: str = "payment"
    PAYKEEPER_PASSWORD: str = "XXXXX"
    PAYKEEPER_SECRET: str = "XXXXX"

@lru_cache()
def get_settings() -> Settings:
    return Settings()
