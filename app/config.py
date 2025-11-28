from pydantic import BaseModel
from functools import lru_cache

class Settings(BaseModel):
    API_PREFIX: str = "/sd7sw1evri9QceF"

    # Telegram Bot Tokens
    TELEGRAM_DELIVERY_TOKEN: str = "7288574878:AAGnjA8Vt9TLanNowkMm9hS_JlemnXVN6Ak"
    TELEGRAM_FULFILMENT_TOKEN: str = "7719633888:AAE9RomasEvZ4V6azv5aQcLHZWQoY8sn51M"
    TELEGRAM_DRIVER_TOKEN: str = "7666157948:AAHMVgkFoEcH3f794WmMGMIekU0B4Z-YxoE"

    # Bitrix24
    BITRIX_WEBHOOK_URL: str = "https://b24-t7zrgf.bitrix24.ru/rest/1/gdyamql0edvxgwsk/"

    # MongoDB
    MONGODB_URI: str = "mongodb://ecom:s8d5%26sDrB1@158.255.0.47:27017/?authSource=admin"

    # Dadata
    DADATA_TOKEN: str = "59f116a7c8bc520577c6d7781bba49e58e72d58e"

    # Payments
    PAYKEEPER_TOKEN_URL: str = "https://ecomdelivery.server.paykeeper.ru/info/settings/token/"
    PAYKEEPER_INVOICE_URL: str = "https://ecomdelivery.server.paykeeper.ru/change/invoice/preview/"
    PAYKEEPER_USER: str = "payment"
    PAYKEEPER_PASSWORD: str = "d7dRE9b6Yw"
    PAYKEEPER_SECRET: str = "EGCYUEds0ts_21hDDvO"

@lru_cache()
def get_settings() -> Settings:
    return Settings()
