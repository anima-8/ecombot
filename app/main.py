from fastapi import FastAPI
from app.config import get_settings
import httpx
import logging
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from app.jobs import send_payment_reminders

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Роутеры
from app.endpoints.delivery import router as delivery_router
from app.endpoints.fulfilment import router as fulfilment_router
from app.endpoints.driver import router as driver_router
from app.endpoints.bitrix import router as bitrix_router
from app.endpoints.payments import router as payments_router

settings = get_settings()
app = FastAPI()

# Подключение роутеров
app.include_router(delivery_router, prefix=settings.API_PREFIX)
app.include_router(fulfilment_router, prefix=settings.API_PREFIX)
app.include_router(driver_router, prefix=settings.API_PREFIX)
app.include_router(bitrix_router, prefix=settings.API_PREFIX)
app.include_router(payments_router, prefix=settings.API_PREFIX)

# Установка Telegram webhook при запуске
@app.on_event("startup")
async def register_telegram_webhooks():
    async with httpx.AsyncClient() as client:
        base_url = "https://api.telegram.org/bot"

        bots = {
            "delivery": settings.TELEGRAM_DELIVERY_TOKEN,
            "fulfilment": settings.TELEGRAM_FULFILMENT_TOKEN,
            "driver": settings.TELEGRAM_DRIVER_TOKEN,
        }

        for name, token in bots.items():
            webhook_url = f"https://ecombot.ru{settings.API_PREFIX}/{name}"
            set_hook_url = f"{base_url}{token}/setWebhook"
            try:
                response = await client.get(set_hook_url, params={"url": webhook_url})
                logger.info("SetWebhook %s → %s", name, response.json())
            except Exception as e:
                print(f"[WEBHOOK ERROR] {name}: {e}")

scheduler = AsyncIOScheduler()
scheduler.add_job(send_payment_reminders, 'cron', hour=9, minute=0, id="daily_reminder_9am")
scheduler.start()
logger.info("Scheduled send_payment_reminders every day at 09:00")