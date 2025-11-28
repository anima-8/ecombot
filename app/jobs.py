import datetime
from app.db import users_collection
import app.services as svc

async def send_payment_reminders():
    now = datetime.datetime.utcnow()
    cursor = users_collection.database["orders"].find({
        "status": "awaiting_payment",
        "invoice_url": {"$exists": True}
    })
    async for order in cursor:
        # можно добавить throttle, чтобы не спамить одной заявкой чаще одного раза в сутки:
        last = order.get("last_reminder_at")
        """ if last and (now - last).total_seconds() < 24*3600:
            continue """

        chat_id   = order["chat_id"]
        deal_id   = order["bitrix_deal_id"]
        url       = order["invoice_url"]
        text      = (
            f"⏰ *Напоминаем о необходимости оплаты заказа #{deal_id}.*\n"
            "Произведите, пожалуйста, оплату доставки:\n"
            f"{url}"
        )
        # бот для delivery / fulfilment берётся из order["type"]
        bot = svc.delivery_bot if order["type"]=="delivery" else svc.fulfilment_bot
        await bot.send_message(
            chat_id,
            text,
            parse_mode="Markdown",
            disable_web_page_preview=True
        )

        # и помечаем, что напомнили
        await users_collection.database["orders"].update_one(
            {"_id":order["_id"]},
            {"$set":{"last_reminder_at": now}}
        )
