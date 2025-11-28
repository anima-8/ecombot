# app/handlers/bitrix.py

import re
from app.db import users_collection
import app.services as svc
from bson import ObjectId
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from html import escape

bot_by_type = {
    "delivery": svc.delivery_bot,
    "fulfilment": svc.fulfilment_bot
}

async def handle_set_driver(params: dict):
    deal_id_raw = params.get("deal")
    driver_raw = params.get("driver", "")

    if not deal_id_raw or not driver_raw:
        return

    deal_id = deal_id_raw.replace("D_", "")
    order = await users_collection.database["orders"].find_one(
        {"bitrix_deal_id": deal_id}
    )
    if not order:
        return

    client_chat_id = order.get("chat_id")
    client = await users_collection.find_one({"chat_id": client_chat_id})
    client_username = client.get("username", "—")
    
    deal_type = order.get("type")
    bot = bot_by_type.get(deal_type)

    cargo_type = order.get("cargo_type")
    cargo_label = "Количество коробов" if cargo_type == "boxes" else "Количество палет"
    cargo_qty = order.get("cargo_quantity", 0)

    # Извлечение логина водителя
    match = re.search(r"tg:([a-zA-Z0-9_]+)", driver_raw)
    if not match:
        return
    driver_username = match.group(1)

    driver = await users_collection.find_one({"type": "driver", "username": driver_username})
    if not driver:
        return

    driver_chat_id = driver["chat_id"]

    # Формируем текст водителю
    text_to_driver = (
        f"<b>Поступила новая заявка #{deal_id} {escape(order.get('warehouse',''))}</b>\n"
        f"Клиент: {escape(order.get('org_name',''))}, "
        f"тел: {escape(order.get('phone_number',''))}, "
        f"tg: @{escape(client_username)}\n"
        f"Адрес забора поставки: {escape(order.get('pickup_address',''))}\n"
        f"Место сдачи поставки: {escape(order.get('warehouse',''))}\n"
        f"{cargo_label}: {cargo_qty}\n"
        f"Дата забора поставки: {svc.format_date(order.get('pickup_date'))}\n"
        f"Дата сдачи поставки: {svc.format_date(order.get('delivery_date'))}"
    )

    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Забрал", callback_data=f"got#{deal_id}")]
        ]
    )

    # 2) Отправляем и получаем объект Message
    message = await svc.driver_bot.send_message(
        chat_id=driver_chat_id,
        text=text_to_driver,
        parse_mode="HTML",
        reply_markup=keyboard
    )

    # 3) Достаём message_id
    message_id = getattr(message, "message_id", None)

    # Сохраняем в order
    await users_collection.database["orders"].update_one(
        {"_id": ObjectId(order["_id"])},
        {
            "$set": {
                "driver_mid": message_id,
                "driver_chat_id": driver_chat_id,
                "driver_username": driver_username
            }
        }
    )

    # Уведомление клиенту
    client_text = (
        f"*Изменился статус Вашей заявки #{deal_id}, {order.get('warehouse')}.*\n"
        f"Текущий статус: Обработано.\n"
        f"К Вам приедет водитель {clean_driver_info(driver_raw.strip())}"
    )

    message = await svc.send_text(
        client_chat_id,
        client_text,
        bot
    )
    message_id = getattr(message, "message_id", None)
    await users_collection.database["orders"].update_one(
        {"_id": ObjectId(order["_id"])},
        {
            "$set": {
                "user_driver_mid": message_id
            }
        }
    )

async def handle_change_driver(params: dict):
    deal_id_raw = params.get("deal")
    if not deal_id_raw:
        return

    deal_id = deal_id_raw.replace("D_", "")
    order = await users_collection.database["orders"].find_one({"bitrix_deal_id": deal_id})
    if not order:
        return
        
    deal_type = order.get("type")
    bot = bot_by_type.get(deal_type)

    # 1. Уведомление водителю
    driver_chat_id = order.get("driver_chat_id")
    if driver_chat_id:
        await svc.send_text(
            driver_chat_id,
            f"❗ *Внимание!* Заявка #{deal_id} была *отменена*.",
            svc.driver_bot
        )

    # 2. Изменить кнопку у водителя
    driver_mid = order.get("driver_mid")
    if driver_chat_id and driver_mid:
        from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
        new_keyboard = InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text="Заявка отменена", callback_data="null")]]
        )
        try:
            await svc.driver_bot.edit_message_reply_markup(
                chat_id=driver_chat_id,
                message_id=driver_mid,
                reply_markup=new_keyboard
            )
        except Exception as e:
            print(f"[edit_driver_message error] {e}")

    # 3. Удалить сообщение у клиента
    client_chat_id = order.get("chat_id")
    user_driver_mid = order.get("user_driver_mid")
    if client_chat_id and user_driver_mid:
        try:
            await bot.delete_message(
                chat_id=client_chat_id,
                message_id=user_driver_mid
            )
        except Exception as e:
            print(f"[delete_client_message error] {e}")

async def handle_payed(params: dict):
    deal_id_raw = params.get("deal")
    if not deal_id_raw:
        return

    deal_id = deal_id_raw.replace("D_", "")
    # 1) Обновляем статус в Mongo
    result = await users_collection.database["orders"].update_one(
        {"bitrix_deal_id": deal_id},
        {"$set": {"status": "payed"}}
    )
    if not result.modified_count:
        logger.warning("Order with deal %s not found or already payed", deal_id)
        return

    # 2) Берём ордер, чтобы достать chat_id, warehouse и type
    order = await users_collection.database["orders"].find_one(
        {"bitrix_deal_id": deal_id}
    )
    if not order:
        logger.error("Order %s updated but not found", deal_id)
        return

    client_chat_id = order.get("chat_id")
    warehouse      = order.get("warehouse", "—")
    deal_type      = order.get("type", "delivery")

    # 3) Выбираем нужный бот
    bot = svc.delivery_bot if deal_type == "delivery" else svc.fulfilment_bot

    # 4) Формируем и шлём сообщение
    text = (
        f"*Изменился статус Вашей заявки #{deal_id}, {warehouse}.*\n"
        f"*Текущий статус:* Доставлено.\n\n"
        "Спасибо, что обратились к нам. Удачных вам продаж! \n"
        "С уважением, команда Ecomdelivery."
    )
    keyboard = {
        "keyboard": [
            [{"text": "📦 Создать новую заявку"}],
            [{"text": "💰 Рассчитать стоимость"}]
        ],
        "resize_keyboard": True
    }
    try:
        await svc.send_text(
            client_chat_id,
            text,
            bot,
            keyboard
        )
        logger.info("Notified client %s about payed deal %s with keyboard", client_chat_id, deal_id)
    except Exception as e:
        logger.error("Failed to notify client about payed deal %s: %s", deal_id, e)

def clean_driver_info(text: str) -> str:
    return re.sub(r"\s*tg:[^\s]+", "", text).strip()