from fastapi import APIRouter, Request
import logging

import app.handlers.driver  # Регистрируем хендлеры
from app.db import users_collection
from app.handlers.decorators import (
    COMMAND_HANDLERS,
    STATE_HANDLERS,
    CALLBACK_HANDLERS,
    CALLBACK_PREFIXES
)
import app.services as svc  # для send_text

logger = logging.getLogger(__name__)
router = APIRouter()

@router.post("/driver")
async def driver_webhook(request: Request):
    data = await request.json()
    logger.info("[DRIVER] incoming: %s", data)

    bot_type = "driver"

    # === обработка callback-кнопок ===
    callback = data.get("callback_query")
    if callback:
        chat_id = callback["from"]["id"]
        data_text = callback.get("data", "").strip()

        user = await users_collection.find_one({"chat_id": chat_id, "type": bot_type})
        state = user.get("state") if user else None

        # 🔒 если водитель в ожидании ворот — блокируем все коллбеки
        if state == "awaiting_gate":
            deal_id = user.get("active_deal_id")
            await svc.send_text(
                chat_id,
                f"Для завершения заявки #{deal_id} введите номер ворот:",
                svc.driver_bot
            )
            return {"ok": True}

        # 🔒 Если водитель в ожидании qty — блокируем любые коллбеки
        if state == "awaiting_final_qty":
            deal_id = user.get("active_deal_id")
            order = await users_collection.database["orders"].find_one({"bitrix_deal_id": deal_id})
            cargo_type = order.get("cargo_type", "boxes")
            unit_label = "коробов" if cargo_type == "boxes" else "палет"
            await svc.send_text(
                chat_id,
                f"❗ Введите итоговое количество {unit_label} для заявки #{deal_id} (целое число)",
                svc.driver_bot
            )
            return {"ok": True}

        handler = CALLBACK_HANDLERS.get(bot_type, {}).get(data_text)
        handler = CALLBACK_HANDLERS.get(bot_type, {}).get(data_text)

        if handler is None:
            for prefix, h in CALLBACK_PREFIXES.get(bot_type, []):
                if data_text.startswith(prefix):
                    handler = h
                    break
        if handler:
            await handler(chat_id, user, callback)
            return {"ok": True}
        else:
            logger.info("No callback handler for: %s", data_text)
            return {"ok": False, "reason": "no handler"}

    # === обычное сообщение ===
    message = data.get("message")
    if message:
        chat_id = message.get("chat", {}).get("id")
        if not chat_id:
            return {"ok": False, "reason": "no chat_id"}

        text = message.get("text", "").strip()
        if text == "/start":
            from datetime import datetime

            first_name = message.get("from", {}).get("first_name")
            last_name = message.get("from", {}).get("last_name")
            username = message.get("from", {}).get("username")

            existing_user = await users_collection.find_one({"chat_id": chat_id, "type": bot_type})
            if not existing_user:
                await users_collection.insert_one({
                    "type": bot_type,
                    "chat_id": chat_id,
                    "created_at": datetime.utcnow(),
                    "first_name": first_name,
                    "last_name": last_name,
                    "username": username,
                })

            await svc.send_text(
                chat_id,
                "✅ Ваш аккаунт успешно добавлен. Теперь заявки будут поступать в этот чат.",
                svc.driver_bot
            )
            return {"ok": True}
        user = await users_collection.find_one({"chat_id": chat_id, "type": bot_type})
        state = user.get("state") if user else None

        if state == "awaiting_gate":
            from app.handlers.driver import handle_gate_input
            await handle_gate_input(chat_id, user, text)
            return {"ok": True}

        # 🔒 Если водитель в ожидании qty — принимаем только числа
        if state == "awaiting_final_qty":
            deal_id = user.get("active_deal_id")
            if not text.isdigit():
                order = await users_collection.database["orders"].find_one({"bitrix_deal_id": deal_id})
                cargo_type = order.get("cargo_type", "boxes")
                unit_label = "коробов" if cargo_type == "boxes" else "палет"
                await svc.send_text(
                    chat_id,
                    f"❗ Введите итоговое количество {unit_label} для заявки #{deal_id} (целое число)",
                    svc.driver_bot
                )
                return {"ok": False, "reason": "not a number"}

            # всё валидно — передаём qty в хендлер
            from app.handlers.driver import handle_final_quantity_input
            await handle_final_quantity_input(chat_id, user, int(text), deal_id)
            return {"ok": True}

        # Обычные команды
        cmd_handler = COMMAND_HANDLERS.get(bot_type, {}).get(text)
        if cmd_handler:
            await cmd_handler(chat_id, user, message)
        else:
            st_handler = STATE_HANDLERS.get(bot_type, {}).get(state)
            if st_handler:
                await st_handler(chat_id, user, text)
            else:
                logger.info("No handler for driver state: %s", state)

    return {"ok": True}
