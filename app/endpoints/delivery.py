# app/endpoints/delivery.py

from fastapi import APIRouter, Request
import logging

# Импортируем, чтобы зарегистрировать все @on_command и @on_state из handlers/delivery.py
import app.handlers.delivery
import app.handlers.delivery_calc
from app.db import users_collection
from app.handlers.decorators import COMMAND_HANDLERS, STATE_HANDLERS

logger = logging.getLogger(__name__)
router = APIRouter()

logger.info("Registered delivery commands: %s", COMMAND_HANDLERS.get("delivery", {}).keys())

@router.post("/delivery")
async def delivery_webhook(request: Request):
    data = await request.json()
    logger.info("[DELIVERY] incoming: %s", data)

    message = data.get("message", {})  
    text    = message.get("text", "")
    contact = message.get("contact")
    chat    = message.get("chat", {})
    chat_id = chat.get("id")
    if not chat_id:
        return {"ok": False, "reason": "no chat_id"}
    if not text and not contact:
        return {"ok": False, "reason": "no text or contact"}

    text = text.strip()
    # Ищем пользователя и его текущее состояние
    user  = await users_collection.find_one({"chat_id": chat_id, "type": "delivery"})
    state = user.get("state") if user else None
    bot_type = "delivery"

    logger.info("Dispatching delivery: text=%r, contact=%s, state=%r", text, bool(contact), state)

    # Сначала пробуем команду
    cmd_handler = COMMAND_HANDLERS.get(bot_type, {}).get(text)
    if cmd_handler:
        await cmd_handler(chat_id, user, message)
    else:
        # затем — по состоянию
        st_handler = STATE_HANDLERS.get(bot_type, {}).get(state)
        if st_handler:
            # иногда нужно передавать полный message, а не text
            if state == "enter_phone_number":
                await st_handler(chat_id, user, message)
            else:
                await st_handler(chat_id, user, text)
        else:
            logger.info("No handler for delivery: cmd=%r state=%r", text, state)

    return {"ok": True}
