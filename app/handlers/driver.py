from aiogram.types import Update, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram import Bot
from app.config import get_settings
import logging
from datetime import datetime
from zoneinfo import ZoneInfo
import re
from app.db import users_collection
import app.services as svc
from bson import ObjectId
from httpx import AsyncClient
from app.handlers.decorators import on_callback, on_state, on_command

logger = logging.getLogger(__name__)
settings = get_settings()
driver_bot = Bot(token=settings.TELEGRAM_DRIVER_TOKEN)

async def handle_driver_start(update_data: dict):
    update = Update(**update_data)
    message = update.message

    if not message or not message.from_user:
        logger.warning("[DRIVER] no message or from_user")
        return

    user_data = {
        "chat_id": message.chat.id,
        "username": message.from_user.username,
        "first_name": message.from_user.first_name,
        "last_name": message.from_user.last_name,
        "type": "driver",
        "created_at": datetime.utcnow()
    }

    # Обновляем запись, если есть такая пара chat_id + type; иначе создаём
    await users_collection.update_one(
        {"chat_id": message.chat.id, "type": "driver"},
        {"$set": user_data},
        upsert=True
    )

    await driver_bot.send_message(
        chat_id=message.chat.id,
        text=f"Аккаунт @{message.from_user.username} успешно добавлен. Теперь новые заявки будут поступать в этот чат."
    )

@on_callback("got#")  # будет перехватывать все got#...
async def handle_driver_got(chat_id, user, callback_query):
    data = callback_query.get("data", "")
    match = re.match(r"got#(\d+)", data)
    if not match:
        return

    deal_id = match.group(1)

    # 1. Найти заказ
    order = await users_collection.database["orders"].find_one({"bitrix_deal_id": deal_id})
    if not order:
        return

    # 2. Изменить кнопку
    driver_mid = order.get("driver_mid")
    if driver_mid:
        keyboard = InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="Ожидание уточнения количества", callback_data="null")]
            ]
        )
        try:
            await svc.driver_bot.edit_message_reply_markup(
                chat_id=chat_id,
                message_id=driver_mid,
                reply_markup=keyboard
            )
        except Exception as e:
            print(f"[edit reply_markup] {e}")

    # 3. Переводим в стадию ожидания ввода количества
    cargo_type = order.get("cargo_type", "boxes")
    unit_label = "коробов" if cargo_type == "boxes" else "палет"

    await users_collection.update_one(
        {"chat_id": chat_id, "type": "driver"},
        {"$set": {
            "state": "awaiting_final_qty",
            "active_deal_id": deal_id
        }}
    )

    await svc.send_text(
        chat_id,
        f"Введите итоговое количество {unit_label} для заявки #{deal_id}:",
        svc.driver_bot
    )

async def handle_final_quantity_input(chat_id: int, user: dict, qty: int, deal_id: str):
    # 1. Найти заказ
    order = await users_collection.database["orders"].find_one({"bitrix_deal_id": deal_id})
    if not order:
        return

    orig_qty = order.get("cargo_quantity", 0)
    warehouse = order.get("warehouse", "")
    raw_type = order.get("cargo_type", "boxes")
    # Русская метка для расчёта
    cargo_label_ru = "Короба" if raw_type == "boxes" else "Палеты"
    unit_label = "коробов" if raw_type == "boxes" else "палет"
    client_chat_id = order.get("chat_id")
    driver_mid = order.get("driver_mid")
    client_summ_mid = order.get("summ_mid")
    deal_type = order.get("type")  # "delivery" или "fulfilment"
    delivery_str = svc.format_date(order.get("delivery_date"))
    pickup_str   = svc.format_date(order.get("pickup_date"))

    # Функция для финального шага (общая)
    async def finalize():
        # 4. Финальное уведомление клиенту
        final_text = (
            f"*Изменился статус Вашей заявки #{deal_id}, {warehouse}.*\n"
            f"Текущий статус: Принято водителем.\n"
            f"Фактическое количество {unit_label}: {qty}\n"
            f"Итоговая стоимость доставки: {new_cost} ₽"
        )
        bot = svc.delivery_bot if deal_type == "delivery" else svc.fulfilment_bot
        await bot.send_message(chat_id=client_chat_id, text=final_text, parse_mode="Markdown")

        # 5. Кнопка водителю — "Упаковывается"
        packing_kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="Упаковывается", callback_data=f"packing#{deal_id}")]
            ]
        )
        try:
            await svc.driver_bot.edit_message_reply_markup(
                chat_id=chat_id,
                message_id=driver_mid,
                reply_markup=packing_kb
            )
        except Exception as e:
            logger.error("Не удалось обновить кнопку водителю: %s", e)
            
        await svc.driver_bot.send_message(
            chat_id=chat_id,
            text=f"Данные по заказу #{deal_id} успешно обновлены."
        )

        # 6. Перевести сделку в C2:PREPAYMENT_INVOICE
        async with AsyncClient() as client:
            await client.post(
                f"{settings.BITRIX_WEBHOOK_URL}crm.deal.update",
                json={
                    "id": deal_id,
                    "fields": {"STAGE_ID": "C2:PREPAYMENT_INVOICE"}
                }
            )

        # Сброс состояния водителя
        await users_collection.update_one(
            {"chat_id": chat_id, "type": "driver"},
            {"$set": {"state": None, "active_deal_id": None}}
        )

    # Сценарий 1: количество не изменилось
    if qty == orig_qty:
        new_cost = order.get("delivery_cost", 0)
        await finalize()
        return

    # Сценарий 2: количество изменилось
    # 1) Пересчёт стоимости
    if deal_type == "fulfilment":
        new_cost = svc.calculate_delivery_cost_fulfilment(warehouse, cargo_label_ru, qty)
    else:
        new_cost = svc.calculate_delivery_cost(warehouse, cargo_label_ru, qty)

    # Обновляем ордер в БД
    await users_collection.database["orders"].update_one(
        {"_id": ObjectId(order["_id"])},
        {"$set": {"cargo_quantity": qty, "delivery_cost": new_cost}}
    )

    # 2) Обновляем поля сделки в Битрикс
    async with AsyncClient() as client:
        await client.post(
            f"{settings.BITRIX_WEBHOOK_URL}crm.deal.update",
            json={
                "id": deal_id,
                "fields": {
                    "UF_CRM_1724923582938": qty,
                    "OPPORTUNITY": new_cost
                }
            }
        )

     # 3) Пересобираем и правим суммари водителя
    order = await users_collection.database["orders"].find_one({"bitrix_deal_id": deal_id})
    new_text, new_kb = await render_driver_message(order)
    try:
        await svc.driver_bot.edit_message_text(
            chat_id=order["driver_chat_id"],
            message_id=order["driver_mid"],
            text=new_text,
            reply_markup=new_kb,
            parse_mode="Markdown"
        )
    except Exception as e:
        logger.warning("Не удалось обновить первое сообщение водителю: %s", e)

    # 4) Пересобираем и правим суммари клиента
    bot = svc.delivery_bot if deal_type == "delivery" else svc.fulfilment_bot
    parts = [
        "✅ Ваша заявка успешно отправлена!",
        "📋 Содержимое заявки:",
        "",
        f"🆔 Номер заявки: #{deal_id}",
        f"🏢 ИП / организация: {order.get('org_name', '—')}",
        f"📍 Адрес ИП / организации: {order.get('org_address', '—')}",
        f"🏦 БИК: {order.get('bik', '—')}",
        f"💳 Р/С: {order.get('rs', '—')}",
        f"📦 Тип поставки: {cargo_label_ru}",
        f"🔢 Количество: {qty}",                # <-- обновлено
        f"🏬 Склад: {warehouse}",
        f"📅 Дата сдачи: {delivery_str}",
        f"🚚 Дата забора: {pickup_str}",
        f"🏠 Адрес забора: {order.get('pickup_address', '—')}",
        f"📞 Телефон: {order.get('phone_number', '—')}",
        f"💰 Стоимость: {new_cost} ₽"           # <-- обновлено
    ]
    new_summary = "\n".join(parts)
    try:
        await bot.edit_message_text(
            chat_id=client_chat_id,
            message_id=client_summ_mid,
            text=new_summary,
            parse_mode="Markdown"
        )
    except Exception as e:
        logger.error("Не удалось обновить суммари клиента: %s", e)

    # Переходим к финалу
    await finalize()

@on_callback("packing#")
async def handle_packing(chat_id: int, user: dict, callback_query: dict):
    data = callback_query.get("data", "")
    m = re.match(r"packing#(\d+)", data)
    if not m:
        return
    deal_id = m.group(1)

    # 1. Перевести сделку в C2:EXECUTING
    async with AsyncClient() as client:
        try:
            await client.post(
                f"{settings.BITRIX_WEBHOOK_URL}crm.deal.update",
                json={"id": deal_id, "fields": {"STAGE_ID": "C2:EXECUTING"}}
            )
        except Exception as e:
            logger.error("Bitrix update error: %s", e)

    # 2. Сменить кнопку у водителя
    order = await users_collection.database["orders"].find_one({"bitrix_deal_id": deal_id})
    driver_mid = order.get("driver_mid")
    if driver_mid:
        kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="В доставке", callback_data=f"delivering#{deal_id}")]
            ]
        )
        try:
            await svc.driver_bot.edit_message_reply_markup(
                chat_id=chat_id,
                message_id=driver_mid,
                reply_markup=kb
            )
        except Exception as e:
            logger.warning("Cannot update driver button: %s", e)

    # 3. Уведомить клиента
    client_chat_id = order.get("chat_id")
    warehouse = order.get("warehouse", "—")
    deal_type = order.get("type", "delivery")
    bot = svc.delivery_bot if deal_type == "delivery" else svc.fulfilment_bot

    text = (
        f"*Изменился статус Вашей заявки #{deal_id}, {warehouse}.*\n"
        f"Текущий статус: Упаковывается."
    )
    try:
        await bot.send_message(chat_id=client_chat_id, text=text, parse_mode="Markdown")
    except Exception as e:
        logger.error("Cannot notify client: %s", e)

@on_callback("delivering#")
async def handle_delivering(chat_id: int, user: dict, callback_query: dict):
    data = callback_query.get("data", "")
    m = re.match(r"delivering#(\d+)", data)
    if not m:
        return
    deal_id = m.group(1)

    # 1. Переводим сделку в C2:FINAL_INVOICE
    async with AsyncClient() as client:
        try:
            await client.post(
                f"{settings.BITRIX_WEBHOOK_URL}crm.deal.update",
                json={"id": deal_id, "fields": {"STAGE_ID": "C2:FINAL_INVOICE"}}
            )
        except Exception as e:
            logger.error("Bitrix update error: %s", e)

    # 2. Сменить кнопку у водителя на "Доставлено"
    order = await users_collection.database["orders"].find_one({"bitrix_deal_id": deal_id})
    driver_mid = order.get("driver_mid")
    if driver_mid:
        kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="Доставлено", callback_data=f"delivered#{deal_id}")]
            ]
        )
        try:
            await svc.driver_bot.edit_message_reply_markup(
                chat_id=chat_id,
                message_id=driver_mid,
                reply_markup=kb
            )
        except Exception as e:
            logger.warning("Cannot update driver button to delivered: %s", e)

    # 3. Уведомить клиента
    client_chat_id = order.get("chat_id")
    warehouse = order.get("warehouse", "—")
    deal_type = order.get("type", "delivery")
    bot = svc.delivery_bot if deal_type == "delivery" else svc.fulfilment_bot

    text = (
        f"*Изменился статус Вашей заявки #{deal_id}, {warehouse}.*\n"
        f"Текущий статус: В доставке.\n\n"
        f"Проверьте правильность оформления поставки в личном кабинете WB:\n"
        f"    1. Статус поставки - \"Отгрузка разрешена\".\n"
        f"    2. В пропуске для водителя количество коробов должно соответствовать фактическому."
    )
    try:
        await bot.send_message(chat_id=client_chat_id, text=text, parse_mode="Markdown")
    except Exception as e:
        logger.error("Cannot notify client about delivering: %s", e)

@on_callback("delivered#")
async def handle_driver_delivered(chat_id: int, user: dict, callback_query: dict):
    data = callback_query.get("data", "")
    m = re.match(r"delivered#(\d+)", data)
    if not m:
        return
    deal_id = m.group(1)

    # 1) Ставим водителя в режим awaiting_gate и сохраняем deal_id
    await users_collection.update_one(
        {"chat_id": chat_id, "type": "driver"},
        {"$set": {"state": "awaiting_gate", "active_deal_id": deal_id}}
    )

    # 2) Меняем кнопку в первом сообщении на "Завершено"
    order = await users_collection.database["orders"].find_one({"bitrix_deal_id": deal_id})
    driver_mid = order.get("driver_mid")
    if driver_mid:
        kb = InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text="Ожидание ввода ворот", callback_data="null")]]
        )
        try:
            await svc.driver_bot.edit_message_reply_markup(
                chat_id=chat_id,
                message_id=driver_mid,
                reply_markup=kb
            )
        except Exception as e:
            logger.warning("Не удалось обновить кнопку водителю после доставки: %s", e)

    # 3) Спрашиваем номер ворот
    await svc.send_text(
        chat_id,
        f"Для завершения заявки #{deal_id} введите номер ворот:",
        svc.driver_bot
    )

@on_state("awaiting_gate")
async def handle_gate_input(chat_id: int, user: dict, text: str):
    deal_id = user.get("active_deal_id")
    if not deal_id:
        return

    gate_number = text.strip()

    # 1) Обновляем сделку в Битрикс
    now_msk = datetime.now(ZoneInfo("Europe/Moscow"))
    # Поле времени в формате ISO (без зоны) и в человекочитаемом для клиента
    iso_time = now_msk.strftime("%Y-%m-%dT%H:%M:%S")
    display_time = now_msk.strftime("%d.%m.%Y %H:%M")

    async with AsyncClient() as client:
        try:
            await client.post(
                f"{settings.BITRIX_WEBHOOK_URL}crm.deal.update",
                json={
                    "id": deal_id,
                    "fields": {
                        "STAGE_ID": "C2:UC_1E3Z8W",
                        "UF_CRM_1724923710659": gate_number,
                        "UF_CRM_1724923678625": iso_time
                    }
                }
            )
        except Exception as e:
            logger.error("Ошибка обновления сделки в Bitrix при вводе ворот: %s", e)

    # 2) В кнопке водителю уже стоит null, ничего не правим (можно убрать клавиатуру)
    # Опционально: можно удалить inline-клавиатуру
    order = await users_collection.database["orders"].find_one({"bitrix_deal_id": deal_id})
    driver_mid = order.get("driver_mid")
    if driver_mid:
        kb = InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text="Завершено", callback_data="null")]]
        )
        try:
            await svc.driver_bot.edit_message_reply_markup(
                chat_id=chat_id,
                message_id=driver_mid,
                reply_markup=kb
            )
        except Exception as e:
            logger.warning("Не удалось обновить кнопку водителю после доставки: %s", e)

    # 3) Шлём водителю финальное уведомление
    await svc.send_text(
        chat_id,
        f"Заявка #{deal_id} успешно завершена.",
        svc.driver_bot
    )

    # 4) Уведомляем клиента
    warehouse = order.get("warehouse", "—")
    deal_type = order.get("type", "delivery")
    bot = svc.delivery_bot if deal_type == "delivery" else svc.fulfilment_bot

    client_text = (
        f"*Изменился статус Вашей заявки #{deal_id}, {warehouse}.*\n"
        f"Текущий статус: Доставлено.\n"
        f"Номер ворот: {gate_number}\n"
        f"Время сдачи груза: {display_time}"
    )
    try:
        await bot.send_message(chat_id=order["chat_id"], text=client_text, parse_mode="Markdown")
    except Exception as e:
        logger.error("Не удалось уведомить клиента о завершении заявки: %s", e)

    # 5) Сбрасываем состояние водителя
    await users_collection.update_one(
        {"chat_id": chat_id, "type": "driver"},
        {"$set": {"state": None, "active_deal_id": None}}
    )

    # 6) Формируем услугу в сделке
    service_name = await svc.set_deal_service_row(deal_id)

    # 7) Переводим ордер в статус awaiting_payment
    update_fields = {"status": "awaiting_payment", "service_name": service_name}

    # 8) Если это fulfilment — генерим счёт и даём ссылку
    order = await users_collection.database["orders"].find_one(
        {"bitrix_deal_id": deal_id}
    )
    deal_type = order.get("type", "delivery")
    if deal_type == "fulfilment":
        url_public = await svc.generate_deal_invoice_public_url(deal_id)
        update_fields["invoice_url"] = url_public
        update_fields["payment_type"] = 'invoice'

    # 9) Сохраняем всё в ордере
    await users_collection.database["orders"].update_one(
        {"bitrix_deal_id": deal_id},
        {"$set": update_fields}
    )

    # 10) Если был fulfilment — шлём клиенту ссылку на счёт
    if deal_type == "fulfilment":
        client_chat_id = order.get("chat_id")
        await svc.fulfilment_bot.send_message(
            chat_id=client_chat_id,
            text=f"📄 Ваш счёт готов и доступен для скачивания:\n{url_public}"
        )

    else:
        # 10) Для delivery просим выбрать способ оплаты
        client_chat_id = order.get("chat_id")
        pay_keyboard = {
            "keyboard": [
                [{"text": "Оплатить по СБП"}, {"text": "Оплатить по счету"}]
            ],
            "resize_keyboard": True
        }
        await svc.send_text(
            client_chat_id,
            "💳 Пожалуйста, выберите способ оплаты:",
            svc.delivery_bot,
            pay_keyboard
        )

async def render_driver_message(order: dict) -> tuple[str, InlineKeyboardMarkup]:
    deal_id    = order["bitrix_deal_id"]
    warehouse  = order["warehouse"]
    pickup     = order["pickup_address"]
    cargo_qty  = order["cargo_quantity"]
    cargo_ru   = "коробов" if order["cargo_type"] == "boxes" else "палет"
    client_tel = order["phone_number"]

    # <-- вот здесь берём тип из заказа
    order_type = order.get("type", "delivery")
    client = await users_collection.find_one({
        "chat_id": order["chat_id"],
        "type":    order_type
    })
    client_username = client.get("username", "—")

    text = (
        f"*Поступила новая заявка #{deal_id} {warehouse}*\n"
        f"Клиент: {order['org_name']}, тел: {client_tel}, tg: @{client_username}\n"
        f"Адрес забора поставки: {pickup}\n"
        f"Место сдачи поставки: {warehouse}\n"
        f"Количество {cargo_ru}: {cargo_qty}\n"
        f"Дата забора поставки: {svc.format_date(order['pickup_date'])}\n"
        f"Дата сдачи поставки: {svc.format_date(order['delivery_date'])}"
    )
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Упаковывается", callback_data=f"packing#{deal_id}")]
        ]
    )
    return text, kb
