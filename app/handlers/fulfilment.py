import re
import logging
from app.config import get_settings
from httpx import AsyncClient
from datetime import datetime
from bson import ObjectId
from app.handlers.decorators import on_command, on_state
from app.db import users_collection
import app.services as svc
from aiogram.enums.chat_action import ChatAction

settings = get_settings()
logger = logging.getLogger(__name__)

@on_command("/start")
@on_command("🔄 Начать заново")
async def handle_fulfilment_start(chat_id, user, message):
    # если такого пользователя ещё нет — создаём
    if not user:
        await users_collection.insert_one({
            "chat_id": chat_id,
            "username": message["chat"].get("username"),
            "first_name": message["chat"].get("first_name"),
            "last_name": message["chat"].get("last_name"),
            "type": "fulfilment",
            "created_at": datetime.utcnow(),
            "active_order": None
        })
    # сбрасываем состояние и текущий заказ
    await users_collection.update_one(
        {"chat_id": chat_id, "type": "fulfilment"},
        {"$set": {"state": "start", "active_order": None}}
    )
    # отсылаем вводное сообщение
    await svc.send_intro_message_ff(chat_id)

@on_state("start")
async def handle_start_state(chat_id, user, text):
    # если на стадии "start" пришло не то, что нужно — повторяем кнопку
    keyboard = {
        "keyboard": [[{"text": "Создать новую заявку"}]],
        "resize_keyboard": True
    }
    await svc.send_text(
        chat_id,
        "Нажмите кнопку «Создать новую заявку», чтобы начать",
        svc.fulfilment_bot,
        keyboard
    )

@on_command("Создать новую заявку")
async def handle_create_application(chat_id, user, message):
    # 1) Смотрим, есть ли у юзера предыдущие заказы и собираем уникальные org_name
    cursor = users_collection.database["orders"].find({"chat_id": chat_id})
    orgs = set()
    async for order in cursor:
        if (order.get("inn")
            and order.get("org_name")
            and order.get("org_address")
            and order.get("rs")
            and order.get("bik")):
            orgs.add(order["org_name"])

    if orgs:
        # 2a) Есть организации — предлагаем выбрать или ввести ИНН
        buttons = [[{"text": n} ] for n in sorted(orgs)]
        buttons.append([{"text": "🔄 Начать заново"}])
        keyboard = {"keyboard": buttons, "resize_keyboard": True}

        await users_collection.update_one(
            {"chat_id": chat_id, "type": "fulfilment"},
            {"$set": {"state": "select_existing_org"}}
        )
        await svc.send_text(
            chat_id,
            "Выберите организацию из списка или введите ИНН компании",
            svc.fulfilment_bot,
            keyboard
        )

    else:
        # 2b) Нет — сразу просим ИНН
        await users_collection.update_one(
            {"chat_id": chat_id, "type": "fulfilment"},
            {"$set": {"state": "awaiting_inn"}}
        )
        await svc.send_text(chat_id, "Введите ИНН компании", svc.fulfilment_bot)

@on_state("awaiting_inn")
async def handle_inn_input(chat_id, user, text):
    inn = text

    # 1) Запрос к Dadata
    async with AsyncClient() as client:
        resp = await client.post(
            "https://suggestions.dadata.ru/suggestions/api/4_1/rs/findById/party",
            headers={
                "Content-Type": "application/json",
                "Accept": "application/json",
                "Authorization": f"Token {settings.DADATA_TOKEN}"
            },
            json={"query": inn}
        )
    data = resp.json()
    suggestions = data.get("suggestions", [])

    # 2) Если не нашли — остаёмся в той же стадии
    if not suggestions:
        await svc.send_text(chat_id, "❌ Организация не найдена. Проверьте ИНН.", svc.fulfilment_bot)
        return

    # 3) Берём первую подсказку
    item = suggestions[0]
    org_name    = item["data"]["name"]["full_with_opf"]
    addr_obj    = item["data"].get("address")
    org_address = addr_obj["value"] if addr_obj else "— адрес не указан —"

    # 4) Вставляем «плоский» заказ в коллекцию orders
    order_doc = {
        "chat_id":       chat_id,
        "inn":           inn,
        "org_name":      org_name,
        "org_address":   org_address,
        "created_at":    datetime.utcnow(),
        "status":        "in_progress",
        "type":          "fulfilment",
        "is_active":     True
    }
    res = await users_collection.database["orders"].insert_one(order_doc)
    order_id = str(res.inserted_id)

    # 5) Сохраняем в профиле пользователя активный заказ и переводим в confirm_inn
    await users_collection.update_one(
        {"chat_id": chat_id, "type": "fulfilment"},
        {"$set": {"active_order": order_id, "state": "confirm_inn"}}
    )

    # 6) Спрашиваем подтверждение
    keyboard = {
        "keyboard": [
            [{"text": "✅ Продолжить"}, {"text": "❌ Ввести другой ИНН"}],
            [{"text": "🔄 Начать заново"}]
        ],
        "resize_keyboard": True
    }
    await svc.send_text(
        chat_id,
        f"✅ Найдена организация:\n{org_name}\nАдрес: {org_address}",
        svc.fulfilment_bot,
        keyboard
    )

@on_state("select_existing_org")
async def handle_select_existing_org(chat_id, user, text):
    # Если пользователь ввёл цифры — это ИНН, переходим к вводу ИНН
    if text.isdigit():
        # Передаём текущего user, но его при этом можно обновить:
        user = await users_collection.find_one({"chat_id": chat_id, "type": "fulfilment"})
        await handle_inn_input(chat_id, user, text)
        return

    # Иначе это выбор существующей организации по названию
    org_name = text

    # 2) Ищем последний полностью заполненный заказ с таким org_name
    last_order = await users_collection.database["orders"].find_one(
        {
            "chat_id": chat_id,
            "org_name": org_name,
            "inn": {"$exists": True},
            "org_address": {"$exists": True},
            "rs": {"$exists": True},
            "bik": {"$exists": True}
        },
        sort=[("created_at", -1)]
    )
    if not last_order:
        # Не нашли — просим ввести ИНН
        await svc.send_text(
            chat_id,
            "Не удалось найти полностью заполнённую организацию. Введите ИНН компании",
            svc.fulfilment_bot,
            {"keyboard":[[{"text":"🔄 Начать заново"}]], "resize_keyboard": True}
        )
        # Остаёмся в той же стадии
        await users_collection.update_one(
            {"chat_id": chat_id, "type": "fulfilment"},
            {"$set": {"state": "select_existing_org"}}
        )
        return

    # 3) Копируем все поля из найденного заказа
    order_doc = {
        "chat_id":      chat_id,
        "inn":          last_order["inn"],
        "org_name":     last_order["org_name"],
        "org_address":  last_order["org_address"],
        "rs":           last_order["rs"],
        "bik":          last_order["bik"],
        "created_at":   datetime.utcnow(),
        "status":       "in_progress",
        "type":         "fulfilment",
        "is_active":    True
    }
    res = await users_collection.database["orders"].insert_one(order_doc)
    new_order_id = str(res.inserted_id)

    # 4) Сохраняем новый active_order и переходим к выбору склада
    await users_collection.update_one(
        {"chat_id": chat_id, "type": "fulfilment"},
        {"$set": {
            "active_order": new_order_id,
            "state":        "select_warehouse"
        }}
    )

    # 5) Предлагаем выбрать склад
    # Предлагаем выбрать склад — используем WAREHOUSES из services.py
    rows = [svc.WAREHOUSES[i : i + 2] for i in range(0, len(svc.WAREHOUSES), 2)]
    buttons = [[{"text": w} for w in row] for row in rows]
    buttons.append([{"text": "🔄 Начать заново"}])
    keyboard = {"keyboard": buttons, "resize_keyboard": True}

    await svc.send_text(
        chat_id,
        "🏬 Выберите склад разгрузки:",
        svc.fulfilment_bot,
        keyboard
    )

@on_state("confirm_inn")
async def handle_confirm_inn(chat_id, user, text):
    # 1) Подтвердили найденную организацию → ввод Р/С
    if text == "✅ Продолжить":
        await users_collection.update_one(
            {"chat_id": chat_id, "type": "fulfilment"},
            {"$set": {"state": "awaiting_rs"}}
        )
        keyboard = {
            "keyboard": [[{"text": "🔄 Начать заново"}]],
            "resize_keyboard": True
        }
        await svc.send_text(
            chat_id,
            "Введите расчётный счёт организации",
            svc.fulfilment_bot,
            keyboard
        )
        return

    # 2) Хотят ввести ИНН заново → возвращаем в awaiting_inn
    if text == "❌ Ввести другой ИНН":
        await users_collection.update_one(
            {"chat_id": chat_id, "type": "fulfilment"},
            {"$set": {"state": "awaiting_inn"}}
        )
        await svc.send_text(
            chat_id,
            "Введите ИНН компании",
            svc.fulfilment_bot
        )
        return

    # 3) Всё остальное → показываем клавиатуру ещё раз
    keyboard = {
        "keyboard": [
            [{"text": "✅ Продолжить"}, {"text": "❌ Ввести другой ИНН"}],
            [{"text": "🔄 Начать заново"}]
        ],
        "resize_keyboard": True
    }
    await svc.send_text(
        chat_id,
        "Пожалуйста, подтвердите организацию или введите ИНН заново:",
        svc.fulfilment_bot,
        keyboard
    )

@on_state("awaiting_rs")
async def handle_rs_input(chat_id, user, text):
    rs = text.strip()
    order_id = user["active_order"]
    # Сохраняем расчётный счёт в заказ
    await users_collection.database["orders"].update_one(
        {"_id": ObjectId(order_id)},
        {"$set": {"rs": rs}}
    )
    # Переходим к вводу БИК
    await users_collection.update_one(
        {"chat_id": chat_id, "type": "fulfilment"},
        {"$set": {"state": "awaiting_bik"}}
    )
    # Только «Начать заново»
    await svc.send_text(
        chat_id,
        "Введите БИК",
        svc.fulfilment_bot,
        {"keyboard":[[{"text":"🔄 Начать заново"}]], "resize_keyboard":True}
    )


@on_state("awaiting_bik")
async def handle_bik_input(chat_id, user, text):
    bik = text.strip()
    order_id = user["active_order"]
    # Сохраняем БИК в заказ
    await users_collection.database["orders"].update_one(
        {"_id": ObjectId(order_id)},
        {"$set": {"bik": bik}}
    )
    # Переходим к выбору склада
    await users_collection.update_one(
        {"chat_id": chat_id, "type": "fulfilment"},
        {"$set": {"state": "select_warehouse"}}
    )
    
    rows = [svc.WAREHOUSES[i : i + 2] for i in range(0, len(svc.WAREHOUSES), 2)]
    buttons = [[{"text": w} for w in row] for row in rows]
    buttons.append([{"text": "🔄 Начать заново"}])
    keyboard = {"keyboard": buttons, "resize_keyboard": True}

    await svc.send_text(
        chat_id,
        "🏬 Выберите склад разгрузки:",
        svc.fulfilment_bot,
        keyboard
    )

@on_state("select_warehouse")
async def handle_select_warehouse(chat_id, user, text):
    order_id = user.get("active_order")
    if not order_id:
        # нет активного заказа — просим начать заново
        await svc.send_text(
            chat_id,
            "⚠️ Заказ не найден. Начните заново",
            svc.fulfilment_bot,
            {"keyboard":[[{"text":"🔄 Начать заново"}]], "resize_keyboard": True}
        )
        await users_collection.update_one(
            {"chat_id": chat_id, "type": "fulfilment"},
            {"$set": {"state": "start"}}
        )
        return

    warehouse = text.strip()

    if warehouse not in svc.WAREHOUSES:
        rows = [svc.WAREHOUSES[i : i + 2] for i in range(0, len(svc.WAREHOUSES), 2)]
        buttons = [[{"text": w} for w in row] for row in rows]
        buttons.append([{"text": "🔄 Начать заново"}])
        keyboard = {"keyboard": buttons, "resize_keyboard": True}

        await svc.send_text(
            chat_id,
            "❌ Некорректный выбор. Пожалуйста, выберите склад из списка:",
            svc.fulfilment_bot,
            keyboard
        )
        return

    # сохраняем выбранный склад
    await users_collection.database["orders"].update_one(
        {"_id": ObjectId(order_id)},
        {"$set": {"warehouse": warehouse}}
    )
    # переходим к выбору даты сдачи
    await users_collection.update_one(
        {"chat_id": chat_id, "type": "fulfilment"},
        {"$set": {"state": "select_delivery_date"}}
    )

    # 1) Получаем пары дат через calculate_schedule
    slots = svc.calculate_schedule(warehouse)
    if not slots:
        await svc.send_text(
            chat_id,
            "⛔ Нет доступных дат сдачи поставки на ближайшие 2 недели.",
            svc.fulfilment_bot,
            {"keyboard":[[{"text":"🔄 Начать заново"}]], "resize_keyboard": True}
        )
        return

    # 2) Собираем до 6 уникальных дат сдачи
    unique_dates: list[str] = []
    for slot in slots:
        d_str = slot["delivery"].strftime("%d.%m.%Y")
        if d_str not in unique_dates:
            unique_dates.append(d_str)
        if len(unique_dates) >= 6:
            break

    # 3) Разбиваем по 2 даты на строку
    rows = [unique_dates[i : i + 2] for i in range(0, len(unique_dates), 2)]
    buttons = [[{"text": d} for d in row] for row in rows]
    buttons.append([{"text": "🔄 Начать заново"}])
    keyboard = {"keyboard": buttons, "resize_keyboard": True}

    await svc.send_text(
        chat_id,
        "📅 Выберите дату сдачи поставки:",
        svc.fulfilment_bot,
        keyboard
    )

@on_state("select_delivery_date")
async def handle_select_delivery_date(chat_id, user, text):
    from datetime import datetime as _dt
    # Парсим выбранную дату разгрузки
    try:
        delivery_date = _dt.strptime(text.strip(), "%d.%m.%Y").date()
    except ValueError:
        await svc.send_text(
            chat_id,
            "❌ Неверный формат даты. Выберите из кнопок.",
            svc.fulfilment_bot
        )
        order_id = user.get("active_order")
        oid = ObjectId(order_id) if isinstance(order_id, str) else order_id
        order = await users_collection.database["orders"].find_one({"_id": oid})
        warehouse = order.get("warehouse", "")
        await svc.prompt_delivery_date_selection(chat_id, svc.fulfilment_bot, warehouse)
        return

    order_id = user.get("active_order")
    await users_collection.database["orders"].update_one(
        {"_id": ObjectId(order_id)},
        {"$set": {"delivery_date": delivery_date.isoformat()}}
    )

    # Получаем возможные даты забора для этой даты разгрузки
    order     = await users_collection.database["orders"].find_one({"_id": ObjectId(order_id)})
    warehouse = order.get("warehouse", "")
    pickups   = svc.get_pickup_dates(warehouse, delivery_date)  # :contentReference[oaicite:0]{index=0}

    # Если только одна дата забора — сохраняем и сразу переходим к выбору типа груза
    if len(pickups) == 1:
        await users_collection.database["orders"].update_one(
            {"_id": ObjectId(order_id)},
            {"$set": {"pickup_date": pickups[0].isoformat()}}
        )
        await users_collection.update_one(
            {"chat_id": chat_id, "type": "fulfilment"},
            {"$set": {"state": "select_cargo_type"}}
        )
        await svc.send_cargo_type_selection(chat_id, svc.fulfilment_bot)
        return

    # Иначе (только для Котовск) — даём выбрать дату забора
    rows = [pickups[i : i + 2] for i in range(0, len(pickups), 2)]
    buttons = [
        [{"text": d.strftime("%d.%m.%Y")} for d in row]
        for row in rows
    ]
    buttons.append([{"text": "🔄 Начать заново"}])
    keyboard = {"keyboard": buttons, "resize_keyboard": True}

    await users_collection.update_one(
        {"chat_id": chat_id, "type": "fulfilment"},
        {"$set": {"state": "select_pickup_date"}}
    )
    await svc.send_text(
        chat_id,
        "🚚 Выберите дату забора поставки:",
        svc.fulfilment_bot,
        keyboard
    )

@on_state("select_pickup_date")
async def handle_select_pickup_date(chat_id, user, text):
    from datetime import datetime as _dt
    # Парсим выбранную дату забора
    try:
        pickup_date = _dt.strptime(text.strip(), "%d.%m.%Y").date()
    except ValueError:
        order_id = user.get("active_order")
        # приводим к ObjectId, если нужно
        oid = ObjectId(order_id) if isinstance(order_id, str) else order_id
        order = await users_collection.database["orders"].find_one({"_id": oid})
        warehouse = order.get("warehouse", "")
        # delivery_date хранится в ISO-формате
        delivery_iso = order.get("delivery_date")
        delivery_date = _dt.fromisoformat(delivery_iso).date() if delivery_iso else None

        # собираем варианты дат забора
        pickups = svc.get_pickup_dates(warehouse, delivery_date)  # :contentReference[oaicite:0]{index=0}

        # формируем кнопки по две в ряд
        rows = [pickups[i : i + 2] for i in range(0, len(pickups), 2)]
        buttons = [
            [{"text": d.strftime("%d.%m.%Y")} for d in row]
            for row in rows
        ]
        buttons.append([{"text": "🔄 Начать заново"}])
        keyboard = {"keyboard": buttons, "resize_keyboard": True}

        await svc.send_text(
            chat_id,
            "❌ Неверная дата. Пожалуйста, выберите дату забора снова:",
            svc.fulfilment_bot,
            keyboard
        )
        return

    order_id = user.get("active_order")
    await users_collection.database["orders"].update_one(
        {"_id": ObjectId(order_id)},
        {"$set": {"pickup_date": pickup_date.isoformat()}}
    )
    await users_collection.update_one(
        {"chat_id": chat_id, "type": "fulfilment"},
        {"$set": {"state": "select_cargo_type"}}
    )
    await svc.send_cargo_type_selection(chat_id, svc.fulfilment_bot)

@on_state("select_cargo_type")
async def handle_select_cargo_type(chat_id, user, text):
    # Проверяем корректность выбора
    if text not in ["📦 Короба", "🧱 Палеты"]:
        await svc.send_text(
            chat_id,
            "❌ Пожалуйста, выберите вариант из кнопок.",
            svc.fulfilment_bot
        )
        await svc.send_cargo_type_selection(chat_id, svc.fulfilment_bot)
        return

    # Определяем тип груза
    cargo_type = "boxes" if "Короба" in text else "pallets"

    # Сохраняем в заказе
    order_id = user.get("active_order")
    await users_collection.database["orders"].update_one(
        {"_id": ObjectId(order_id)},
        {"$set": {"cargo_type": cargo_type}}
    )

    # Переходим к вводу количества и меняем состояние
    await users_collection.update_one(
        {"chat_id": chat_id, "type": "fulfilment"},
        {"$set": {"state": "enter_cargo_quantity"}}
    )

    # Запрос количества
    cargo_label = "коробов" if cargo_type == "boxes" else "палет"
    keyboard = {
        "keyboard": [[{"text": "🔄 Начать заново"}]],
        "resize_keyboard": True
    }
    await svc.send_text(
        chat_id,
        f"✏️ Введите количество {cargo_label} (целое число)",
        svc.fulfilment_bot,
        keyboard
    )

# 1) Ввод количества груза
@on_state("enter_cargo_quantity")
async def handle_enter_cargo_quantity(chat_id, user, text):
    try:
        qty = int(text)
        if qty <= 0:
            raise ValueError
    except ValueError:
        # берём метку активного заказа и тип груза, чтобы показать корректное сообщение
        order_id = user.get("active_order")
        order = await users_collection.database["orders"].find_one({"_id": ObjectId(order_id)})
        cargo_label = "коробов" if order.get("cargo_type") == "boxes" else "палет"
        await svc.send_text(
            chat_id,
            f"❌ Введите положительное целое число {cargo_label}",
            svc.fulfilment_bot
        )
        return

    # сохраняем количество
    order_id = user.get("active_order")
    await users_collection.database["orders"].update_one(
        {"_id": ObjectId(order_id)},
        {"$set": {"cargo_quantity": qty}}
    )
    # переходим к выбору/вводу адреса
    await users_collection.update_one(
        {"chat_id": chat_id, "type": "fulfilment"},
        {"$set": {"state": "enter_pickup_address"}}
    )
    await svc.prompt_pickup_address_selection(chat_id, svc.fulfilment_bot)


# 2) Выбор или ввод адреса
@on_state("enter_pickup_address")
async def handle_enter_pickup_address(chat_id, user, text):
    address = text.replace("📍 ", "").strip()
    if not address:
        await svc.send_text(
            chat_id,
            "❌ Адрес не может быть пустым. Введите корректный адрес.",
            svc.fulfilment_bot
        )
        return

    order_id = user.get("active_order")
    await users_collection.database["orders"].update_one(
        {"_id": ObjectId(order_id)},
        {"$set": {"pickup_address": address}}
    )
    await users_collection.update_one(
        {"chat_id": chat_id, "type": "fulfilment"},
        {"$set": {"state": "enter_phone_number"}}
    )
    await svc.prompt_phone_number_selection(chat_id, svc.fulfilment_bot)


@on_state("typing_pickup_address")
async def handle_typing_pickup_address(chat_id, user, text):
    address = text.strip()
    if not address:
        await svc.send_text(
            chat_id,
            "❌ Адрес не может быть пустым. Введите корректный адрес.",
            svc.fulfilment_bot
        )
        return

    order_id = user.get("active_order")
    await users_collection.database["orders"].update_one(
        {"_id": ObjectId(order_id)},
        {"$set": {"pickup_address": address}}
    )
    await users_collection.update_one(
        {"chat_id": chat_id, "type": "fulfilment"},
        {"$set": {"state": "enter_phone_number"}}
    )
    await svc.prompt_phone_number_selection(chat_id, svc.fulfilment_bot)


@on_state("enter_phone_number")
async def handle_enter_phone_number(chat_id, user, message):
    # 0) Логируем, что пришло
    logger.info("enter_phone_number ➔ %r", message)

    # 1) Пробуем достать контакт из message["contact"]
    phone = None
    contact = message.get("contact")
    if contact and contact.get("phone_number"):
        phone = contact["phone_number"]

    # 2) Иначе пробуем извлечь из текста
    text = message.get("text", "").strip()
    if phone is None and text.startswith("📞 "):
        phone = text.lstrip("📞 ").strip()
    if phone is None and re.fullmatch(r"\+\d+", text):
        phone = text

    # 3) Если не получилось — перерисовываем меню выбора телефона
    if not phone:
        await svc.prompt_phone_number_selection(chat_id, svc.fulfilment_bot)
        return

    if phone and not phone.startswith("+"):
        phone = "+" + phone

    # 4) Сохраняем номер в заказ и рассчитываем стоимость
    order_id = user.get("active_order")
    await users_collection.database["orders"].update_one(
        {"_id": ObjectId(order_id)},
        {"$set": {"phone_number": phone}}
    )
    cost = await svc.calculate_delivery_cost_ff(chat_id)
    await users_collection.database["orders"].update_one(
        {"_id": ObjectId(order_id)},
        {"$set": {"delivery_cost": cost}}
    )

    # 5) Переходим к финальному суммари
    await users_collection.update_one(
        {"chat_id": chat_id, "type": "fulfilment"},
        {"$set": {"state": "awaiting_order_submit"}}
    )

    order = await users_collection.database["orders"].find_one(
        {"_id": ObjectId(order_id)}
    )

    delivery_iso = order.get("delivery_date")
    pickup_iso   = order.get("pickup_date")

    delivery_str = (
    datetime.fromisoformat(delivery_iso)
    .strftime("%d.%m.%Y")
    if delivery_iso
    else "—"
    )
    pickup_str = (
        datetime.fromisoformat(pickup_iso)
        .strftime("%d.%m.%Y")
        if pickup_iso
        else "—"
    )

    parts = [
        "📋 *Проверьте данные заявки:*",
        f"🏢 *Организация*: {order.get('org_name','—')}",
        f"📍 *Адрес организации*: {order.get('org_address','—')}",
        f"💳 *Р/С*: {order.get('rs','—')}",
        f"🏦 *БИК*: {order.get('bik','—')}",
        f"📦 *Тип поставки*: {'Короба' if order.get('cargo_type')=='boxes' else 'Палеты'}",
        f"🔢 *Количество*: {order.get('cargo_quantity','—')}",
        f"🏬 *Склад*: {order.get('warehouse','—')}",
        f"📅 *Дата сдачи*: {delivery_str}",
        f"🚚 *Дата забора*: {pickup_str}",
        f"🏠 *Адрес забора*: {order.get('pickup_address','—')}",
        f"📞 *Телефон*: {phone}",
        f"💰 *Стоимость*: {cost} ₽"
    ]
    review_text = "\n".join(parts)
    keyboard = {
        "keyboard": [[{"text":"📨 Отправить заявку"}],[{"text":"🔄 Начать заново"}]],
        "resize_keyboard": True
    }

    sent = await svc.send_text(
        chat_id,
        review_text,
        svc.fulfilment_bot,
        keyboard
    )
    # 6) Сохраняем ID этого суммари для последующего удаления
    mid = getattr(sent, "message_id", None) or sent.json().get("result",{}).get("message_id")
    if mid:
        await users_collection.database["orders"].update_one(
            {"_id": ObjectId(order_id)},
            {"$set": {"summ_mid": mid}}
        )

# 4) Отправка заявки (awaiting_order_submit)
@on_state("awaiting_order_submit")
async def handle_awaiting_order_submit(chat_id, user, text):
    
    if not text:
        return

    if text == "🔄 Начать заново":
        await handle_fulfilment_start(chat_id, user, message=None)
        return

    if text != "📨 Отправить заявку":
        await svc.send_text(
            chat_id,
            "❌ Пожалуйста, нажмите «📨 Отправить заявку» или «🔄 Начать заново».",
            svc.fulfilment_bot
        )
        return

    order_id = user.get("active_order")
    if not order_id:
        await svc.send_text(
            chat_id,
            "⚠️ Не удалось найти активный заказ. Начните заново.",
            svc.fulfilment_bot
        )
        return

    order = await users_collection.database["orders"].find_one(
        {"_id": ObjectId(order_id)}
    )
    delivery_iso = order.get("delivery_date")
    pickup_iso   = order.get("pickup_date")

    delivery_str = (
    datetime.fromisoformat(delivery_iso)
    .strftime("%d.%m.%Y")
    if delivery_iso
    else "—"
    )
    pickup_str = (
        datetime.fromisoformat(pickup_iso)
        .strftime("%d.%m.%Y")
        if pickup_iso
        else "—"
    )

    # Отправляем в Битрикс и получаем номер заявки
    await svc.fulfilment_bot.send_chat_action(chat_id, ChatAction.TYPING)
    deal_id = await svc.send_to_bitrix(order, user.get("username", ""))

    # Сохраняем bitrix_deal_id в заказе
    await users_collection.database["orders"].update_one(
        {"_id": ObjectId(order_id)},
        {"$set": {"bitrix_deal_id": deal_id}}
    )
    
    # Удаляем прежнее суммари
    summ_mid = order.get("summ_mid")
    if summ_mid:
        try:
            await svc.fulfilment_bot.delete_message(chat_id, summ_mid)
        except:
            pass

    # Составляем окончательный суммари с номером заявки
    parts = [
        "✅ Ваша заявка успешно отправлена!",
        "📋 Содержимое заявки:",
        "",
        f"🆔 Номер заявки: #{deal_id}",
        f"🏢 Организация: {order.get('org_name', '—')}",
        f"📍 Адрес организации: {order.get('org_address', '—')}",
        f"🏦 БИК: {order.get('bik', '—')}",
        f"💳 Р/С: {order.get('rs', '—')}",
        f"📦 Тип поставки: {'Короба' if order.get('cargo_type') == 'boxes' else 'Палеты'}",
        f"🔢 Количество: {order.get('cargo_quantity', '—')}",
        f"🏬 Склад: {order.get('warehouse', '—')}",
        f"📅 Дата сдачи: {delivery_str}",
        f"🚚 Дата забора: {pickup_str}",
        f"🏠 Адрес забора: {order.get('pickup_address', '—')}",
        f"📞 Телефон: {order.get('phone_number', '—')}",
        f"💰 Стоимость: {order.get('delivery_cost', '—')} ₽"
    ]
    final_summary = "\n".join(parts)
    
    # Отправляем финальное суммари и сохраняем его message_id
    sent = await svc.send_text(
        chat_id,
        final_summary,
        svc.fulfilment_bot
    )
    new_mid = getattr(sent, "message_id", None) or sent.json().get("result", {}).get("message_id")
    if new_mid:
        await users_collection.database["orders"].update_one(
            {"_id": ObjectId(order_id)},
            {"$set": {"summ_mid": new_mid}}
        )

    notify_text = "📨 При изменении статуса вы получите уведомление."
    keyboard = {
        "keyboard": [[{"text": "🔄 Начать заново"}]],
        "resize_keyboard": True
    }
    await svc.send_text(
        chat_id,
        notify_text,
        svc.fulfilment_bot,
        keyboard
    )