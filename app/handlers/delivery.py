# app/handlers/delivery.py
import re
import logging
from datetime import datetime
from app.handlers.decorators import on_command, on_state
from app.db import users_collection
from app.db import calcs_collection
import app.services as svc
from bson import ObjectId
from httpx import AsyncClient
import base64
from aiogram.enums.chat_action import ChatAction
from app.config import get_settings

settings = get_settings()

logger = logging.getLogger(__name__)

@on_command("/start")
@on_command("🔄 Начать заново")
async def handle_delivery_start(chat_id, user, message):
    # если пользователя нет — создаём
    if not user:
        await users_collection.insert_one({
            "chat_id":    chat_id,
            "username":   message["chat"].get("username"),
            "first_name": message["chat"].get("first_name"),
            "last_name":  message["chat"].get("last_name"),
            "type":       "delivery",
            "created_at": datetime.utcnow()
        })
    else:
        # сбрасываем состояние
        await users_collection.update_one(
            {"chat_id": chat_id, "type": "delivery"},
            {"$set": {"state": None}}
        )

    # 1) Приветствие и выбор действия
    keyboard = {
        "keyboard": [
            [{"text": "📦 Создать новую заявку"}],
            [{"text": "💰 Рассчитать стоимость"}]
        ],
        "resize_keyboard": True
    }
    await svc.send_text(
        chat_id,
        "Добро пожаловать в чат-бот компании Ecomdelivery.\nВыберите действие, нажав на кнопку ниже строки ввода текста:",
        svc.delivery_bot,
        keyboard
    )

    # 2) Переходим в состояние "start"
    await users_collection.update_one(
        {"chat_id": chat_id, "type": "delivery"},
        {"$set": {"state": "start"}}
    )

@on_command("📦 Создать новую заявку")
@on_command("/new")
async def handle_delivery_new_application(chat_id, user, message):
    # Отправляем вводное сообщение для создания заявки
    await svc.send_intro_message(chat_id)
    # Устанавливаем состояние на ожидание ввода следующих данных
    await users_collection.update_one(
        {"chat_id": chat_id, "type": "delivery"},
        {"$set": {"state": "awaiting_inn"}}
    )

@on_command("📦 Создать заявку")
async def handle_create_application(chat_id, user, message):
    # 1) Собираем из прошлых заказов ИНН и «org_name» (ИП / организации)
    cursor = users_collection.database["orders"].find({"chat_id": chat_id})
    ips = set()
    async for order in cursor:
        if (
            order.get("inn")
            and order.get("org_name")
            and order.get("org_address")
            and order.get("rs")
            and order.get("bik")
        ):
            ips.add(order["org_name"])

    if ips:
        # 2a) Есть ИП / организации — предлагаем выбрать или ввести ИНН ИП / компании
        buttons = [[{"text": name}] for name in sorted(ips)]
        buttons.append([{"text": "🔄 Начать заново"}])
        keyboard = {"keyboard": buttons, "resize_keyboard": True}

        await users_collection.update_one(
            {"chat_id": chat_id, "type": "delivery"},
            {"$set": {"state": "select_existing_org"}}
        )
        await svc.send_text(
            chat_id,
            "Выберите ИП / организацию из списка или введите ИНН ИП / компании",
            svc.delivery_bot,
            keyboard
        )  # адаптировано из оригинала :contentReference[oaicite:0]{index=0}

    else:
        # 2b) Нет — сразу просим ИНН ИП / компании
        await users_collection.update_one(
            {"chat_id": chat_id, "type": "delivery"},
            {"$set": {"state": "awaiting_inn"}}
        )
        await svc.send_text(
            chat_id,
            "Введите ИНН ИП / компании",
            svc.delivery_bot
        )

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
        await svc.send_text(
            chat_id,
            "❌ ИП / компания не найдена. Проверьте ИНН ИП / компании.",
            svc.delivery_bot
        )
        return  # :contentReference[oaicite:0]{index=0}

    # 3) Берём первую подсказку
    item       = suggestions[0]
    org_name   = item["data"]["name"]["full_with_opf"]
    addr_obj   = item["data"].get("address")
    org_address = addr_obj["value"] if addr_obj else "— адрес не указан —"

    # 4) Создаём новый «плоский» заказ с type="delivery"
    order_doc = {
        "chat_id":      chat_id,
        "inn":          inn,
        "org_name":     org_name,
        "org_address":  org_address,
        "created_at":   datetime.utcnow(),
        "status":       "in_progress",
        "type":         "delivery",
        "is_active":    True
    }
    res = await users_collection.database["orders"].insert_one(order_doc)
    order_id = str(res.inserted_id)  # :contentReference[oaicite:1]{index=1}

    # 5) Сохраняем активный заказ и переключаем состояние
    await users_collection.update_one(
        {"chat_id": chat_id, "type": "delivery"},
        {"$set": {"active_order": order_id, "state": "confirm_inn"}}
    )

    # 6) Просим подтвердить или ввести другой ИНН
    keyboard = {
        "keyboard": [
            [{"text": "✅ Продолжить"}, {"text": "❌ Ввести другой ИНН"}],
            [{"text": "🔄 Начать заново"}]
        ],
        "resize_keyboard": True
    }
    await svc.send_text(
        chat_id,
        f"✅ Найдена ИП / организация:\n{org_name}\nАдрес: {org_address}",
        svc.delivery_bot,
        keyboard
    )

@on_state("select_existing_org")
async def handle_select_existing_org(chat_id, user, text):
    # Если ввели цифры — это новый ИНН, перенаправляем в handle_inn_input
    if text.isdigit():
        # обновим профиль пользователя и передадим в существующий хендлер ввода ИНН
        user = await users_collection.find_one({"chat_id": chat_id, "type": "delivery"})
        await handle_inn_input(chat_id, user, text)
        return  # :contentReference[oaicite:0]{index=0}

    # Иначе выбранная ИП / организация по названию
    org_name = text
    last_order = await users_collection.database["orders"].find_one(
        {
            "chat_id":      chat_id,
            "org_name":     org_name,
            "inn":          {"$exists": True},
            "org_address":  {"$exists": True},
            "rs":           {"$exists": True},
            "bik":          {"$exists": True},
        },
        sort=[("created_at", -1)]
    )

    if not last_order:
        # Не нашли — просим ввести ИНН ИП / компании заново
        await svc.send_text(
            chat_id,
            "Не удалось найти полностью заполнённую ИП / организацию. Введите ИНН ИП / компании",
            svc.delivery_bot,
            {"keyboard":[[{"text":"🔄 Начать заново"}]], "resize_keyboard": True}
        )
        await users_collection.update_one(
            {"chat_id": chat_id, "type": "delivery"},
            {"$set": {"state": "select_existing_org"}}
        )
        return  # :contentReference[oaicite:1]{index=1}

    # Копируем поля из последнего заказа в новый
    order_doc = {
        "chat_id":      chat_id,
        "inn":          last_order["inn"],
        "org_name":     last_order["org_name"],
        "org_address":  last_order["org_address"],
        "rs":           last_order["rs"],
        "bik":          last_order["bik"],
        "created_at":   datetime.utcnow(),
        "status":       "in_progress",
        "type":         "delivery",
        "is_active":    True
    }
    res = await users_collection.database["orders"].insert_one(order_doc)
    new_order_id = str(res.inserted_id)

    # Сохраняем новый active_order и переходим к выбору склада
    await users_collection.update_one(
        {"chat_id": chat_id, "type": "delivery"},
        {"$set": {"active_order": new_order_id, "state": "select_warehouse"}}
    )

    # Предлагаем выбрать склад — используем WAREHOUSES из services.py
    rows = [svc.WAREHOUSES[i : i + 2] for i in range(0, len(svc.WAREHOUSES), 2)]
    buttons = [[{"text": w} for w in row] for row in rows]
    buttons.append([{"text": "🔄 Начать заново"}])
    keyboard = {"keyboard": buttons, "resize_keyboard": True}

    await svc.send_text(
        chat_id,
        "🏬 Выберите склад разгрузки:",
        svc.delivery_bot,
        keyboard
    )

@on_state("confirm_inn")
async def handle_confirm_inn(chat_id, user, text):
    # 1) Подтвердили найденную ИП / организацию → ввод Р/С
    if text == "✅ Продолжить":
        await users_collection.update_one(
            {"chat_id": chat_id, "type": "delivery"},
            {"$set": {"state": "awaiting_rs"}}
        )
        keyboard = {
            "keyboard": [[{"text": "🔄 Начать заново"}]],
            "resize_keyboard": True
        }
        await svc.send_text(
            chat_id,
            "Введите расчётный счёт ИП / организации",
            svc.delivery_bot,
            keyboard
        )
        return

    # 2) Хотят ввести ИНН заново → возвращаем в awaiting_inn
    if text == "❌ Ввести другой ИНН":
        await users_collection.update_one(
            {"chat_id": chat_id, "type": "delivery"},
            {"$set": {"state": "awaiting_inn"}}
        )
        await svc.send_text(
            chat_id,
            "Введите ИНН ИП / компании",
            svc.delivery_bot
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
        "Пожалуйста, подтвердите ИП / организацию или введите ИНН ИП / компании заново:",
        svc.delivery_bot,
        keyboard
    )

@on_state("awaiting_rs")
async def handle_rs_input(chat_id, user, text):
    rs = text.strip()
    order_id = user.get("active_order")
    # Сохраняем расчётный счёт в заказ
    await users_collection.database["orders"].update_one(
        {"_id": ObjectId(order_id)},
        {"$set": {"rs": rs}}
    )
    # Переходим к вводу БИК
    await users_collection.update_one(
        {"chat_id": chat_id, "type": "delivery"},
        {"$set": {"state": "awaiting_bik"}}
    )
    # Только «Начать заново»
    await svc.send_text(
        chat_id,
        "Введите БИК",
        svc.delivery_bot,
        {"keyboard": [[{"text": "🔄 Начать заново"}]], "resize_keyboard": True}
    )

@on_state("awaiting_bik")
async def handle_bik_input(chat_id, user, text):
    bik = text.strip()
    order_id = user.get("active_order")
    # Сохраняем БИК в заказ
    await users_collection.database["orders"].update_one(
        {"_id": ObjectId(order_id)},
        {"$set": {"bik": bik}}
    )
    # Переходим к выбору склада
    await users_collection.update_one(
        {"chat_id": chat_id, "type": "delivery"},
        {"$set": {"state": "select_warehouse"}}
    )
    # Предлагаем выбрать склад — используем svc.WAREHOUSES
    rows = [svc.WAREHOUSES[i : i + 2] for i in range(0, len(svc.WAREHOUSES), 2)]
    buttons = [[{"text": w} for w in row] for row in rows]
    buttons.append([{"text": "🔄 Начать заново"}])
    keyboard = {"keyboard": buttons, "resize_keyboard": True}

    await svc.send_text(
        chat_id,
        "🏬 Выберите склад разгрузки:",
        svc.delivery_bot,
        keyboard
    )

@on_state("select_warehouse")
async def handle_select_warehouse(chat_id, user, text):
    warehouse = text.strip()

    if warehouse not in svc.WAREHOUSES:
        rows = [svc.WAREHOUSES[i : i + 2] for i in range(0, len(svc.WAREHOUSES), 2)]
        buttons = [[{"text": w} for w in row] for row in rows]
        buttons.append([{"text": "🔄 Начать заново"}])
        keyboard = {"keyboard": buttons, "resize_keyboard": True}

        await svc.send_text(
            chat_id,
            "❌ Некорректный выбор. Пожалуйста, выберите склад из списка:",
            svc.delivery_bot,
            keyboard
        )
        return

    order_id = user.get("active_order")
    await users_collection.database["orders"].update_one(
        {"_id": ObjectId(order_id)},
        {"$set": {"warehouse": warehouse}}
    )
    await users_collection.update_one(
        {"chat_id": chat_id, "type": "delivery"},
        {"$set": {"state": "select_delivery_date"}}
    )

    await svc.prompt_delivery_date_selection(chat_id, svc.delivery_bot, warehouse)

@on_state("select_delivery_date")
async def handle_select_delivery_date(chat_id, user, text):
    from datetime import datetime as _dt

    # 1) Парсим выбранную дату сдачи
    try:
        delivery_date = _dt.strptime(text.strip(), "%d.%m.%Y").date()
    except ValueError:
        await svc.send_text(
            chat_id,
            "❌ Неверный формат даты. Выберите из кнопок.",
            svc.delivery_bot
        )
        order_id = user.get("active_order")
        oid = ObjectId(order_id) if isinstance(order_id, str) else order_id
        order = await users_collection.database["orders"].find_one({"_id": oid})
        warehouse = order.get("warehouse", "")
        await svc.prompt_delivery_date_selection(chat_id, svc.delivery_bot, warehouse)
        return

    order_id = user.get("active_order")
    # 2) Сохраняем дату сдачи в заказе
    await users_collection.database["orders"].update_one(
        {"_id": ObjectId(order_id)},
        {"$set": {"delivery_date": delivery_date.isoformat()}}
    )  # :contentReference[oaicite:0]{index=0}

    # 3) Получаем все возможные даты забора для этой доставки
    warehouse = user.get("warehouse") or (await users_collection.database["orders"]
        .find_one({"_id": ObjectId(order_id)})).get("warehouse", "")
    pickups = svc.get_pickup_dates(warehouse, delivery_date)  # :contentReference[oaicite:1]{index=1}

    # 4) Если только одна дата забора — сразу записываем и идём к выбору типа груза
    if len(pickups) == 1:
        await users_collection.database["orders"].update_one(
            {"_id": ObjectId(order_id)},
            {"$set": {"pickup_date": pickups[0].isoformat()}}
        )
        await users_collection.update_one(
            {"chat_id": chat_id, "type": "delivery"},
            {"$set": {"state": "select_cargo_type"}}
        )
        await svc.send_cargo_type_selection(chat_id, svc.delivery_bot)
        return  # :contentReference[oaicite:2]{index=2}

    # 5) Иначе — два варианта (Котовск) — показываем выбор даты забора
    rows = [
        pickups[i : i + 2]
        for i in range(0, len(pickups), 2)
    ]
    buttons = [
        [{"text": d.strftime("%d.%m.%Y")} for d in row]
        for row in rows
    ]
    buttons.append([{"text": "🔄 Начать заново"}])
    keyboard = {"keyboard": buttons, "resize_keyboard": True}

    await users_collection.update_one(
        {"chat_id": chat_id, "type": "delivery"},
        {"$set": {"state": "select_pickup_date"}}
    )
    await svc.send_text(
        chat_id,
        "🚚 Выберите дату забора поставки:",
        svc.delivery_bot,
        keyboard
    )


@on_state("select_pickup_date")
async def handle_select_pickup_date(chat_id, user, text):
    from datetime import datetime as _dt

    # 1) Парсим выбранную дату забора
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
            svc.delivery_bot,
            keyboard
        )
        return

    order_id = user.get("active_order")
    # 2) Сохраняем дату забора и идём к выбору типа груза
    await users_collection.database["orders"].update_one(
        {"_id": ObjectId(order_id)},
        {"$set": {"pickup_date": pickup_date.isoformat()}}
    )
    await users_collection.update_one(
        {"chat_id": chat_id, "type": "delivery"},
        {"$set": {"state": "select_cargo_type"}}
    )
    await svc.send_cargo_type_selection(chat_id, svc.delivery_bot)

@on_state("select_cargo_type")
async def handle_select_cargo_type(chat_id, user, text):
    # Проверяем корректность выбора
    if text not in ["📦 Короба", "🧱 Палеты"]:
        await svc.send_text(
            chat_id,
            "❌ Пожалуйста, выберите вариант из кнопок.",
            svc.delivery_bot
        )
        await svc.send_cargo_type_selection(chat_id, svc.delivery_bot)
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
        {"chat_id": chat_id, "type": "delivery"},
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
        svc.delivery_bot,
        keyboard
    )

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
            svc.delivery_bot
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
        {"chat_id": chat_id, "type": "delivery"},
        {"$set": {"state": "enter_pickup_address"}}
    )
    await svc.prompt_pickup_address_selection(chat_id, svc.delivery_bot)

# 2) Выбор или ввод адреса
@on_state("enter_pickup_address")
async def handle_enter_pickup_address(chat_id, user, text):
    address = text.replace("📍 ", "").strip()
    if not address:
        await svc.send_text(
            chat_id,
            "❌ Адрес не может быть пустым. Введите корректный адрес.",
            svc.delivery_bot
        )
        return

    order_id = user.get("active_order")
    await users_collection.database["orders"].update_one(
        {"_id": ObjectId(order_id)},
        {"$set": {"pickup_address": address}}
    )
    await users_collection.update_one(
        {"chat_id": chat_id, "type": "delivery"},
        {"$set": {"state": "enter_phone_number"}}
    )
    await svc.prompt_phone_number_selection(chat_id, svc.delivery_bot)

@on_state("enter_phone_number")
async def handle_enter_phone_number(chat_id, user, message):
    # 0) Логируем, что пришло
    logger.info("enter_phone_number ➔ %r", message)

    # 1) Пробуем достать контакт
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
        await svc.prompt_phone_number_selection(chat_id, svc.delivery_bot)
        return

    if phone and not phone.startswith("+"):
        phone = "+" + phone

    # 4) Сохраняем номер в заказ
    order_id = user.get("active_order")
    try:
        oid = ObjectId(order_id) if isinstance(order_id, str) else order_id
    except Exception:
        oid = None

    order = None
    if oid:
        order = await users_collection.database["orders"].find_one({"_id": oid})

    if not order:
        # заказ удалён или не найден — просим начать заново
        await svc.send_text(
            chat_id,
            "❌ Ваш заказ не найден (возможно, он был удалён). Давайте начнём сначала.",
            svc.delivery_bot,
            {"keyboard": [[{"text": "📦 Создать новую заявку"}]], "resize_keyboard": True}
        )
        # сброс состояния
        await users_collection.update_one(
            {"chat_id": chat_id, "type": "delivery"},
            {"$set": {"state": "start", "active_order": None}}
        )
        return
    await users_collection.database["orders"].update_one(
        {"_id": ObjectId(order_id)},
        {"$set": {"phone_number": phone}}
    )

    # 5) Рассчитываем стоимость через calculate_delivery_cost
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
    warehouse     = order.get("warehouse", "")
    quantity      = order.get("cargo_quantity", 0)
    raw_ct = order.get("cargo_type", "")
    # приводим к русским строкам, если встретилось английское
    cargo_type = {
        "boxes":  "Короба",
        "pallets":"Палеты"
    }.get(raw_ct, raw_ct)

    cost = svc.calculate_delivery_cost(
        warehouse,
        cargo_type,
        quantity
    )  # :contentReference[oaicite:0]{index=0}

    await users_collection.database["orders"].update_one(
        {"_id": ObjectId(order_id)},
        {"$set": {"delivery_cost": cost}}
    )

    # 6) Переходим к финальному суммари
    await users_collection.update_one(
        {"chat_id": chat_id, "type": "delivery"},
        {"$set": {"state": "awaiting_order_submit"}}
    )

    # 7) Шлём итоговое сообщение с проверкой данных
    parts = [
        "📋 *Проверьте данные заявки:*",
        f"🏢 *ИП / организация*: {order.get('org_name','—')}",
        f"📍 *Адрес ИП / организации*: {order.get('org_address','—')}",
        f"💳 *Р/С*: {order.get('rs','—')}",
        f"🏦 *БИК*: {order.get('bik','—')}",
        f"📦 *Тип поставки*: {'Короба' if cargo_type=='Короба' else 'Палеты'}",
        f"🔢 *Количество*: {quantity}",
        f"🏬 *Склад*: {warehouse}",
        f"📅 *Дата сдачи*: {delivery_str}",
        f"🚚 *Дата забора*: {pickup_str}",
        f"🏠 *Адрес забора*: {order.get('pickup_address','—')}",
        f"📞 *Телефон*: {phone}",
        f"💰 *Стоимость*: {cost} ₽"
    ]
    review_text = "\n".join(parts)
    keyboard = {
        "keyboard": [
            [{"text": "📨 Отправить заявку"}],
            [{"text": "🔄 Начать заново"}]
        ],
        "resize_keyboard": True
    }

    sent = await svc.send_text(
        chat_id,
        review_text,
        svc.delivery_bot,
        keyboard
    )
    # 8) Сохраняем message_id суммари для возможного удаления
    mid = getattr(sent, "message_id", None) or sent.json().get("result", {}).get("message_id")
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
        await handle_delivery_start(chat_id, user, message=None)
        return

    if text != "📨 Отправить заявку":
        await svc.send_text(
            chat_id,
            "❌ Пожалуйста, нажмите «📨 Отправить заявку» или «🔄 Начать заново».",
            svc.delivery_bot
        )
        return

    order_id = user.get("active_order")
    if not order_id:
        await svc.send_text(
            chat_id,
            "⚠️ Не удалось найти активный заказ. Начните заново.",
            svc.delivery_bot
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
    await svc.delivery_bot.send_chat_action(chat_id, ChatAction.TYPING)
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
            await svc.delivery_bot.delete_message(chat_id, summ_mid)
        except:
            pass

    # Составляем окончательный суммари с номером заявки
    parts = [
        "✅ Ваша заявка успешно отправлена!",
        "📋 Содержимое заявки:",
        "",
        f"🆔 Номер заявки: #{deal_id}",
        f"🏢 ИП / организация: {order.get('org_name', '—')}",
        f"📍 Адрес ИП / организации: {order.get('org_address', '—')}",
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

    # 1) Отправляем суммари без кнопок и сохраняем summ_mid
    sent = await svc.send_text(
        chat_id,
        final_summary,
        svc.delivery_bot
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
        svc.delivery_bot,
        keyboard
    )

@on_command("Оплатить по счету")
async def handle_pay_by_invoice(chat_id: int, user: dict, message: dict):
    await svc.delivery_bot.send_chat_action(chat_id, ChatAction.TYPING)
    # 1) Берём самый свежий заказ в статусе awaiting_payment, где ещё нет ссылки на счёт
    order = await users_collection.database["orders"].find_one(
        {
            "chat_id":    chat_id,
            "status":     "awaiting_payment",
            "invoice_url": {"$exists": False}
        },
        sort=[("created_at", -1)]
    )
    if not order:
        await svc.send_text(
            chat_id,
            "❗ У вас нет заявок, ожидающих оплаты по счету, или счет уже сгенерирован.",
            svc.delivery_bot
        )
        return

    deal_id = order.get("bitrix_deal_id")
    if not deal_id:
        await svc.send_text(
            chat_id,
            "❗ У заявки отсутствует привязка к сделке Bitrix.",
            svc.delivery_bot
        )
        return

    # 2) Генерируем публичную ссылку на счёт
    try:
        url_public = await svc.generate_deal_invoice_public_url(deal_id)
    except Exception as e:
        logger.error("Ошибка при генерации счёта для сделки %s: %s", deal_id, e)
        await svc.send_text(
            chat_id,
            "❌ Не удалось сформировать счёт. Пожалуйста, попробуйте чуть позже.",
            svc.delivery_bot
        )
        return

    # 3) Сохраняем ссылку в ордере
    await users_collection.database["orders"].update_one(
        {"_id": order["_id"]},
        {"$set": {"invoice_url": url_public, "payment_type": "invoice"}}
    )

    # 4) Отправляем клиенту ссылку
    keyboard = {
        "keyboard": [
            [{"text": "📦 Создать новую заявку"}],
            [{"text": "💰 Рассчитать стоимость"}]
        ],
        "resize_keyboard": True
    }
    await svc.send_text(
        chat_id,
        f"📄 Ваш счёт готов и доступен для скачивания:\n{url_public}",
        svc.delivery_bot,
        keyboard
    )

@on_command("Оплатить по СБП")
async def handle_pay_by_sbp(chat_id: int, user: dict, message: dict):
    await svc.delivery_bot.send_chat_action(chat_id, ChatAction.TYPING)
    # 1) Берём самый свежий заказ в статусе awaiting_payment без invoice_url
    order = await users_collection.database["orders"].find_one(
        {
            "chat_id":     chat_id,
            "status":      "awaiting_payment",
            "invoice_url": {"$exists": False}
        },
        sort=[("created_at", -1)]
    )
    if not order:
        await svc.send_text(
            chat_id,
            "❗ У вас нет заявок, ожидающих оплаты по СБП.",
            svc.delivery_bot
        )
        return

    deal_id       = order.get("bitrix_deal_id")
    amount        = order.get("delivery_cost", 0)
    client_name   = order.get("org_name", "")
    orderid       = f"Заказ №{deal_id}"
    service_name  = order.get("service_name", "Услуга")
    client_phone  = order.get("phone_number", "")

    # 2) Получаем токен от PayKeeper
    creds = f"{settings.PAYKEEPER_USER}:{settings.PAYKEEPER_PASSWORD}".encode()
    b64   = base64.b64encode(creds).decode()
    headers = {
        "Content-Type":  "application/x-www-form-urlencoded",
        "Authorization": f"Basic {b64}"
    }

    async with AsyncClient() as client:
        # запрос токена
        r1 = await client.get(
            settings.PAYKEEPER_TOKEN_URL,
            headers=headers
        )
        try:
            r1.raise_for_status()
            token = r1.json().get("token")
            if not token:
                raise ValueError("нет поля token")
        except Exception as e:
            logger.error("Ошибка получения токена PayKeeper: %s — %s", e, r1.text)
            await svc.send_text(
                chat_id,
                "❌ Не удалось получить токен оплаты. Попробуйте позже.",
                svc.delivery_bot
            )
            return

        # 3) Формируем данные и запрашиваем счет
        payment_data = {
            "pay_amount":   amount,
            "clientid":     client_name,
            "orderid":      orderid,
            "service_name": service_name,
            "client_phone": client_phone,
            "token":        token
        }
        body = client.build_request(
            "POST",
            settings.PAYKEEPER_INVOICE_URL,
            data=payment_data,
            headers=headers
        ).content  # сформированный x-www-form-urlencoded
        r2 = await client.post(
            settings.PAYKEEPER_INVOICE_URL,
            data=body,
            headers=headers
        )
        try:
            r2.raise_for_status()
            resp2 = r2.json()
            invoice_id = resp2.get("invoice_id")
            if not invoice_id:
                raise ValueError("нет invoice_id")
        except Exception as e:
            logger.error("Ошибка создания счёта PayKeeper: %s — %s", e, r2.text)
            await svc.send_text(
                chat_id,
                "❌ Не удалось сформировать счёт СБП. Попробуйте позже.",
                svc.delivery_bot
            )
            return

    # 4) Собираем публичную ссылку
    server = settings.PAYKEEPER_INVOICE_URL.split("/change")[0]
    link   = f"{server}/bill/{invoice_id}/"

    # 5) Сохраняем в ордере
    await users_collection.database["orders"].update_one(
        {"_id": ObjectId(order["_id"])},
        {"$set": {
            "invoice_url":  link,
            "payment_type": "SBP"
        }}
    )

    keyboard = {
        "keyboard": [
            [{"text": "📦 Создать новую заявку"}],
            [{"text": "💰 Рассчитать стоимость"}]
        ],
        "resize_keyboard": True
    }
    # 6) Отправляем клиенту ссылку
    await svc.send_text(
        chat_id,
        f"🔗 Ссылка для оплаты заявки #{deal_id}:\n{link}",
        svc.delivery_bot,
        keyboard
    )