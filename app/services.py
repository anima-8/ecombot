from aiogram import Bot
from aiogram.types import ReplyKeyboardMarkup
from datetime import datetime, date, timedelta
from bson import ObjectId
from app.db import users_collection
from app.db import calcs_collection
from app.config import get_settings
from httpx import AsyncClient
from typing import Optional, List, Dict
import logging
settings = get_settings()
logger = logging.getLogger(__name__)

fulfilment_bot = Bot(token=settings.TELEGRAM_FULFILMENT_TOKEN)
delivery_bot = Bot(token=settings.TELEGRAM_DELIVERY_TOKEN)
driver_bot = Bot(token=settings.TELEGRAM_DRIVER_TOKEN)

WAREHOUSES = [
    "Коледино",
    "Подольск",
    "Подольск 4",
    "Тула",
    "Электросталь",
    "Обухово",
    "Казань",
    "Владимир",
    "Рязань",
    "Котовск",
    "Новосемейкино",
]

WAREHOUSE_MAP = {
    "Коледино": 54, "Подольск": 58, "Подольск 4": 60,
    "Электросталь": 62, "Обухово": 64, "Рязань": 244,
    "Тула": 246, "Казань": 66, "Котовск": 248, "Владимир": 253,
    "Новосемейкино": 264
}

INV_WAREHOUSE_MAP = {v: k for k, v in WAREHOUSE_MAP.items()}

DELIVERY_DAYS_BY_WAREHOUSE = {
    # ПН, СР, ПТ
    **{wh: [0, 2, 4] for wh in ["Электросталь", "Обухово", "Рязань", "Владимир"]},
    # ПН только Котовск
    "Котовск": [0],
    # ВТ, ЧТ, СБ
    **{wh: [1, 3, 5] for wh in ["Коледино", "Подольск", "Подольск 4", "Тула"]},
    # ВС
    **{wh: [6] for wh in ["Казань", "Новосемейкино"]},
}

CARGO_TYPE_OPTIONS = ["Короба", "Палеты"]

def format_date(iso: str | None) -> str:
    if not iso:
        return "—"
    return datetime.fromisoformat(iso).strftime("%d.%m.%Y")

async def send_text(chat_id: int, text: str, bot: Bot, reply_markup: ReplyKeyboardMarkup = None, parse_mode: str = "Markdown"):
    message = await bot.send_message(
        chat_id=chat_id,
        text=text,
        reply_markup=reply_markup,
        parse_mode=parse_mode
    )
    return message

async def send_intro_message(chat_id: int) -> None:
    text = (
        "Для создания заявки потребуется указать следующие данные:\n"
        "    - ИНН ИП / организации отправителя\n"
        "    - Рассчетный счет ИП / организации отправителя\n"
        "    - БИК\n"
        "    - Склад разгрузки\n"
        "    - Необходимая дата разгрузки\n"
        "    - Количество коробов / палет\n"
        "    - Адрес забора поставки\n"
        "    - Контактный номер телефона отправителя"
    )
    keyboard = {"keyboard": [[{"text": "📦 Создать заявку"}], [{"text": "🔄 Начать заново"}]], "resize_keyboard": True}
    await send_text(chat_id, text, delivery_bot, keyboard)

async def prompt_delivery_date_selection(
    chat_id: int,
    bot,
    warehouse: str
) -> bool:
    """
    Показывает первые 6 уникальных дат сдачи поставки
    (только начиная с завтрашнего дня) в виде ReplyKeyboardMarkup.
    """
    # calculate_schedule теперь начинается с завтра
    slots = calculate_schedule(warehouse)
    if not slots:
        await send_text(
            chat_id,
            "⛔ Нет доступных дат сдачи поставки на ближайшие 2 недели.",
            bot,
            {"keyboard":[[{"text":"🔄 Начать заново"}]], "resize_keyboard": True}
        )
        return False

    unique_dates: List[str] = []
    for slot in slots:
        d_str = slot["delivery"].strftime("%d.%m.%Y")
        if d_str not in unique_dates:
            unique_dates.append(d_str)
        if len(unique_dates) >= 6:
            break

    rows = [unique_dates[i : i + 2] for i in range(0, len(unique_dates), 2)]
    buttons = [[{"text": d} for d in row] for row in rows]
    buttons.append([{"text": "🔄 Начать заново"}])
    keyboard = {"keyboard": buttons, "resize_keyboard": True}

    await send_text(
        chat_id,
        "📅 Выберите дату сдачи поставки:",
        bot,
        keyboard
    )
    return True

async def send_cargo_type_selection(chat_id: int, bot) -> None:
    keyboard = {
        "keyboard": [
            [{"text": "📦 Короба"}, {"text": "🧱 Палеты"}],
            [{"text": "🔄 Начать заново"}]
        ],
        "resize_keyboard": True
    }
    await send_text(chat_id, "📦 Выберите тип поставки:", bot, keyboard)

async def init_calc(chat_id: int, marketplace: str) -> ObjectId:
    """
    Создаёт в базе новую запись расчёта и возвращает её _id.
    Поля можно дополнять по мере поступления данных.
    """
    calc_doc = {
        "user_id": chat_id,
        "marketplace": marketplace,
        "created_at": datetime.utcnow(),
    }
    result = await calcs_collection.insert_one(calc_doc)
    return result.inserted_id

def calculate_schedule(
    warehouse: str,
    start_date: date = None,
    days_ahead: int = 14
) -> List[Dict[str, date]]:
    """
    Возвращает список словарей с ключами:
      - 'delivery': дата перевозки (строго > сегодня)
      - 'pickup':   дата забора (строго > сегодня)
    По умолчанию start_date = завтра.
    """
    if start_date is None:
        start_date = date.today() + timedelta(days=1)

    result: List[Dict[str, date]] = []
    delivery_days = [0] if warehouse == "Котовск" else DELIVERY_DAYS_BY_WAREHOUSE.get(warehouse, [])

    for offset in range(days_ahead):
        d = start_date + timedelta(days=offset)
        if d.weekday() in delivery_days:
            for pickup_date in get_pickup_dates(warehouse, d):
                result.append({"delivery": d, "pickup": pickup_date})

    return result

def get_pickup_dates(
    warehouse: str,
    delivery_date: date
) -> List[date]:
    """
    Возвращает доступные даты забора (строго > сегодня):
      - Котовск: предыдущий воскресный день + день доставки
      - Казань/Новосемейкино: пятница перед доставкой
      - Все остальные: день доставки
    Отфильтровывает любые даты ≤ сегодня.
    """
    from datetime import timedelta

    today = date.today()
    candidates: List[date] = []

    if warehouse == "Котовск":
        prev_sunday = delivery_date
        while prev_sunday.weekday() != 6:
            prev_sunday -= timedelta(days=1)
        candidates = [prev_sunday, delivery_date]

    elif warehouse in {"Казань", "Новосемейкино"}:
        prev_friday = delivery_date
        while prev_friday.weekday() != 4:
            prev_friday -= timedelta(days=1)
        candidates = [prev_friday]

    else:
        candidates = [delivery_date]

    # оставляем только те, что строго после сегодняшнего дня
    return [d for d in candidates if d > today]

async def prompt_warehouse_selection(chat_id: int, bot: Bot) -> None:
    """
    Просит пользователя выбрать склад для расчёта стоимости.
    """
    # разбиваем на строки по 2 склада
    rows = [WAREHOUSES[i : i + 2] for i in range(0, len(WAREHOUSES), 2)]
    buttons = [[{"text": w} for w in row] for row in rows]
    # добавляем кнопку перезапуска
    buttons.append([{"text": "🔄 Начать заново"}])
    keyboard = {"keyboard": buttons, "resize_keyboard": True}
    await send_text(
        chat_id,
        "🏬 Выберите место сдачи поставки:",
        bot,
        keyboard
    )

async def prompt_cargo_type_selection(chat_id: int, bot: Bot) -> None:
    """
    Просит пользователя выбрать тип поставки:
    2 кнопки в ряд: Короба | Палеты
    """
    # разбиваем на строки по 2 элемента
    rows = [CARGO_TYPE_OPTIONS[i : i + 2] for i in range(0, len(CARGO_TYPE_OPTIONS), 2)]
    buttons = [
        [{"text": opt} for opt in row]
        for row in rows
    ]
    buttons.append([{"text": "🔄 Начать заново"}])
    keyboard = {"keyboard": buttons, "resize_keyboard": True}
    await send_text(
        chat_id,
        "🚛 Выберите тип поставки:",
        bot,
        keyboard
    )

def calculate_delivery_cost(
    warehouse: str,
    cargo_type: str,
    quantity: int
) -> int:
    """
    Вычисляет стоимость доставки.

    Параметры:
      - warehouse: название склада (например, "Коледино", "Новосемейкино" и т.д.)
      - cargo_type: "Короба" или "Палеты"
      - quantity: количество коробов или палет (целое >= 0)

    Возвращает:
      - общую стоимость в рублях (int)
    """
   # Группы складов для коробов
    box_group1 = {"Коледино", "Электросталь", "Подольск", "Подольск 4", "Обухово"}
    box_group2 = {"Владимир", "Тула", "Рязань"}
    box_group3 = {"Казань", "Котовск"}
    box_group4 = {"Новосемейкино"}
    pickup_fee = 500  # единая плата за забор для коробов

    # Группы складов для палет
    pal_group1 = box_group1
    pal_group2 = box_group2
    pal_group3 = {"Казань", "Котовск"}
    pal_group4 = {"Новосемейкино"}

    # Обработка нулевого количества
    if quantity <= 0:
        return 0

    # Расчёт базовой стоимости
    if cargo_type == "Короба":
        if warehouse in box_group1:
            rate = 200
        elif warehouse in box_group2:
            rate = 300
        elif warehouse in box_group3:
            rate = 500
        elif warehouse in box_group4:
            rate = 600
        else:
            raise ValueError(f"Неизвестный склад для коробов: {warehouse}")
        base_cost = rate * quantity + pickup_fee

    elif cargo_type == "Палеты":
        if warehouse in pal_group1:
            # 3000 за первую + 1250 за каждую дополнительную
            base_cost = 3000 + max(0, quantity - 1) * 1250
        elif warehouse in pal_group2:
            # 4000 за первую + 2000 за каждую дополнительную
            base_cost = 4000 + max(0, quantity - 1) * 2000
        elif warehouse in pal_group3:
            # 6000 за каждую палету
            base_cost = 6000 * quantity
        elif warehouse in pal_group4:
            # 7500 за каждую палету
            base_cost = 7500 * quantity
        else:
            raise ValueError(f"Неизвестный склад для палет: {warehouse}")

    else:
        raise ValueError(f"Неизвестный тип поставки: {cargo_type}")

    return base_cost

def calculate_delivery_cost_fulfilment(
    warehouse: str,
    cargo_type: str,
    quantity: int
) -> int:
    """
    Вычисляет стоимость доставки.

    Параметры:
      - warehouse: название склада (например, "Коледино", "Новосемейкино" и т.д.)
      - cargo_type: "Короба" или "Палеты"
      - quantity: количество коробов или палет (целое >= 0)

    Возвращает:
      - общую стоимость в рублях (int)
    """
   # Группы складов для коробов
    box_group1 = {"Коледино", "Электросталь", "Подольск", "Подольск 4", "Обухово"}
    box_group2 = {"Владимир", "Тула", "Рязань"}
    box_group3 = {"Казань", "Котовск"}
    box_group4 = {"Новосемейкино"}
    pickup_fee = 500  # единая плата за забор для коробов

    # Группы складов для палет
    pal_group1 = box_group1
    pal_group2 = box_group2
    pal_group3 = {"Казань", "Котовск"}
    pal_group4 = {"Новосемейкино"}

    # Обработка нулевого количества
    if quantity <= 0:
        return 0

    # Расчёт базовой стоимости
    if cargo_type == "Короба":
        if warehouse in box_group1:
            rate = 182
        elif warehouse in box_group2:
            rate = 245
        elif warehouse in box_group3:
            rate = 400
        elif warehouse in box_group4:
            rate = 500
        else:
            raise ValueError(f"Неизвестный склад для коробов: {warehouse}")
        base_cost = rate * quantity + pickup_fee

    elif cargo_type == "Палеты":
        if warehouse in pal_group1:
            # 3000 за первую + 1250 за каждую дополнительную
            base_cost = 3000 + max(0, quantity - 1) * 1250
        elif warehouse in pal_group2:
            # 4000 за первую + 2000 за каждую дополнительную
            base_cost = 4000 + max(0, quantity - 1) * 2000
        elif warehouse in pal_group3:
            # 6000 за каждую палету
            base_cost = 6000 * quantity
        elif warehouse in pal_group4:
            # 7500 за каждую палету
            base_cost = 7500 * quantity
        else:
            raise ValueError(f"Неизвестный склад для палет: {warehouse}")

    else:
        raise ValueError(f"Неизвестный тип поставки: {cargo_type}")

    return base_cost

async def prompt_pickup_address_selection(chat_id: int, bot: Bot) -> None:
    orders_cursor = users_collection.database["orders"].find({"chat_id": chat_id})
    addresses = set()

    async for order in orders_cursor:
        addr = order.get("pickup_address")
        if not addr:
            continue
        clean = addr.strip()
        # Пропускаем «адреса», которые состоят только из цифр
        if clean.isdigit():
            continue
        addresses.add(clean)

    if addresses:
        buttons = [[{"text": f"📍 {a}"}] for a in sorted(addresses)]
        buttons.append([{"text": "🔄 Начать заново"}])
        keyboard = {"keyboard": buttons, "resize_keyboard": True}
        await send_text(
            chat_id,
            "📍 Выберите адрес забора или введите новый:",
            bot,
            keyboard
        )
    else:
        keyboard = {
            "keyboard": [[{"text": "🔄 Начать заново"}]],
            "resize_keyboard": True
        }
        await send_text(
            chat_id,
            "✏️ Введите адрес забора поставки:\n_Пример: Красногорск, ул. Карбышева, 9 к 2 под 3_",
            bot,
            keyboard
        )

async def prompt_phone_number_selection(chat_id: int, bot: Bot) -> None:
    cursor = users_collection.database["orders"].find({"chat_id": chat_id})
    phones = set()

    async for order in cursor:
        phone = order.get("phone_number")
        if phone:
            phones.add(phone.strip())

    buttons = [[{"text": f"📞 {p}"}] for p in sorted(phones)] if phones else []
    buttons.append([{
        "text": "📲 Отправить контакт",
        "request_contact": True
    }])
    buttons.append([{"text": "🔄 Начать заново"}])

    keyboard = {"keyboard": buttons, "resize_keyboard": True}
    await send_text(
        chat_id,
        "📞 Выберите номер телефона, поделитесь контактом или введите номер вручную в формате +7XXXXXXXXXX",
        bot,
        keyboard
    )

async def send_intro_message_ff(chat_id: int) -> None:
    text = (
        "Для создания заявки потребуется указать следующие данные:\n"
        "    - ИНН организации отправителя\n"
        "    - Рассчетный счет организации отправителя\n"
        "    - БИК\n"
        "    - Склад разгрузки\n"
        "    - Необходимая дата разгрузки\n"
        "    - Количество коробов / палет\n"
        "    - Адрес забора поставки\n"
        "    - Контактный номер телефона отправителя"
    )
    keyboard = {"keyboard": [[{"text": "Создать новую заявку"}]], "resize_keyboard": True}
    await send_text(chat_id, text, fulfilment_bot, keyboard)

async def calculate_delivery_cost_ff(chat_id: int) -> int:
    # 1) Берём профиль пользователя с учётом типа
    user = await users_collection.find_one(
        {"chat_id": chat_id, "type": "fulfilment"}
    )
    order = None

    # 2) Если в профиле есть active_order, пробуем по нему найти заказ
    order_id = user.get("active_order") if user else None
    if order_id:
        try:
            order = await users_collection.database["orders"].find_one(
                {"_id": ObjectId(order_id)}
            )
        except Exception:
            order = None

    # 3) Если не нашли — берём самый свежий «in_progress» заказ этого чата
    if not order:
        order = await users_collection.database["orders"].find_one(
            {"chat_id": chat_id, "status": "in_progress"},
            sort=[("created_at", -1)]
        )

    # 4) Если всё ещё нет — возвращаем 0
    if not order:
        return 0

    # 5) Теперь гарантированно есть order, берём поля
    warehouse  = order.get("warehouse", "")
    cargo_type = order.get("cargo_type")
    quantity   = order.get("cargo_quantity", 0)
    cost = 0

    if cargo_type == "boxes":
        if warehouse in ["Коледино", "Электросталь", "Подольск", "Подольск 4", "Обухово"]:
            cost = quantity * 182
        elif warehouse in ["Тула", "Рязань", "Владимир"]:
            cost = quantity * 245
        elif warehouse in ["Казань", "Котовск"]:
            cost = quantity * 400
        elif warehouse in ["Новосемейкино"]:
            cost = quantity * 500
        cost += 500

    elif cargo_type == "pallets":
        if warehouse in ["Коледино", "Электросталь", "Подольск", "Подольск 4", "Обухово"]:
            if quantity >= 1:
                cost = 3000 + 1250 * (quantity - 1)
        elif warehouse in ["Тула", "Рязань", "Владимир"]:
            if quantity >= 1:
                cost = 4000 + 2000 * (quantity - 1)
        elif warehouse in ["Казань", "Котовск"]:
            cost = 6000 * quantity
        elif warehouse in ["Новосемейкино"]:
            cost = 7500 * quantity

    return cost

async def send_to_bitrix(order: dict, telegram_username: str) -> str:
    """
    1) Ищем компанию по org_name, если не нашли — создаём с реквизитами.
    2) Создаём сделку (deal) в стадии NEW с полями из плоского order.
    Сохраняем bitrix_deal_id в заказе и возвращаем его.
    """
    async with AsyncClient() as client:
        # ——————————————————————————————
        # 1) Найти или создать компанию
        # — crm.company.list
        company_name = order["org_name"]
        payload = {"filter": {"TITLE": company_name}}
        logger.info("Bitrix → crm.company.list: %s", payload)
        resp = await client.post(
            f"{settings.BITRIX_WEBHOOK_URL}crm.company.list",
            json=payload
        )
        resp.raise_for_status()
        items = resp.json().get("result", [])

        if items:
            company_id = items[0]["ID"]
            logger.info("Bitrix: найдено company_id=%s", company_id)
        else:
            # — crm.company.add
            payload = {
                "fields": {
                    "TITLE":   company_name,
                    "PHONE":   [{"VALUE": order["phone_number"], "VALUE_TYPE": "WORK"}],
                    "IM":      [{"VALUE": telegram_username, "VALUE_TYPE": "TELEGRAM"}],
                }
            }
            logger.info("Bitrix → crm.company.add: %s", payload)
            resp = await client.post(
                f"{settings.BITRIX_WEBHOOK_URL}crm.company.add",
                json=payload
            )
            resp.raise_for_status()
            company_id = resp.json()["result"]
            logger.info("Bitrix: создана компания company_id=%s", company_id)

            # — crm.requisite.add (основной реквизит)
            payload = {
                "fields": {
                    "ENTITY_TYPE_ID":   4,   # 4 = Company
                    "ENTITY_ID":        company_id,
                    "PRESET_ID":        1,
                    "NAME":             "Основной реквизит",
                    "RQ_INN":           order["inn"],
                    "RQ_COMPANY_NAME":  order["org_name"],
                    "RQ_COMPANY_FULL_NAME": order["org_name"]
                }
            }
            logger.info("Bitrix → crm.requisite.add: %s", payload)
            resp = await client.post(
                f"{settings.BITRIX_WEBHOOK_URL}crm.requisite.add",
                json=payload
            )
            resp.raise_for_status()
            requisite_id = resp.json()["result"]

            # — crm.address.add (юридический адрес)
            payload = {
                "fields": {
                    "TYPE_ID":         6,  # Legal
                    "ENTITY_TYPE_ID":  8,  # Requisite
                    "ENTITY_ID":       requisite_id,
                    "COUNTRY":         "RU",
                    "ADDRESS_1":       order["org_address"]
                }
            }
            logger.info("Bitrix → crm.address.add: %s", payload)
            resp = await client.post(
                f"{settings.BITRIX_WEBHOOK_URL}crm.address.add",
                json=payload
            )
            resp.raise_for_status()

            # — crm.requisite.bankdetail.add (банковские реквизиты)
            payload = {
                "fields": {
                    "ENTITY_ID":       requisite_id,
                    "NAME": "Банк",
                    "RQ_BIK":          order["bik"],
                    "RQ_ACC_NUM":      order["rs"],
                    "RQ_ACC_CURRENCY": "RUB"
                }
            }
            logger.info("Bitrix → crm.requisite.bankdetail.add: %s", payload)
            resp = await client.post(
                f"{settings.BITRIX_WEBHOOK_URL}crm.requisite.bankdetail.add",
                json=payload
            )
            resp.raise_for_status()

        # ——————————————————————————————
        # 2) Создать сделку
        # — crm.deal.add
        dt_now = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
        warehouse_id = WAREHOUSE_MAP.get(order["warehouse"])

        if order.get("type") == "fulfilment":
            deal_title = f"Фулфилмент → {order['warehouse']}, {order['org_name']}"
        else:
            deal_title = f"Доставка → {order['warehouse']}, {order['org_name']}"

        deal_fields = {
            "TITLE":         deal_title,
            "STAGE_ID":      "NEW",
            "OPPORTUNITY":   order["delivery_cost"],
            "CURRENCY_ID":   "RUB",
            "COMPANY_ID":    company_id,
            "DATE_CREATE":   dt_now,
            "ASSIGNED_BY_ID": 1,
            "CATEGORY_ID": 2,
            "UF_CRM_1729569844156": 114,
            "UF_CRM_1724923450176": order["pickup_address"],
            "UF_CRM_1724923582938": order["cargo_quantity"],
            "UF_CRM_1751787406541": 252 if order["cargo_type"]=="pallets" else 250,
            "UF_CRM_1724923635379": order["delivery_date"],
            "UF_CRM_1724923649863": order["pickup_date"],
            "UF_CRM_1724923726538": order["chat_id"],
            "UF_CRM_1724923553452": warehouse_id,
        }
        if order.get("type") == "fulfilment":
            deal_fields["UF_CRM_1751787327257"] = 1
        
        logger.info("Bitrix → crm.deal.add: %s", deal_fields)
        resp = await client.post(
            f"{settings.BITRIX_WEBHOOK_URL}crm.deal.add",
            json={"fields": deal_fields}
        )
        resp.raise_for_status()
        deal_id = resp.json().get("result")
        logger.info("Bitrix: создана сделка deal_id=%s", deal_id)

        # 3) Сохраняем deal_id в Mongo для последующих обновлений
        await users_collection.database["orders"].update_one(
            {"_id": ObjectId(order["_id"])},
            {"$set": {"bitrix_deal_id": str(deal_id)}}
        )

        return str(deal_id)

async def set_deal_service_row(deal_id: str) -> None:
    async with AsyncClient() as client:
        # 1) Получаем сделку
        resp = await client.post(
            f"{settings.BITRIX_WEBHOOK_URL}crm.deal.get",
            json={"id": deal_id}
        )
        resp.raise_for_status()
        deal = resp.json().get("result", {})

        # Извлекаем поля
        price             = deal.get("OPPORTUNITY", 0)
        wh_code           = deal.get("UF_CRM_1724923553452")
        raw_deliv_date    = deal.get("UF_CRM_1724923649863")
        contract_number   = deal.get("UF_CRM_1751973413773")
        raw_contract_date = deal.get("UF_CRM_1752132156032")

        # 2) Форматим
        warehouse = INV_WAREHOUSE_MAP.get(int(wh_code), str(wh_code))
        def fmt_date(d: Optional[str]) -> Optional[str]:
            try:
                return datetime.fromisoformat(d).strftime("%d.%m.%Y")
            except Exception:
                return None

        deliv_date = fmt_date(raw_deliv_date)
        contract_date = fmt_date(raw_contract_date)

        # 3) Собираем название услуги
        if contract_number:
            name = f"Оплата по договору №{contract_number}"
            if contract_date:
                name += f" от {contract_date}"
            name += ", транспортировка "
        else:
            name = "Транспортировка "
        if deliv_date:
            name += f"{deliv_date} "
        name += warehouse

        # 4) Формируем rows и пушим в Bitrix
        product_rows = [{
            "PRODUCT_NAME": name,
            "PRICE":        price,
            "QUANTITY":     1
        }]

        resp2 = await client.post(
            f"{settings.BITRIX_WEBHOOK_URL}crm.deal.productrows.set",
            json={"id": deal_id, "rows": product_rows}
        )
        if resp2.is_error:
            logger.error("Bitrix productrows.set failed: %s", resp2.text)
        else:
            logger.info("Bitrix productrows.set response: %s", resp2.json())
        return name

async def generate_deal_invoice_public_url(deal_id: str) -> str:
    """
    Генерирует в Bitrix документ «Счёт» по сделке и возвращает публичный URL для скачивания.
    
    1) crm.documentgenerator.document.add
    2) crm.documentgenerator.document.enablepublicurl
    3) crm.documentgenerator.document.get.json
    
    :param deal_id: ID сделки в Битрикс
    :raises RuntimeError: при ошибках API или отсутствии нужных полей в ответе
    :return: публичная ссылка на документ
    """
    async with AsyncClient() as client:
        # 1. Создаём документ
        doc_payload = {
            "templateId":    4,        # ID шаблона «Счёт»
            "entityTypeId":  2,        # 2 = Deal
            "entityId":      deal_id,
            "values":        [],       # доп. поля
            "stampsEnabled": 0         # без печати/подписи
        }
        logger.info("Bitrix → crm.documentgenerator.document.add: %r", doc_payload)
        resp = await client.post(
            f"{settings.BITRIX_WEBHOOK_URL}crm.documentgenerator.document.add",
            json=doc_payload
        )
        resp.raise_for_status()
        result = resp.json().get("result", {})
        document = result.get("document", {})
        document_id = document.get("id")
        if not document_id:
            logger.error("Bitrix did not return document.id: %s", resp.text)
            raise RuntimeError("Не удалось получить document_id от Bitrix")

        logger.info("Создан документ «Счёт», ID=%s", document_id)

        # 2. Включаем публичный доступ
        enable_payload = {"id": document_id, "status": 1}
        logger.info("Bitrix → crm.documentgenerator.document.enablepublicurl: %r", enable_payload)
        resp = await client.post(
            f"{settings.BITRIX_WEBHOOK_URL}crm.documentgenerator.document.enablepublicurl",
            json=enable_payload
        )
        resp.raise_for_status()
        logger.info("Публичная ссылка включена для документа %s", document_id)

        # 3. Получаем публичный URL
        get_payload = {"id": document_id}
        logger.info("Bitrix → crm.documentgenerator.document.get.json: %r", get_payload)
        resp = await client.post(
            f"{settings.BITRIX_WEBHOOK_URL}crm.documentgenerator.document.get.json",
            json=get_payload
        )
        resp.raise_for_status()
        doc = resp.json().get("result", {}).get("document", {})
        url_public = doc.get("publicUrl")
        if not url_public:
            logger.error("Bitrix did not return publicUrl: %s", resp.text)
            raise RuntimeError("Не удалось получить publicUrl от Bitrix")

        logger.info("Сгенерирован публичный URL счёта: %s", url_public)
        return url_public