# app/handlers/delivery_calc.py
import logging
from datetime import datetime
from bson import ObjectId

from app.handlers.decorators import on_command, on_state
from app.db import users_collection, calcs_collection
import app.services as svc

logger = logging.getLogger(__name__)
logger.info("Loaded delivery_calc.py")

# — Обработка первой команды калькулятора / кнопки
@on_command("/calc")
@on_command("💰 Рассчитать стоимость")
async def handle_delivery_calc(chat_id, user, message):
    # сразу создаём расчёт для Wildberries
    calc_id = await svc.init_calc(chat_id, "Wildberries")
    # сохраняем active_calc и назначаем следующее состояние
    await users_collection.update_one(
        {"chat_id": chat_id, "type": "delivery"},
        {"$set": {"active_calc": calc_id, "state": "delivery_calc_warehouse"}}
    )
    # сразу показываем выбор склада
    await svc.prompt_warehouse_selection(chat_id, svc.delivery_bot)


# — Обработка выбора склада
@on_state("delivery_calc_warehouse")
async def handle_warehouse_selected(chat_id, user, payload):
    warehouse = payload
    # Проверяем только по единому списку WAREHOUSES
    if warehouse not in svc.WAREHOUSES:
        await svc.send_text(
            chat_id,
            "Пожалуйста, выберите склад из списка.",
            svc.delivery_bot
        )
        await svc.prompt_warehouse_selection(chat_id, svc.delivery_bot)
        return

    # Сохраняем выбранный склад в документ рассчёта
    calc_id = user.get("active_calc")
    await calcs_collection.update_one(
        {"_id": ObjectId(calc_id)},
        {"$set": {"warehouse": warehouse}}
    )

    # Переходим к состоянию выбора типа груза
    await users_collection.update_one(
        {"chat_id": chat_id, "type": "delivery"},
        {"$set": {"state": "delivery_calc_cargo_type"}}
    )
    await svc.prompt_cargo_type_selection(chat_id, svc.delivery_bot)

@on_state("delivery_calc_cargo_type")
async def handle_cargo_type_selected(chat_id, user, payload):
    cargo_type = payload  # "Короба" или "Палеты"

    # Проверяем, что пользователь выбрал одну из опций
    if cargo_type not in svc.CARGO_TYPE_OPTIONS:
        await svc.send_text(
            chat_id,
            "❗ Пожалуйста, выберите тип поставки из списка ниже:",
            svc.delivery_bot
        )
        await svc.prompt_cargo_type_selection(chat_id, svc.delivery_bot)
        return

    # Сохраняем тип поставки в документе расчёта
    calc_id = user.get("active_calc")
    await calcs_collection.update_one(
        {"_id": ObjectId(calc_id)},
        {"$set": {"cargo_type": cargo_type}}
    )

    # Переходим в состояние ввода количества
    await users_collection.update_one(
        {"chat_id": chat_id, "type": "delivery"},
        {"$set": {"state": "delivery_calc_quantity"}}
    )

    # Формируем текст с учётом выбранного типа
    label = "коробов" if cargo_type == "Короба" else "палет"
    # Отправляем запрос пользователю, пример курсивом
    await svc.send_text(
        chat_id,
        f"Введите количество {label}\n_Пример: 7_",
        svc.delivery_bot
    )

@on_state("delivery_calc_quantity")
async def handle_quantity_input(chat_id, user, payload):
    # Получаем ID расчёта из профиля
    calc_id = user.get("active_calc")
    if not calc_id:
        await svc.send_text(
            chat_id,
            "❗ Не найден активный расчёт. Пожалуйста, начните сначала: нажмите /calc или кнопку «💰 Рассчитать стоимость».",
            svc.delivery_bot
        )
        return

    # Загружаем документ расчёта
    calc = await calcs_collection.find_one({"_id": ObjectId(calc_id)})
    if not calc:
        await svc.send_text(
            chat_id,
            "❗ Не удалось загрузить ваш расчёт. Пожалуйста, начните заново: нажмите /calc.",
            svc.delivery_bot
        )
        return

    # Проверяем, что склад был выбран
    warehouse = calc.get("warehouse")
    if not warehouse:
        await svc.send_text(
            chat_id,
            "❗ Пожалуйста, выберите склад перед вводом количества.",
            svc.delivery_bot
        )
        await users_collection.update_one(
            {"chat_id": chat_id, "type": "delivery"},
            {"$set": {"state": "delivery_calc_warehouse"}}
        )
        await svc.prompt_warehouse_selection(chat_id, svc.delivery_bot)
        return

    cargo_type = calc.get("cargo_type")

    # Валидируем ввод как положительное целое
    label = "коробов" if cargo_type == "Короба" else "палет"
    text  = payload.strip()
    try:
        quantity = int(text)
        if quantity <= 0:
            raise ValueError
    except ValueError:
        await svc.send_text(
            chat_id,
            f"❗ Введите корректное количество {label} (положительное целое).\n_Пример: 7_",
            svc.delivery_bot
        )
        return

    # Сохраняем количество и делаем расчёт
    await calcs_collection.update_one(
        {"_id": ObjectId(calc_id)},
        {"$set": {"quantity": quantity}}
    )
    schedule = svc.calculate_schedule(warehouse)   # список словарей с pickup/delivery
    cost     = svc.calculate_delivery_cost(warehouse, cargo_type, quantity)

    # Формируем и отправляем ответ
    lines = [
        f"Стоимость доставки: {cost} руб.\n",
        "Доступные даты забора / сдачи поставки на склад:"
    ]
    for item in schedule:
        pickup   = item["pickup"].strftime("%d.%m.%Y")
        delivery = item["delivery"].strftime("%d.%m.%Y")
        lines.append(f"{pickup} / {delivery}")

    message = "\n".join(lines)
    buttons = [
        [{"text": "📦 Создать новую заявку"}],
        [{"text": "💰 Рассчитать стоимость"}],
    ]
    keyboard = {"keyboard": buttons, "resize_keyboard": True}

    await svc.send_text(chat_id, message, svc.delivery_bot, keyboard)
