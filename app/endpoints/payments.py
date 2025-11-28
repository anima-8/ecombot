# app/endpoints/payments.py

import re
import hashlib
import logging
from fastapi import APIRouter, Request, HTTPException
from httpx import AsyncClient
from app.config import get_settings
from app.db import users_collection

router = APIRouter()
logger = logging.getLogger("payments")
settings = get_settings()

@router.post("/payments")
async def payments_hook(request: Request):
    form = await request.form()
    data = dict(form)

    payment_id  = data.get("id")
    raw_sum     = data.get("sum")
    clientid    = data.get("clientid", "")
    orderid_raw = data.get("orderid", "")
    signature   = data.get("key", "")

    # 2) Базовая валидация
    if not payment_id or not raw_sum or not signature:
        logger.warning("Missing required params in payments hook: %r", data)
        raise HTTPException(status_code=400, detail="Missing required parameters")

    # 3) Проверка подписи: md5(id + sum + clientid + orderid + secret)
    sign_str = f"{payment_id}{raw_sum}{clientid}{orderid_raw}{settings.PAYKEEPER_SECRET}"
    expected = hashlib.md5(sign_str.encode("utf-8")).hexdigest()
    if expected != signature.lower():
        logger.error("Invalid signature: got %s, expected %s", signature, expected)
        raise HTTPException(status_code=400, detail="Invalid signature")

    # 4) Извлекаем numeric deal_id из orderid ("Заказ №5584" → "5584")
    m = re.search(r"(\d+)", orderid_raw or "")
    if not m:
        logger.error("Cannot parse orderid from '%s'", orderid_raw)
        raise HTTPException(status_code=400, detail="Invalid orderid")
    deal_id = m.group(1)

    # 5) Находим заказ в Mongo и сверяем сумму (delivery_cost)
    order = await users_collection.database["orders"].find_one({"bitrix_deal_id": deal_id})
    if not order:
        logger.error("Order with deal_id=%s not found in DB", deal_id)
        raise HTTPException(status_code=404, detail="Order not found")

    try:
        paid = float(raw_sum)
    except ValueError:
        logger.error("Invalid sum value: %s", raw_sum)
        raise HTTPException(status_code=400, detail="Invalid sum")
    expected_cost = order.get("delivery_cost", 0)
    if abs(paid - expected_cost) > 0.01:
        logger.error(
            "Sum mismatch for deal %s: paid=%s expected=%s",
            deal_id, paid, expected_cost
        )
        raise HTTPException(status_code=400, detail="Sum mismatch")

    # 6) Переводим сделку в Bitrix в стадию C2:WON
    async with AsyncClient() as client:
        try:
            resp = await client.post(
                f"{settings.BITRIX_WEBHOOK_URL}crm.deal.update",
                json={"id": deal_id, "fields": {"STAGE_ID": "C2:WON"}}
            )
            resp.raise_for_status()
        except Exception as e:
            logger.error("Bitrix deal.update failed for deal %s: %s", deal_id, e)
            raise HTTPException(status_code=500, detail="Failed to update Bitrix")

    logger.info("Payment hook processed successfully for deal %s", deal_id)
    return {"ok": True}

