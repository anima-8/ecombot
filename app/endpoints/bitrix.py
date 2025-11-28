# app/endpoints/bitrix.py

from fastapi import APIRouter, Request
from app.handlers import bitrix as bitrix_handlers
import logging
logger = logging.getLogger("bitrix")
router = APIRouter()

@router.api_route("/bitrix", methods=["GET", "POST"])
async def bitrix_hook(request: Request):
    params = dict(request.query_params)
    stage = params.get("stage")
    logger.warning(f"[HOOK] Получен запрос: stage={stage}, params={params}")
    if stage == "set_driver":
        await bitrix_handlers.handle_set_driver(params)
        return {"ok": True, "message": "driver assigned"}

    if stage == "change_driver":
        logger.warning("[HOOK] Вход в handle_change_driver")
        await bitrix_handlers.handle_change_driver(params)
        return {"ok": True, "message": "driver changed"}

    if stage == "payed":
        logger.warning("[HOOK] Вход в handle_payed")
        await bitrix_handlers.handle_payed(params)
        return {"ok": True, "message": "payment received"}
    
    return {"ok": True, "message": "unhandled"}