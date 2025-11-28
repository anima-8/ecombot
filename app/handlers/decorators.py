from typing import Callable
from functools import wraps

COMMAND_HANDLERS: dict[str, dict[str, Callable]] = {}
STATE_HANDLERS:   dict[str, dict[str, Callable]] = {}
CALLBACK_HANDLERS: dict[str, dict[str, Callable]] = {}
CALLBACK_PREFIXES: dict[str, list[tuple[str, Callable]]] = {}  # ← поддержка префиксов

def on_command(cmd: str):
    def decorator(func: Callable):
        module_name = func.__module__.split('.')[-1]
        bot_type = module_name.split('_', 1)[0]
        COMMAND_HANDLERS.setdefault(bot_type, {})[cmd] = func
        @wraps(func)
        async def wrapper(chat_id, user, message):
            return await func(chat_id, user, message)
        return wrapper
    return decorator

def on_state(state: str):
    def decorator(func: Callable):
        module_name = func.__module__.split('.')[-1]
        bot_type = module_name.split('_', 1)[0]
        STATE_HANDLERS.setdefault(bot_type, {})[state] = func
        @wraps(func)
        async def wrapper(chat_id, user, payload):
            return await func(chat_id, user, payload)
        return wrapper
    return decorator

def on_callback(data: str):
    def decorator(func: Callable):
        module_name = func.__module__.split('.')[-1]
        bot_type = module_name.split('_', 1)[0]

        # если data заканчивается на # — значит это префикс
        if data.endswith("#"):
            CALLBACK_PREFIXES.setdefault(bot_type, []).append((data, func))
        else:
            CALLBACK_HANDLERS.setdefault(bot_type, {})[data] = func

        @wraps(func)
        async def wrapper(chat_id, user, callback_query):
            return await func(chat_id, user, callback_query)
        return wrapper
    return decorator
