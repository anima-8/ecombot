#!/bin/bash

# Переход в директорию проекта
cd /opt/ecom

# Активировать виртуальное окружение, если используем (опционально)
# source venv/bin/activate

# Запустить приложение
uvicorn app.main:app \
  --host 0.0.0.0 \
  --port 8000 \
  --workers 2 \
  --proxy-headers \
  --forwarded-allow-ips="*"
