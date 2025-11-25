# syntax=docker/dockerfile:1
FROM python:3.11-slim

WORKDIR /app

# 1) Ставим Python-зависимости
COPY api/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# 2) Копируем код по папкам (быстрее кэшируется и не тащим лишнее)
COPY api /app/api
COPY retriever /app/retriever
COPY llm /app/llm
COPY utils /app/utils

# (если есть другие необходимые папки/файлы — добавь их аналогично)
# COPY data /app/data
# COPY configs /app/configs

ENV PYTHONPATH=/app \
    PYTHONUNBUFFERED=1

EXPOSE 8080
CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8080"]
