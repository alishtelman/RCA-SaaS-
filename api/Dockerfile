# syntax=docker/dockerfile:1
FROM python:3.11-slim

WORKDIR /app

# нужно для llama-cpp-python, если будет сборка из исходников
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential cmake git \
    && rm -rf /var/lib/apt/lists/*

# 1) Ставим Python-зависимости
COPY api/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# 2) Копируем код по папкам
COPY api /app/api
COPY retriever /app/retriever
COPY llm /app/llm
COPY utils /app/utils

ENV PYTHONPATH=/app \
    PYTHONUNBUFFERED=1

EXPOSE 8080
CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8080"]
