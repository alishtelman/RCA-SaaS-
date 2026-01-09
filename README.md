# RGA-SD

Сервис для поиска похожих инцидентов и генерации ответов по модели RAG. Проект включает
инфраструктуру для загрузки текстов, построения эмбеддингов, гибридного поиска и FastAPI
приложение с простым UI/feedback контуром.

## Архитектура и основные папки
- **api/** — FastAPI-приложение (эндпоинты `/ask`, `/manage/reindex`, `/feedback`).
- **retriever/** — гибридный поиск и доиндексация новых тикетов.
- **indexer/** — скрипты для построения эмбеддингов и пересоздания таблиц под разные модели.
- **llm/** — генерация ответов и переформулировка запросов на базе `llama_cpp`.
- **ui/** — Streamlit UI для ручного общения с API.
- **data/** — артефакты и входные данные (например, `data/anonymized/*.json`, `data/new_tickets`).
- **tests/** — вспомогательные smoke/утилиты для проверки поиска и анонимизации.

## Быстрый старт через Docker Compose
1. Подготовьте файл `.env`:
   - `DB_URL` — строка подключения к Postgres с `pgvector` (например, `postgresql://USER:PASSWORD@postgres:5432/DB`).
   - `EMBEDDING_MODEL` — модель sentence-transformers (по умолчанию `intfloat/multilingual-e5-small`).
   - `LLM_MODEL_PATH` и `LLM_REPHRASE_MODEL_PATH` — пути к GGUF моделям внутри контейнера API.
   - Дополнительно: `API_PORT`, `TOP_K`, `RETR_TABLE`, `APP_TITLE` и др.
2. Скопируйте модели в `./models` (она монтируется в контейнер API read-only).
3. Поместите входные JSON-файлы для индексации в `data/anonymized/`.
4. Запустите стек:
   ```bash
   docker compose up --build
   ```
   - Postgres поднимается с расширением `pgvector`.
   - API доступен на `http://localhost:8080` (простая HTML-форма и эндпоинты).
   - UI (Streamlit) доступен на `http://localhost:8501` и ходит в API.
5. Проверка готовности: `curl http://localhost:8080/readyz` → `{ "ready": true }`.

## Индексация данных
- Базовая загрузка из `data/anonymized/*.json` в таблицу `documents`:
  ```bash
  python indexer/embeddings.py
  ```
- Доиндексация новых тикетов из CSV (`issue_key,text`) из каталога `data/new_tickets`:
  - вручную: `python -c "from retriever.ingest_new import ingest_new_tickets; print(ingest_new_tickets())"`
  - через API: `curl -X POST http://localhost:8080/manage/reindex`
- Перестроение таблицы под другую embedding-модель: `python indexer/reindex_for_model.py --model <model_name>`
  (создаёт отдельную таблицу `documents_<model>_<dim>`).

## Работа с API
- `POST /ask` — основной RAG-запрос. Параметры формы: `issue_text`, `context_count` (int), `service` (опц.).
  ```bash
  curl -X POST http://localhost:8080/ask \
    -F "issue_text=Падает обработка платежей" \
    -F "context_count=10"
  ```
- `POST /feedback` — запись оценки ответа (`query`, `answer_full_text`, `is_helpful`, `comment`, `used_issue_keys`).
- `GET /feedback/stats` — простая метрика доли полезных ответов.
- `POST /manage/reindex` — ручной запуск доиндексации CSV.
- `GET /healthz` / `GET /readyz` — проверки состояния контейнера.

## Запуск локально без Docker
```bash
python -m venv .venv && source .venv/bin/activate
pip install -r api/requirements.txt
uvicorn api.main:app --host 0.0.0.0 --port 8080 --reload
```
Перед запуском убедитесь, что переменные окружения `DB_URL`, `EMBEDDING_MODEL`, пути к GGUF моделям
и каталог `data/new_tickets` доступны локально.

## Тесты и утилиты
- Минимальные sanity-скрипты лежат в `tests/` (например, `tests/retrieval_smoke.py`).
- Для работы им нужен доступ к Postgres с заполненной таблицей и скачанным emb-моделям.
- Запуск примера:
  ```bash
  python tests/retrieval_smoke.py --db "$DB_URL" --model "$EMBEDDING_MODEL" --table documents --q "пример запроса"
  ```

## Полезные заметки по качеству
- Импорт `postprocess` и `rephrase_issue` опционален: если модуль отсутствует, логика продолжит работу без него.
- `models.py` был пустым и удалён как неиспользуемый артефакт.
- Избегайте try/except вокруг импортов; проверка наличия модулей теперь делается через `importlib.util.find_spec`.
