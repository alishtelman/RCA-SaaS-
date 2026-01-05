# api/main.py
from __future__ import annotations

import importlib.util
import os
from fastapi import FastAPI, Form
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse

from retriever.hybrid_search import search
from llm.generator import generate
from api.utils.formatter import to_structured
from api.manage import router as manage_router
from api.feedback import router as feedback_router


def _load_postprocess_func():
    """Load optional formatter postprocessor if the module is present."""
    spec = importlib.util.find_spec("api.utils.postprocess")
    if not spec or not spec.loader:
        return None

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return getattr(module, "postprocess", None)


postprocess = _load_postprocess_func()

APP_TITLE = os.getenv("APP_TITLE", "RAG Agent API")
app = FastAPI(title=APP_TITLE)

# CORS (на время разработки)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

FORM_HTML = """<form method='post' action='/ask' style="font-family:ui-sans-serif">
  <label>Текст запроса:</label><br>
  <textarea name='issue_text' rows=6 cols=80 placeholder='Опишите проблему…'></textarea><br><br>
  <label>Сколько контекстов (top_k):</label>
  <input type='number' name='context_count' value='20' min='1' max='50' />
  <button type='submit'>Ask</button>
</form>"""


@app.get("/", response_class=HTMLResponse)
def form_root():
    return FORM_HTML


# важно: браузер часто открывает /ask как GET — отдадим форму, а не 500
@app.get("/ask", response_class=HTMLResponse)
def form_alias():
    return FORM_HTML


@app.post("/ask")
def ask(
    issue_text: str = Form(...),
    context_count: int = Form(20),
    service: str | None = Form(None),
):
    """
    1) Ищем контекст (RAG)
    2) Генерим ответ (LLM)
    3) Превращаем «сырой» ответ в структуру (formatter)
    4) (опц.) postprocess
    5) Возвращаем предсказуемый JSON
    """
    if not issue_text or not issue_text.strip():
        return JSONResponse(
            status_code=400,
            content={"error": "empty_issue_text", "message": "issue_text не должен быть пустым"},
        )

    # 1) Поиск контекста
    try:
        # ожидаем формат от search: (issue_key, snippet, score)
        top_k = max(1, min(50, int(context_count)))
        results = search(issue_text, top_k=top_k, service=service) or []

        used_issue_keys = [r[0] for r in results]
        used_snippets = [str(r[1])[:300] for r in results]
        context_chunks = [str(r[1]) for r in results]
        context = "\n\n---\n\n".join(context_chunks)

    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"error": "retrieval_failed", "message": f"Ошибка поиска контекста: {e}"},
        )

    # 2) Генерация LLM
    try:
        raw_answer = generate(context=context, question=issue_text)
    except Exception as e:
        return JSONResponse(
            status_code=502,
            content={"error": "llm_failed", "message": f"Ошибка генерации ответа LLM: {e}"},
        )

    # 3) Форматирование → 4 секции
    try:
        structured = to_structured(raw_answer)
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={
                "error": "format_failed",
                "message": f"Ошибка форматирования: {e}",
                "raw_answer": raw_answer[:1000],
            },
        )

    # 4) (опционально) пост-обработка
    if callable(postprocess):
        try:
            structured = postprocess(structured, query=issue_text, context=context)  # type: ignore
        except Exception:
            # не ломаем ответ, если постпроцессор дал сбой
            pass

    # 5) Ответ
    return JSONResponse(
        {
            "query": issue_text,
            "context_count": len(results),
            "description": structured.get("description", []),
            "causes": structured.get("causes", []),
            "actions": structured.get("actions", []),
            "next_steps": structured.get("next_steps", []),
            "full_text": structured.get("full_text", raw_answer),
            "used_chunks": used_snippets,
            "used_issue_keys": used_issue_keys,
        }
    )


@app.get("/healthz")
def healthz():
    return {"ok": True}


@app.get("/readyz")
def readyz():
    return {"ready": True}


app.include_router(manage_router)
app.include_router(feedback_router)
