# llm/rephrase.py
import os
import requests

# Используем те же переменные окружения, что и для основного генератора
OLLAMA_URL = os.getenv("LLM_REMOTE_URL", "http://host.docker.internal:11434")
REPHRASE_MODEL = os.getenv("LLM_REPHRASE_MODEL", os.getenv("LLM_MODEL", "qwen2.5:7b-instruct-q4_K_M"))


def build_rephrase_prompt(text: str) -> str:
    """
    Промпт для улучшения описания инцидента перед семантическим поиском.
    """
    return f"""Ты — инженер технической поддержки, который готовит описание инцидентов
для последующего поиска похожих случаев в базе.

Переформулируй описание инцидента так, чтобы:
- оно было понятным и технически точным;
- сохранились ключевые симптомы и контекст (какой сервис, кто страдает, что именно не работает);
- убраны лишние детали (лишние формулировки, приветствия, общая переписка);
- длина была 1–3 абзаца.

Если исходный текст очень короткий и непонятный, аккуратно ДОПОЛНИ контекст:
- опиши, что именно может ломаться,
- какие компоненты могут быть задействованы,
- какие типовые причины бывают для таких случаев.

Верни только ОДНО переформулированное описание без пояснений, списков и маркировок.

Исходное описание инцидента:
{text}
"""


def rephrase_issue(text: str, max_len: int = 600) -> str:
    """
    Переформулирует/обогащает описание инцидента через LLM.
    Если что-то пошло не так — возвращает исходный текст.
    """
    # очень короткие тексты можно не трогать
    if not text or len(text.strip()) < 30:
        return text.strip()

    prompt = build_rephrase_prompt(text)

    try:
        resp = requests.post(
            f"{OLLAMA_URL}/api/generate",
            json={
                "model": REPHRASE_MODEL,
                "prompt": prompt,
                "stream": False,
            },
            timeout=40,  # короче, чем для основного ответа
        )
        resp.raise_for_status()
        data = resp.json()
        result = (data.get("response") or "").strip()
        if not result:
            return text.strip()

        # подрежем слишком длинный ответ, чтобы не раздувать эмбеддинг
        if len(result) > max_len:
            result = result[:max_len]

        return result

    except Exception:
        # на любых проблемах с LLM просто возвращаем оригинальный текст
        return text.strip()
