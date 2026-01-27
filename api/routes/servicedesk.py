from __future__ import annotations

import csv
import os
from datetime import datetime
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, Header, HTTPException
from pydantic import BaseModel

from api.schemas.servicedesk import TicketsBatchIn

# ВАЖНО: этот импорт у вас уже должен работать, т.к. в README вы используете именно его
# python -c "from retriever.ingest_new import ingest_new_tickets; ..."
from retriever.ingest_new import ingest_new_tickets

router = APIRouter(prefix="/sd", tags=["service-desk"])


class TicketsBatchOut(BaseModel):
    accepted: int
    stored_file: str
    reindex_started: bool
    reindex_result: Optional[dict] = None


def _get_env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except Exception:
        return default


@router.post("/tickets", response_model=TicketsBatchOut)
def ingest_tickets_from_sd(
    payload: TicketsBatchIn,
    x_api_key: Optional[str] = Header(default=None, alias="X-API-Key"),
):
    # 1) Auth
    expected_key = os.getenv("SERVICE_DESK_API_KEY", "").strip()
    if not expected_key:
        # если ключ не задан — это конфигурационная ошибка (лучше падать явно)
        raise HTTPException(status_code=500, detail="SERVICE_DESK_API_KEY is not configured")

    if not x_api_key or x_api_key.strip() != expected_key:
        raise HTTPException(status_code=401, detail="Unauthorized")

    # 2) Validate limits
    max_batch = _get_env_int("SD_MAX_BATCH", 500)
    max_text_len = _get_env_int("SD_MAX_TEXT_LEN", 10000)

    if not payload.tickets:
        raise HTTPException(status_code=400, detail="tickets is empty")

    if len(payload.tickets) > max_batch:
        raise HTTPException(status_code=413, detail=f"too many tickets (max {max_batch})")

    # 3) Prepare dir
    new_tickets_dir = Path(os.getenv("NEW_TICKETS_DIR", "data/new_tickets"))
    new_tickets_dir.mkdir(parents=True, exist_ok=True)

    # 4) Write CSV (issue_key,text)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = new_tickets_dir / f"sd_{ts}.csv"

    accepted = 0
    with out_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["issue_key", "text"])
        for t in payload.tickets:
            text = (t.text or "").strip()
            if not text:
                continue
            if len(text) > max_text_len:
                text = text[:max_text_len]
            w.writerow([t.issue_key.strip(), text])
            accepted += 1

    if accepted == 0:
        # если все тексты пустые — удаляем файл, чтобы не копить мусор
        try:
            out_path.unlink(missing_ok=True)
        except Exception:
            pass
        raise HTTPException(status_code=400, detail="no valid tickets to ingest")

    # 5) Trigger reindex (same as /manage/reindex does conceptually)
    reindex_started = False
    reindex_result = None
    try:
        reindex_started = True
        # ingest_new_tickets() в вашем проекте возвращает результат (обычно dict/str)
        res = ingest_new_tickets()
        # приведем к dict если возможно
        if isinstance(res, dict):
            reindex_result = res
        else:
            reindex_result = {"result": str(res)}
    except Exception as e:
        # ВАЖНО: файл уже сохранен — SD не должен повторять батч бесконечно
        # Поэтому возвращаем 200, но с флагом/деталями (или можно 500 — как договоритесь)
        reindex_started = False
        reindex_result = {"error": str(e)}

    return TicketsBatchOut(
        accepted=accepted,
        stored_file=str(out_path),
        reindex_started=reindex_started,
        reindex_result=reindex_result,
    )
