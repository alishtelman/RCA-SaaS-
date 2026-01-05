# retriever/ingest_new.py
from __future__ import annotations

import csv
import glob
import os
from typing import Dict, List

import psycopg

from retriever.hybrid_search import DB_URL, TABLE, ensure_schema, get_model


# Путь внутри контейнера (см. volume в docker-compose)
NEW_TICKETS_DIR = os.getenv("NEW_TICKETS_DIR", "/data/new_tickets")


def _load_csv_rows() -> List[Dict[str, str]]:
    """
    Читает все *.csv из NEW_TICKETS_DIR.
    Ожидает столбцы: issue_key, text
    """
    rows: List[Dict[str, str]] = []

    if not os.path.isdir(NEW_TICKETS_DIR):
        return rows

    pattern = os.path.join(NEW_TICKETS_DIR, "*.csv")
    for path in glob.glob(pattern):
        with open(path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for r in reader:
                issue_key = (r.get("issue_key") or "").strip()
                text = (r.get("text") or "").strip()
                if not issue_key or not text:
                    continue
                rows.append({"issue_key": issue_key, "text": text})

    return rows


def ingest_new_tickets() -> int:
    """
    1) Читает новые тикеты из CSV.
    2) Считает эмбеддинги тем же моделем, что и hybrid_search.
    3) Вставляет только те issue_key, которых ещё нет в таблице.
    Возвращает количество добавленных записей.
    """
    rows = _load_csv_rows()
    if not rows:
        return 0

    model = get_model()
    texts = [r["text"] for r in rows]
    embeddings = model.encode(texts, normalize_embeddings=True).tolist()

    with psycopg.connect(DB_URL) as conn, conn.cursor() as cur:
        ensure_schema(conn)

        # узнаём, какие issue_key уже есть
        cur.execute(f"SELECT DISTINCT issue_key FROM {TABLE}")
        existing_keys = {row[0] for row in cur.fetchall()}

        to_insert = []
        for r, emb in zip(rows, embeddings):
            if r["issue_key"] in existing_keys:
                continue
            to_insert.append((r["issue_key"], r["text"], emb))

        if not to_insert:
            return 0

        # вставляем новые
        sql = f"""
        INSERT INTO {TABLE} (issue_key, text_chunk, embedding)
        VALUES (%s, %s, %s)
        """
        for issue_key, text, emb in to_insert:
            cur.execute(sql, (issue_key, text, emb))

        conn.commit()
        return len(to_insert)
