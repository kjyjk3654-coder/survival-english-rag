import argparse
import json
import os
from typing import Any, Dict, List

from dotenv import load_dotenv


def _load_items(path: str) -> List[Dict[str, Any]]:
    with open(path, "r", encoding="utf-8") as f:
        payload = json.load(f)
    items = payload.get("items")
    if not isinstance(items, list) or not items:
        raise RuntimeError("items missing")

    out: List[Dict[str, Any]] = []
    for it in items:
        if not isinstance(it, dict):
            continue
        expr = str(it.get("expression", "")).strip()
        meaning_ko = str(it.get("meaning_ko", "")).strip()
        context = str(it.get("context", "")).strip()
        source_channel = str(it.get("source_channel", "")).strip()
        source_url = str(it.get("source_url", "")).strip()

        if not expr or not meaning_ko or not context or not source_channel or not source_url:
            raise RuntimeError("empty required field in items")

        out.append(
            {
                "expression": expr,
                "meaning_ko": meaning_ko,
                "context": context,
                "source_channel": source_channel,
                "source_url": source_url,
            }
        )

    if not out:
        raise RuntimeError("no valid items")
    return out


def _embed_texts(texts: List[str], model: str) -> List[List[float]]:
    from openai import OpenAI

    api_key = os.getenv("OPENAI_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY is missing")

    client = OpenAI(api_key=api_key)

    res = client.embeddings.create(model=model, input=texts)
    if not getattr(res, "data", None):
        raise RuntimeError("embedding response empty")

    vectors: List[List[float]] = []
    for row in res.data:
        vec = getattr(row, "embedding", None)
        if not isinstance(vec, list) or not vec:
            raise RuntimeError("invalid embedding vector")
        vectors.append([float(x) for x in vec])

    if len(vectors) != len(texts):
        raise RuntimeError("embedding size mismatch")

    return vectors


def _chunk(lst, n: int):
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


def main():
    load_dotenv()

    p = argparse.ArgumentParser()
    p.add_argument("--in", dest="inp", required=True, help="items json created by clean_transcript_llm.py")
    p.add_argument("--table", default="phrase_items")
    p.add_argument("--embedding-model", default="text-embedding-3-small")
    p.add_argument("--batch", type=int, default=64)
    args = p.parse_args()

    supabase_url = os.getenv("SUPABASE_URL", "").strip()
    supabase_key = os.getenv("SUPABASE_SERVICE_ROLE_KEY", "").strip()
    if not supabase_url:
        raise RuntimeError("SUPABASE_URL is missing")
    if not supabase_key:
        raise RuntimeError("SUPABASE_SERVICE_ROLE_KEY is missing")

    from supabase import create_client

    sb = create_client(supabase_url, supabase_key)

    items = _load_items(args.inp)

    rows_to_upsert: List[Dict[str, Any]] = []
    for batch_items in _chunk(items, max(1, args.batch)):
        texts = [
            f"{it['expression']}\n{it['meaning_ko']}\n{it['context']}"
            for it in batch_items
        ]
        vectors = _embed_texts(texts, model=args.embedding_model)

        for it, vec in zip(batch_items, vectors):
            rows_to_upsert.append(
                {
                    "expression": it["expression"],
                    "meaning_ko": it["meaning_ko"],
                    "context": it["context"],
                    "source_channel": it["source_channel"],
                    "source_url": it["source_url"],
                    "embedding": vec,
                }
            )

    if not rows_to_upsert:
        raise RuntimeError("nothing to upsert")

    resp = (
        sb.table(args.table)
        .upsert(rows_to_upsert, on_conflict="source_url,expression")
        .execute()
    )

    data = getattr(resp, "data", None)
    count = len(data) if isinstance(data, list) else None
    print(json.dumps({"ok": True, "upserted": count}, ensure_ascii=False))


if __name__ == "__main__":
    main()
