import argparse
import json
import os
import re
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv


PROMPT_TEMPLATE = """You are building a Korean-first survival English phrase dataset.

Input is a raw transcript excerpt from a YouTube video.
Your task: extract ONLY practical, spoken, native-like English expressions that are reusable in real life.

Return ONLY strict JSON. No code fences. No explanation.

Output schema:
{
  \"items\": [
    {
      \"expression\": \"...\",
      \"meaning_ko\": \"...\",
      \"context\": \"...\",
      \"source_channel\": \"...\",
      \"source_url\": \"...\"
    }
  ]
}

Hard rules:
- All fields MUST be present.
- No empty strings anywhere.
- expression must be natural spoken English (not grammar explanations).
- meaning_ko must be a natural Korean translation.
- context must describe when to use it (Korean, 1 sentence).
- source_channel must be exactly: {source_channel}
- source_url must be a direct YouTube link for the same video. If possible, include a timestamp.
  You MUST use this base video URL as the same video: {video_url}
- items length: 5 to 20.

Transcript excerpt:
---
{transcript}
---
"""


def _strip_code_fences(s: str) -> str:
    t = (s or "").strip()
    if t.startswith("```"):
        t = re.sub(r"^```[a-zA-Z0-9_-]*\n?", "", t)
        t = re.sub(r"```\s*$", "", t)
    return t.strip()


def _extract_first_json_object(s: str) -> Optional[str]:
    t = s
    start = t.find("{")
    if start < 0:
        return None
    depth = 0
    for i in range(start, len(t)):
        if t[i] == "{":
            depth += 1
        elif t[i] == "}":
            depth -= 1
            if depth == 0:
                return t[start : i + 1]
    return None


def _parse_json_strict(text: str) -> Dict[str, Any]:
    raw = _strip_code_fences(text)
    try:
        return json.loads(raw)
    except Exception:
        extracted = _extract_first_json_object(raw)
        if not extracted:
            raise
        return json.loads(extracted)


def _validate_items(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    items = payload.get("items")
    if not isinstance(items, list) or not items:
        raise ValueError("items missing")

    out: List[Dict[str, Any]] = []
    for it in items:
        if not isinstance(it, dict):
            continue
        exp = str(it.get("expression", "")).strip()
        mean = str(it.get("meaning_ko", "")).strip()
        ctx = str(it.get("context", "")).strip()
        sch = str(it.get("source_channel", "")).strip()
        surl = str(it.get("source_url", "")).strip()
        if not exp or not mean or not ctx or not sch or not surl:
            raise ValueError("empty core field")

        out.append(
            {
                "expression": exp,
                "meaning_ko": mean,
                "context": ctx,
                "source_channel": sch,
                "source_url": surl,
            }
        )

    if not out:
        raise ValueError("no valid items")
    return out


def _call_openai(prompt: str, model: str) -> str:
    from openai import OpenAI

    api_key = os.getenv("OPENAI_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY is missing")

    client = OpenAI(api_key=api_key)
    res = client.chat.completions.create(
        model=model,
        messages=[{"role": "user", "content": prompt}],
        temperature=0.2,
        response_format={"type": "json_object"},
    )
    return (res.choices[0].message.content or "").strip()


def _call_gemini(prompt: str, model: str) -> str:
    import google.generativeai as genai

    api_key = os.getenv("GEMINI_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("GEMINI_API_KEY is missing")

    genai.configure(api_key=api_key)
    m = genai.GenerativeModel(model)
    res = m.generate_content(
        prompt,
        generation_config={
            "temperature": 0.2,
            "response_mime_type": "application/json",
        },
    )
    return (getattr(res, "text", "") or "").strip()


def main():
    load_dotenv()

    p = argparse.ArgumentParser()
    p.add_argument("--in", dest="inp", required=True, help="input transcript json from extractor")
    p.add_argument("--video-url", required=True)
    p.add_argument("--source-channel", required=True)
    p.add_argument("--provider", choices=["openai", "gemini"], required=True)
    p.add_argument("--model", required=True)
    p.add_argument("--out", default="", help="output json path (default: stdout)")
    args = p.parse_args()

    with open(args.inp, "r", encoding="utf-8") as f:
        tjson = json.load(f)

    transcript_text = str(tjson.get("text", "")).strip()
    if not transcript_text:
        raise RuntimeError("empty transcript text")

    prompt = PROMPT_TEMPLATE.format(
        transcript=transcript_text,
        source_channel=args.source_channel.strip(),
        video_url=args.video_url.strip(),
    )

    if args.provider == "openai":
        text = _call_openai(prompt, model=args.model)
    else:
        text = _call_gemini(prompt, model=args.model)

    payload = _parse_json_strict(text)
    items = _validate_items(payload)

    out = json.dumps({"items": items}, ensure_ascii=False, indent=2)
    if args.out:
        with open(args.out, "w", encoding="utf-8") as f:
            f.write(out)
    else:
        print(out)


if __name__ == "__main__":
    main()
