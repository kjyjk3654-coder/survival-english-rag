import json
import os
import re
import subprocess
import sys
from pathlib import Path
from urllib.parse import urlparse, parse_qs


def _extract_video_id(url_or_id: str) -> str:
    s = (url_or_id or "").strip()
    if re.fullmatch(r"[A-Za-z0-9_-]{11}", s):
        return s

    u = urlparse(s)
    if u.netloc in ("youtu.be", "www.youtu.be"):
        vid = u.path.strip("/")
        if re.fullmatch(r"[A-Za-z0-9_-]{11}", vid):
            return vid

    if u.netloc in ("youtube.com", "www.youtube.com", "m.youtube.com"):
        if u.path == "/watch":
            q = parse_qs(u.query)
            vid = (q.get("v") or [""])[0]
            if re.fullmatch(r"[A-Za-z0-9_-]{11}", vid):
                return vid
        if u.path.startswith("/shorts/"):
            vid = u.path.split("/shorts/")[-1].split("/")[0]
            if re.fullmatch(r"[A-Za-z0-9_-]{11}", vid):
                return vid

    raise ValueError(f"could not extract video id from: {url_or_id}")


def _run(cmd):
    p = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    if p.returncode != 0:
        raise RuntimeError(f"command failed rc={p.returncode}\n{p.stdout}")
    return p.stdout


def main():
    here = Path(__file__).resolve().parent
    sources_path = here / "sources.json"
    if not sources_path.exists():
        raise RuntimeError("sources.json missing")

    with open(sources_path, "r", encoding="utf-8") as f:
        sources = json.load(f).get("sources")

    if not isinstance(sources, list) or not sources:
        raise RuntimeError("sources empty")

    provider = os.getenv("RAG_PROVIDER", "openai").strip().lower()
    model = os.getenv("RAG_MODEL", "gpt-4o-mini").strip()

    out_dir = here / "out"
    out_dir.mkdir(parents=True, exist_ok=True)

    for src in sources:
        ch = str(src.get("source_channel", "")).strip()
        url = str(src.get("video_url", "")).strip()
        lang = str(src.get("lang", "en")).strip() or "en"
        if not ch or not url:
            raise RuntimeError("source_channel/video_url required")

        vid = _extract_video_id(url)
        transcript_path = out_dir / f"transcript_{vid}.json"
        items_path = out_dir / f"items_{vid}.json"

        _run(
            [
                sys.executable,
                str(here / "youtube_transcript_extract.py"),
                "--url",
                url,
                "--lang",
                lang,
                "--out",
                str(transcript_path),
            ]
        )

        _run(
            [
                sys.executable,
                str(here / "clean_transcript_llm.py"),
                "--in",
                str(transcript_path),
                "--video-url",
                url,
                "--source-channel",
                ch,
                "--provider",
                provider,
                "--model",
                model,
                "--out",
                str(items_path),
            ]
        )

        _run(
            [
                sys.executable,
                str(here / "embed_and_upsert_supabase.py"),
                "--in",
                str(items_path),
            ]
        )

    print(json.dumps({"ok": True, "processed": len(sources)}, ensure_ascii=False))


if __name__ == "__main__":
    main()
