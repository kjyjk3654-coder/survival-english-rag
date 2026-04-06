import json
import os
import re
import subprocess
import sys
import xml.etree.ElementTree as ET
from pathlib import Path
from urllib.parse import urlparse, parse_qs

import requests

CHANNEL_IDS = [
    {"source_channel": "Gabriel Silva", "channel_id": "UC6_f3A2_I9T-S_aG3_S4_TQ"},
    {"source_channel": "BBC Learning English", "channel_id": "UCHaHD477h-FeBbVh9Sh7syA"},
    {
        "source_channel": "Learn English with TV Series",
        "channel_id": "UCKgpamMlm872zkGDcBJHYDg",
    },
    {"source_channel": "English with Lucy", "channel_id": "UCz4tgANd4yy8Oe0iXCdSWfA"},
    {"source_channel": "mmmEnglish", "channel_id": "UC_OskgZBoS4dAnVUg05Isog"},
    {
        "source_channel": "English with Jennifer",
        "channel_id": "UCEKXieT70wByfvZwP1Cxd8w",
    },
    {"source_channel": "Rachel's English", "channel_id": "UCvn_XCl_mgQz3PrWSX85f6w"},
    {
        "source_channel": "Bob the Canadian",
        "channel_id": "UCmW5tmKIUmryNfd5YtVw72A",
    },
    {
        "source_channel": "Speak English With Vanessa",
        "channel_id": "UC8pPD3E6KjKzJ_igPh_kyDg",
    },
    {"source_channel": "EnglishClass101", "channel_id": "UC969rtU7f_S8K3K64z_t48w"},
]


def _latest_video_urls_for_channel(channel_id: str, limit: int) -> list[str]:
    feed_url = f"https://www.youtube.com/feeds/videos.xml?channel_id={channel_id}"
    res = requests.get(feed_url, timeout=25)
    if res.status_code != 200:
        raise RuntimeError(f"failed to fetch rss feed channel_id={channel_id} http={res.status_code}")

    root = ET.fromstring(res.text)
    urls: list[str] = []

    for entry in root.findall("{http://www.w3.org/2005/Atom}entry"):
        vid_el = entry.find("{http://www.youtube.com/xml/schemas/2015}videoId")
        if vid_el is None:
            continue
        vid = (vid_el.text or "").strip()
        if not vid:
            continue
        urls.append(f"https://www.youtube.com/watch?v={vid}")
        if len(urls) >= limit:
            break

    return urls


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
    provider = os.getenv("RAG_PROVIDER", "openai").strip().lower()
    model = os.getenv("RAG_MODEL", "gpt-4o-mini").strip()
    per_channel = int(os.getenv("RAG_PER_CHANNEL", "3").strip() or "3")

    out_dir = here / "out"
    out_dir.mkdir(parents=True, exist_ok=True)

    processed = 0
    for cfg in CHANNEL_IDS:
        source_channel = str(cfg.get("source_channel", "")).strip()
        channel_id = str(cfg.get("channel_id", "")).strip()
        if not source_channel or not channel_id:
            raise RuntimeError("CHANNEL_IDS must include source_channel and channel_id")

        lang = "en"
        video_urls = _latest_video_urls_for_channel(channel_id, limit=max(1, per_channel))

        for url in video_urls:
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
                    source_channel,
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

            processed += 1

    print(json.dumps({"ok": True, "processed": processed}, ensure_ascii=False))


if __name__ == "__main__":
    main()
