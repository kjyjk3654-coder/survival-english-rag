import argparse
import json
import re
from datetime import datetime, timezone
from urllib.parse import urlparse, parse_qs

from youtube_transcript_api import YouTubeTranscriptApi


def _extract_video_id(url_or_id: str) -> str:
    s = (url_or_id or "").strip()
    if not s:
        raise ValueError("empty url")

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


def _fetch_transcript(video_id: str, lang: str):
    langs = [lang] if lang else ["en", "en-US", "en-GB", "ko", "ja"]
    try:
        return YouTubeTranscriptApi.get_transcript(video_id, languages=langs)
    except Exception:
        try:
            tlist = YouTubeTranscriptApi.list_transcripts(video_id)
            if lang:
                try:
                    return tlist.find_transcript([lang]).fetch()
                except Exception:
                    pass
            try:
                return tlist.find_manually_created_transcript(["en", "ko"]).fetch()
            except Exception:
                return tlist.find_generated_transcript(["en", "ko"]).fetch()
        except Exception as e:
            raise RuntimeError(f"transcript not available: {e}")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--url", required=True, help="YouTube URL or 11-char video id")
    p.add_argument("--lang", default="en", help="preferred language code (default: en)")
    p.add_argument("--out", default="", help="output json path (default: stdout)")
    args = p.parse_args()

    video_id = _extract_video_id(args.url)
    items = _fetch_transcript(video_id, args.lang)

    now = datetime.now(timezone.utc).isoformat()
    out = {
        "video_id": video_id,
        "source_url": args.url,
        "lang": args.lang,
        "fetched_at": now,
        "segments": items,
        "text": " ".join([(i.get("text") or "").replace("\n", " ").strip() for i in items]).strip(),
    }

    payload = json.dumps(out, ensure_ascii=False, indent=2)
    if args.out:
        with open(args.out, "w", encoding="utf-8") as f:
            f.write(payload)
    else:
        print(payload)


if __name__ == "__main__":
    main()
