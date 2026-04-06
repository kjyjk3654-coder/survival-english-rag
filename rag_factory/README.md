# RAG Factory (YouTube)

## Setup
```bash
python -m venv .venv
.\.venv\Scripts\activate
pip install -r rag_factory\requirements.txt
```

## 1) Extract transcript
```bash
python rag_factory\youtube_transcript_extract.py --url "https://www.youtube.com/watch?v=VIDEO_ID" --lang en --out transcript.json
```

## 2) Clean to JSON items (OpenAI)
Set env:
- `OPENAI_API_KEY`

```bash
python rag_factory\clean_transcript_llm.py --in transcript.json --video-url "https://www.youtube.com/watch?v=VIDEO_ID" --source-channel "CHANNEL_NAME" --provider openai --model gpt-4o-mini --out items.json
```

## 2) Clean to JSON items (Gemini)
Set env:
- `GEMINI_API_KEY`

```bash
python rag_factory\clean_transcript_llm.py --in transcript.json --video-url "https://www.youtube.com/watch?v=VIDEO_ID" --source-channel "CHANNEL_NAME" --provider gemini --model gemini-1.5-flash --out items.json
```

## 3) Embed + upsert to Supabase (pgvector)
Set env:
- `OPENAI_API_KEY`
- `SUPABASE_URL`
- `SUPABASE_SERVICE_ROLE_KEY`

```bash
python rag_factory\embed_and_upsert_supabase.py --in items.json
```
