# Vector DB schema (RAG Factory)

## Supabase (Postgres + pgvector) - FINAL

### 1) Extensions
```sql
create extension if not exists vector;
create extension if not exists pgcrypto;
```

### 2) Table
```sql
create table if not exists public.phrase_items (
  id uuid primary key default gen_random_uuid(),
  expression text not null,
  meaning_ko text not null,
  context text not null,
  source_channel text not null,
  source_url text not null,

  embedding vector(1536),

  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);
```

### 3) Index (vector)
- Use cosine distance
```sql
create index if not exists phrase_items_embedding_idx
on public.phrase_items
using ivfflat (embedding vector_cosine_ops)
with (lists = 100);
```

### 4) Upsert key (dedup suggestion)
```sql
create unique index if not exists phrase_items_dedup
on public.phrase_items (source_url, expression);
```

### 5) Query example
```sql
select id, expression, meaning_ko, context, source_channel, source_url,
  1 - (embedding <=> $1) as similarity
from public.phrase_items
order by embedding <=> $1
limit 10;
```
