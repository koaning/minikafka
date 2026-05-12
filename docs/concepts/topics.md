# Topics

A **topic** is a named, append-only log inside a `Source`. It is anchored
to a Pydantic model — the schema is hashed and persisted, so reopening
the database with a different model raises `SchemaMismatchError`.

## Lifecycle

Records have two states: `new` and `handled`. New rows accumulate until
something acks them — either `Topic.set_handled` or a pipeline consuming
the topic. `iter_new` and `iter_handled` stream each set in insertion
order.

## Validation

Every payload passed to `Topic.append` is validated against the topic's
model via `model_validate`. The stored row keeps the canonical JSON form
and a SHA-256 `payload_hash`.

## Deduplication

Pass `dedup=(...)` to `Source.topic` to enforce uniqueness on a tuple of
fields. The database stores a `dedup_hash` derived from those fields and a
unique partial index on `(topic, dedup_hash)` makes duplicates fail at
insert with `DuplicateMessageError`.

```python
videos = source.topic("videos", Video, dedup=("url",))
videos.append({"url": "https://x/1", ...})
videos.append({"url": "https://x/1", ...})  # -> DuplicateMessageError
```

The dedup tuple is part of the topic identity: reopening a topic with a
different dedup tuple raises `SchemaMismatchError`.

## Schema migrations

To rewrite every stored payload under a new model, use `Topic.migrate`:

```python
class CleanedV2(BaseModel):
    creator: str
    url: str
    duration_min: int
    quality: str = "unknown"


def upgrade(old: CleanedV1) -> CleanedV2:
    return CleanedV2(**old.model_dump(), quality="unknown")


cleaned_v2 = cleaned_v1.migrate(CleanedV2, upgrade)
```

The migration is atomic: the topic's schema row and every message row are
rewritten in a single SQLite transaction. The dedup tuple is preserved
and must still be present on the new model.

## Export to Polars

For topics with flat (non-nested) payloads:

```python
df = videos.to_polars()
```

Returns a `polars.DataFrame` with one row per record. Requires the
`polars` extra (`pip install minikafka[polars]`).
