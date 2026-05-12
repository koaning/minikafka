# Quickstart

This page walks through the full lifecycle of a minikafka topic — create,
append, iterate, ack — and then composes two topics with a pipeline.

## 1. Define a model

Topics are typed by a Pydantic model. The model is also stored as a JSON
schema inside the database so reopens can fail fast on mismatch.

```python
from pydantic import BaseModel

class Video(BaseModel):
    creator: str
    url: str
    length_seconds: int
```

## 2. Open a source and create a topic

```python
from minikafka import Source

source = Source(":memory:")  # or a path to a .sqlite file
videos = source.topic("videos", Video, dedup=("url",))
```

The `dedup=("url",)` tuple makes the database reject duplicate URLs at insert
time. Pass `dedup=None` to disable.

## 3. Append records

You can pass either a model instance or a plain dict.

```python
videos.append({"creator": "example", "url": "https://x/1", "length_seconds": 90})
videos.append(Video(creator="example", url="https://x/2", length_seconds=120))
```

A second insert with the same `url` raises `DuplicateMessageError`.

## 4. Consume records

There are two ways to drain a topic.

**Manual loop** — read with `iter_new`, do your work, ack with
`set_handled`. Useful for ad-hoc scripts, quick experiments, and cases
where the side effect is non-Pythonic (writing to an API, dispatching a
job, etc.):

```python
for record in videos.iter_new(records=True):
    print(record.id, record.created_at, record.data.creator)
    videos.set_handled(record=record)
```

`set_handled` flips the row from `new` to `handled` and refreshes
`handled_at`. The transition is idempotent.

**Pipeline** — declare a transformation from one topic into another with
`topic.pipe(fn).to(target)`. This is the preferred shape for anything
larger than a one-off script: each row is consumed inside a single
SQLite transaction (target insert + source ack), so a crash mid-run
leaves no half-processed state, and the DAG composes cleanly with
`Source.full_pipeline(...)` for multi-topic flows.

```python
class Cleaned(BaseModel):
    creator: str
    url: str
    duration_min: int


cleaned = source.topic("cleaned", Cleaned, dedup=("url",))


def clean(v: Video) -> Cleaned:
    return Cleaned(
        creator=v.creator.strip().lower(),
        url=v.url,
        duration_min=v.length_seconds // 60,
    )


videos.pipe(clean).to(cleaned).run()

for item in cleaned.iter_new():
    print(item)
```

## 5. Observe events

Pass `on_event` to a `Source` to watch every state transition:

```python
def log(event, **kwargs):
    print(event, kwargs)


source = Source(":memory:", on_event=log)
```

Events emitted: `topic_created`, `message_appended`, `message_handled`,
`pipeline_start`, `pipeline_end`.

With one exception, every event fires **after** the underlying SQLite
operation has committed — so if you observe a `message_appended`, the
row is guaranteed to be in the database, and if a write fails the
observer never sees it. The exception is `pipeline_start`, which fires
**before** the run begins iterating rows (its `pipeline_end` counterpart
is emitted after the run finishes).

Exceptions raised inside `on_event` are swallowed so observability
cannot break the pipeline.

## Where to next

- [Topics](concepts/topics.md) — schema migrations, Polars export, dedup.
- [Pipelines](concepts/pipelines.md) — `dry_run`, return values, Mermaid plots.
- [Fan-out & fan-in](concepts/fan-out.md) — multi-topic DAGs with `FullPipeline`.
