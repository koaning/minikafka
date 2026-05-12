from __future__ import annotations

import hashlib
import inspect
import json
import sqlite3
from collections import defaultdict, deque
from collections.abc import Callable, Iterator, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Generic, TypeVar

from pydantic import BaseModel

ModelT = TypeVar("ModelT", bound=BaseModel)
OutputT = TypeVar("OutputT", bound=BaseModel)


class SlimlinkError(Exception):
    """Base class for slimlink errors."""


class SchemaMismatchError(SlimlinkError):
    """Raised when a topic exists with incompatible schema or configuration."""


class DuplicateMessageError(SlimlinkError):
    """Raised when a message violates a topic's deduplication constraint."""


@dataclass(frozen=True)
class FanOutFailure:
    record_id: int
    source: str
    target: str | None
    exception: BaseException


class FanOutError(SlimlinkError):
    """Raised at the end of a best_effort FullPipeline run if any sibling raised.

    Successful sibling writes are preserved. Parent rows are marked
    `handled` only when every sibling succeeded; rows with any failure
    stay `new` so a corrected run can retry them.
    """

    def __init__(self, failures: list[FanOutFailure]):
        self.failures: list[FanOutFailure] = list(failures)
        super().__init__(self._format())

    def _format(self) -> str:
        head = f"{len(self.failures)} sibling failure(s) during best_effort run"
        lines = [head]
        for failure in self.failures:
            arrow = (
                f"{failure.source} -> {failure.target}"
                if failure.target is not None
                else failure.source
            )
            lines.append(
                f"  record={failure.record_id} {arrow}: "
                f"{type(failure.exception).__name__}: {failure.exception}"
            )
        return "\n".join(lines)


@dataclass(frozen=True)
class Record(Generic[ModelT]):
    id: int
    topic: str
    created_at: datetime
    handled_at: datetime | None
    status: str
    payload_hash: str
    dedup_hash: str | None
    data: ModelT


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _parse_dt(value: str | None) -> datetime | None:
    if value is None:
        return None
    return datetime.fromisoformat(value)


def _canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _stable_hash(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _schema_for(model: type[BaseModel]) -> dict[str, Any]:
    return model.model_json_schema()


def _schema_hash(model: type[BaseModel]) -> str:
    return _stable_hash(_schema_for(model))


def _validate_model_class(model: Any, *, argument: str = "model") -> type[BaseModel]:
    if not inspect.isclass(model) or not issubclass(model, BaseModel):
        raise TypeError(f"{argument} must be a Pydantic BaseModel class")
    return model


def _validate_dedup_fields(
    model: type[BaseModel], dedup: Sequence[str] | None
) -> tuple[str, ...]:
    if dedup is None:
        return ()
    if len(set(dedup)) != len(tuple(dedup)):
        raise ValueError("dedup fields must be unique")
    fields = set(model.model_fields)
    missing = [field for field in dedup if field not in fields]
    if missing:
        raise ValueError(f"dedup fields are not present on model: {missing}")
    return tuple(sorted(dedup))


def _model_to_payload(model: BaseModel) -> dict[str, Any]:
    return model.model_dump(mode="json")


class Source:
    def __init__(self, path: str | Path):
        self.path = str(path)
        self._conn = sqlite3.connect(self.path)
        self._conn.row_factory = sqlite3.Row
        self._models: dict[str, type[BaseModel]] = {}
        self._init_db()

    def topic(
        self,
        name: str,
        model: type[ModelT],
        *,
        dedup: Sequence[str] | None,
    ) -> Topic[ModelT]:
        model = _validate_model_class(model)  # type: ignore[assignment]
        dedup_fields = _validate_dedup_fields(model, dedup)
        schema = _schema_for(model)
        schema_hash = _schema_hash(model)
        existing = self._topic_row(name)

        if existing is None:
            now = _utc_now()
            self._conn.execute(
                """
                INSERT INTO topics (name, schema_json, schema_hash, dedup_fields, created_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    name,
                    _canonical_json(schema),
                    schema_hash,
                    _canonical_json(list(dedup_fields)),
                    now,
                ),
            )
            self._conn.commit()
        else:
            existing_dedup = tuple(json.loads(existing["dedup_fields"]))
            if existing["schema_hash"] != schema_hash or existing_dedup != dedup_fields:
                raise SchemaMismatchError(
                    f"topic exists with a different schema or dedup config: {name}"
                )

        self._models[name] = model
        return Topic(self, name, model)

    def full_pipeline(self, *pipelines: Pipeline[Any, Any]) -> FullPipeline:
        return FullPipeline(list(pipelines))

    def close(self) -> None:
        self._conn.close()

    def _init_db(self) -> None:
        self._conn.executescript(
            """
            PRAGMA foreign_keys = ON;

            CREATE TABLE IF NOT EXISTS topics (
                name TEXT PRIMARY KEY,
                schema_json TEXT NOT NULL,
                schema_hash TEXT NOT NULL,
                dedup_fields TEXT NOT NULL,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                topic TEXT NOT NULL REFERENCES topics(name),
                payload_json TEXT NOT NULL,
                schema_hash TEXT NOT NULL,
                payload_hash TEXT NOT NULL,
                dedup_hash TEXT,
                created_at TEXT NOT NULL,
                handled_at TEXT,
                status TEXT NOT NULL DEFAULT 'new',
                CHECK (status IN ('new', 'handled'))
            );

            CREATE INDEX IF NOT EXISTS idx_messages_topic_status_id
            ON messages(topic, status, id);

            CREATE UNIQUE INDEX IF NOT EXISTS idx_messages_topic_dedup_hash
            ON messages(topic, dedup_hash)
            WHERE dedup_hash IS NOT NULL;
            """
        )
        self._conn.commit()

    def _topic_row(self, name: str) -> sqlite3.Row | None:
        return self._conn.execute("SELECT * FROM topics WHERE name = ?", (name,)).fetchone()

    def _registered_topic(self, name: str) -> Topic[Any]:
        model = self._models.get(name)
        if model is None:
            raise KeyError(
                f"topic is not registered in this Source: {name}; "
                "pass a Topic object or call Source.topic(name, model) first"
            )
        return Topic(self, name, model)


class Topic(Generic[ModelT]):
    def __init__(self, source: Source, name: str, model: type[ModelT]):
        self.source = source
        self.name = name
        self.model = model

    def append(self, payload: ModelT | dict[str, Any]) -> ModelT:
        model = self._validate(payload)
        payload_dict = _model_to_payload(model)
        self._insert_payload(payload_dict)
        return model

    def iter_new(
        self, *, records: bool = False, as_dict: bool = False
    ) -> Iterator[ModelT | Record[ModelT] | dict[str, Any]]:
        yield from self._iter(status="new", records=records, as_dict=as_dict)

    def iter_handled(
        self, *, records: bool = False, as_dict: bool = False
    ) -> Iterator[ModelT | Record[ModelT] | dict[str, Any]]:
        yield from self._iter(status="handled", records=records, as_dict=as_dict)

    def set_handled(
        self, _id: int | None = None, *, record: Record[Any] | None = None
    ) -> None:
        message_id = _id if _id is not None else record.id if record is not None else None
        if message_id is None:
            raise ValueError("set_handled requires _id or record")
        self.source._conn.execute(
            """
            UPDATE messages
            SET handled_at = ?, status = 'handled'
            WHERE id = ? AND topic = ?
            """,
            (_utc_now(), message_id, self.name),
        )
        self.source._conn.commit()

    def pipe(self, fn: Callable[[ModelT], Any]) -> Pipeline[ModelT, Any]:
        return Pipeline(self, fn)

    def migrate(
        self,
        new_model: type[OutputT],
        migration_function: Callable[[ModelT], OutputT | dict[str, Any]],
    ) -> Topic[OutputT]:
        new_model = _validate_model_class(new_model, argument="new_model")  # type: ignore[assignment]
        rows = self.source._conn.execute(
            "SELECT * FROM messages WHERE topic = ? ORDER BY id", (self.name,)
        ).fetchall()
        new_payloads: list[tuple[int, dict[str, Any], str, str | None]] = []
        old_dedup_fields = tuple(json.loads(self.source._topic_row(self.name)["dedup_fields"]))
        _validate_dedup_fields(new_model, old_dedup_fields)
        new_schema = _schema_for(new_model)
        new_schema_hash = _schema_hash(new_model)

        for row in rows:
            old_payload = json.loads(row["payload_json"])
            old_instance = self.model.model_validate(old_payload)
            migrated = new_model.model_validate(migration_function(old_instance))
            payload_dict = _model_to_payload(migrated)
            payload_hash = _stable_hash(payload_dict)
            dedup_hash = self._dedup_hash_for_payload(payload_dict, old_dedup_fields)
            new_payloads.append((row["id"], payload_dict, payload_hash, dedup_hash))

        with self.source._conn:
            self.source._conn.execute(
                """
                UPDATE topics
                SET schema_json = ?, schema_hash = ?
                WHERE name = ?
                """,
                (_canonical_json(new_schema), new_schema_hash, self.name),
            )
            for message_id, payload_dict, payload_hash, dedup_hash in new_payloads:
                self.source._conn.execute(
                    """
                    UPDATE messages
                    SET payload_json = ?, schema_hash = ?, payload_hash = ?, dedup_hash = ?
                    WHERE id = ? AND topic = ?
                    """,
                    (
                        _canonical_json(payload_dict),
                        new_schema_hash,
                        payload_hash,
                        dedup_hash,
                        message_id,
                        self.name,
                    ),
                )

        self.source._models[self.name] = new_model
        return Topic(self.source, self.name, new_model)

    def to_polars(self):
        rows = list(self.iter_new(as_dict=True)) + list(self.iter_handled(as_dict=True))
        for row in rows:
            for key, value in row.items():
                if isinstance(value, (dict, list, tuple)):
                    raise ValueError(f"to_polars only supports flat payloads; {key!r} is nested")
        try:
            import polars as pl
        except ImportError as exc:
            raise ImportError(
                "to_polars() requires the optional 'polars' dependency"
            ) from exc
        return pl.DataFrame(rows)

    def _validate(self, payload: ModelT | dict[str, Any]) -> ModelT:
        return self.model.model_validate(payload)

    def _iter(
        self, *, status: str, records: bool, as_dict: bool
    ) -> Iterator[ModelT | Record[ModelT] | dict[str, Any]]:
        if records and as_dict:
            raise ValueError("records and as_dict cannot both be True")
        rows = self.source._conn.execute(
            """
            SELECT * FROM messages
            WHERE topic = ? AND status = ?
            ORDER BY id
            """,
            (self.name, status),
        )
        for row in rows:
            payload = json.loads(row["payload_json"])
            if as_dict:
                yield payload
                continue
            model = self.model.model_validate(payload)
            if records:
                yield Record(
                    id=row["id"],
                    topic=row["topic"],
                    created_at=_parse_dt(row["created_at"]),  # type: ignore[arg-type]
                    handled_at=_parse_dt(row["handled_at"]),
                    status=row["status"],
                    payload_hash=row["payload_hash"],
                    dedup_hash=row["dedup_hash"],
                    data=model,
                )
            else:
                yield model

    def _insert_payload(self, payload_dict: dict[str, Any]) -> None:
        with self.source._conn:
            self._insert_payload_in_current_transaction(payload_dict)

    def _insert_payload_in_current_transaction(self, payload_dict: dict[str, Any]) -> None:
        topic_row = self.source._topic_row(self.name)
        if topic_row is None:
            raise KeyError(f"unknown topic: {self.name}")
        schema_hash = topic_row["schema_hash"]
        dedup_fields = tuple(json.loads(topic_row["dedup_fields"]))
        payload_hash = _stable_hash(payload_dict)
        dedup_hash = self._dedup_hash_for_payload(payload_dict, dedup_fields)
        try:
            self.source._conn.execute(
                """
                INSERT INTO messages (
                    topic, payload_json, schema_hash, payload_hash,
                    dedup_hash, created_at, status
                )
                VALUES (?, ?, ?, ?, ?, ?, 'new')
                """,
                (
                    self.name,
                    _canonical_json(payload_dict),
                    schema_hash,
                    payload_hash,
                    dedup_hash,
                    _utc_now(),
                ),
            )
        except sqlite3.IntegrityError as exc:
            if dedup_hash is not None:
                raise DuplicateMessageError(
                    f"duplicate message for topic {self.name!r} and dedup fields {dedup_fields}"
                ) from exc
            raise

    def _dedup_hash_for_payload(
        self, payload_dict: dict[str, Any], dedup_fields: Sequence[str]
    ) -> str | None:
        if not dedup_fields:
            return None
        return _stable_hash({field: payload_dict[field] for field in dedup_fields})


class Pipeline(Generic[ModelT, OutputT]):
    def __init__(self, source_topic: Topic[ModelT], fn: Callable[[ModelT], OutputT]):
        self.source_topic = source_topic
        self.fn = fn
        self.target_topic: Topic[Any] | None = None

    def to(self, target: Topic[OutputT] | str) -> Pipeline[ModelT, OutputT]:
        if isinstance(target, str):
            target = self.source_topic.source._registered_topic(target)
        self.target_topic = target
        return self

    def run(self, *, dry_run: bool = False) -> list[Any]:
        rows = list(self.source_topic.iter_new(records=True))
        results: list[Any] = []
        for record in rows:
            assert isinstance(record, Record)
            result = self.fn(record.data)
            if self.target_topic is None:
                results.append(result)
                continue
            if result is None:
                results.append(None)
                if not dry_run:
                    self.source_topic.set_handled(record=record)
                continue
            target_model = self.target_topic._validate(result)
            results.append(target_model)
            if dry_run:
                continue
            target_payload = _model_to_payload(target_model)
            with self.source_topic.source._conn:
                self.target_topic._insert_payload_in_current_transaction(target_payload)
                self.source_topic.source._conn.execute(
                    """
                    UPDATE messages
                    SET handled_at = ?, status = 'handled'
                    WHERE id = ? AND topic = ?
                    """,
                    (_utc_now(), record.id, self.source_topic.name),
                )
        return results

    @property
    def source_name(self) -> str:
        return self.source_topic.name

    @property
    def target_name(self) -> str | None:
        return self.target_topic.name if self.target_topic is not None else None

    def plot(self) -> str:
        return FullPipeline([self]).plot()


class FullPipeline:
    def __init__(self, pipelines: list[Pipeline[Any, Any]]):
        self.pipelines = pipelines

    def run(
        self,
        *,
        dry_run: bool = False,
        strategy: str = "strict",
    ) -> list[list[Any]]:
        if strategy not in ("strict", "best_effort"):
            raise ValueError(
                f"strategy must be 'strict' or 'best_effort', got {strategy!r}"
            )

        results: dict[int, list[Any]] = {id(p): [] for p in self.pipelines}
        failures: list[FanOutFailure] = []

        for source_topic, siblings in self._grouped_in_order():
            for record in list(source_topic.iter_new(records=True)):
                assert isinstance(record, Record)
                if strategy == "strict":
                    self._run_row_strict(
                        source_topic, siblings, record, dry_run, results
                    )
                else:
                    self._run_row_best_effort(
                        source_topic, siblings, record, dry_run, results, failures
                    )

        if failures:
            raise FanOutError(failures)

        return [results[id(p)] for p in self.pipelines]

    def plot(self) -> str:
        lines = ["graph TD"]
        for pipeline in self.pipelines:
            if pipeline.target_name is None:
                lines.append(f"    {pipeline.source_name}")
            else:
                lines.append(f"    {pipeline.source_name} --> {pipeline.target_name}")
        return "\n".join(lines)

    def _grouped_in_order(
        self,
    ) -> list[tuple[Topic[Any], list[Pipeline[Any, Any]]]]:
        nodes: set[str] = {p.source_name for p in self.pipelines}
        outgoing: dict[str, list[str]] = defaultdict(list)
        indegree: dict[str, int] = {}
        for p in self.pipelines:
            if p.target_name is not None:
                nodes.add(p.target_name)
                outgoing[p.source_name].append(p.target_name)
        for node in nodes:
            indegree[node] = 0
        for p in self.pipelines:
            if p.target_name is not None:
                indegree[p.target_name] += 1

        queue = deque(sorted(node for node, deg in indegree.items() if deg == 0))
        ordered_sources: list[str] = []
        while queue:
            node = queue.popleft()
            ordered_sources.append(node)
            for target in outgoing[node]:
                indegree[target] -= 1
                if indegree[target] == 0:
                    queue.append(target)

        if len(ordered_sources) != len(nodes):
            raise ValueError("pipeline contains a cycle")

        by_source: dict[str, list[Pipeline[Any, Any]]] = defaultdict(list)
        for p in self.pipelines:
            by_source[p.source_name].append(p)

        groups: list[tuple[Topic[Any], list[Pipeline[Any, Any]]]] = []
        emitted: set[str] = set()
        for source_name in ordered_sources:
            if source_name in by_source and source_name not in emitted:
                pipelines = by_source[source_name]
                groups.append((pipelines[0].source_topic, pipelines))
                emitted.add(source_name)
        return groups

    def _run_row_strict(
        self,
        source: Topic[Any],
        siblings: list[Pipeline[Any, Any]],
        record: Record[Any],
        dry_run: bool,
        results: dict[int, list[Any]],
    ) -> None:
        transforms: list[tuple[Pipeline[Any, Any], Any]] = []
        for p in siblings:
            result = p.fn(record.data)
            if p.target_topic is None or result is None:
                transforms.append((p, result))
                continue
            target_model = p.target_topic._validate(result)
            transforms.append((p, target_model))

        any_target = any(p.target_topic is not None for p in siblings)
        if not dry_run and any_target:
            with source.source._conn:
                for p, model in transforms:
                    if p.target_topic is not None and model is not None:
                        payload = _model_to_payload(model)
                        p.target_topic._insert_payload_in_current_transaction(payload)
                source.source._conn.execute(
                    """
                    UPDATE messages
                    SET handled_at = ?, status = 'handled'
                    WHERE id = ? AND topic = ?
                    """,
                    (_utc_now(), record.id, source.name),
                )

        for p, model in transforms:
            results[id(p)].append(model)

    def _run_row_best_effort(
        self,
        source: Topic[Any],
        siblings: list[Pipeline[Any, Any]],
        record: Record[Any],
        dry_run: bool,
        results: dict[int, list[Any]],
        failures: list[FanOutFailure],
    ) -> None:
        siblings_done = 0
        for p in siblings:
            try:
                result = p.fn(record.data)
                if p.target_topic is None:
                    results[id(p)].append(result)
                    siblings_done += 1
                    continue
                if result is None:
                    results[id(p)].append(None)
                    siblings_done += 1
                    continue
                target_model = p.target_topic._validate(result)
                if not dry_run:
                    payload = _model_to_payload(target_model)
                    try:
                        with source.source._conn:
                            p.target_topic._insert_payload_in_current_transaction(
                                payload
                            )
                    except DuplicateMessageError:
                        pass
                results[id(p)].append(target_model)
                siblings_done += 1
            except Exception as exc:
                failures.append(
                    FanOutFailure(
                        record_id=record.id,
                        source=source.name,
                        target=p.target_name,
                        exception=exc,
                    )
                )

        any_target = any(p.target_topic is not None for p in siblings)
        if siblings_done == len(siblings) and any_target and not dry_run:
            with source.source._conn:
                source.source._conn.execute(
                    """
                    UPDATE messages
                    SET handled_at = ?, status = 'handled'
                    WHERE id = ? AND topic = ?
                    """,
                    (_utc_now(), record.id, source.name),
                )
