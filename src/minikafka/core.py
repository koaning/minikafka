"""Core implementation of minikafka.

Defines the public surface of the library:

- ``Source`` — a connection to a SQLite database that holds one or more topics.
- ``Topic`` — a typed, append-only queue backed by a Pydantic model.
- ``Pipeline`` and ``FullPipeline`` — chain transformations between topics,
  with single-source and multi-source (fan-out / fan-in) variants.
- ``Record`` — a frozen dataclass returned when iterating topics with
  ``records=True``, exposing storage metadata alongside the decoded payload.
- Exception hierarchy rooted at ``MinikafkaError``.
"""

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
from types import TracebackType
from typing import Any, Generic, Literal, TypeVar, overload

from pydantic import BaseModel

ModelT = TypeVar("ModelT", bound=BaseModel)
OutputT = TypeVar("OutputT", bound=BaseModel)


class MinikafkaError(Exception):
    """Base class for minikafka errors."""


class SchemaMismatchError(MinikafkaError):
    """Raised when a topic exists with incompatible schema or configuration."""


class DuplicateMessageError(MinikafkaError):
    """Raised when a message violates a topic's deduplication constraint."""


@dataclass(frozen=True)
class FanOutFailure:
    """A single sibling failure recorded during a ``best_effort`` fan-out run.

    Instances are collected on the ``failures`` list of ``FanOutError`` when
    ``FullPipeline.run(strategy="best_effort")`` is used. The parent record
    that triggered the failure is left in the ``new`` state so a corrected
    run can retry it.

    Attributes:
        record_id: Primary key of the parent record in its source topic.
        source: Name of the topic that produced the record.
        target: Name of the target topic the sibling pipeline was writing to,
            or ``None`` if the pipeline had no target.
        exception: The original exception raised by the sibling pipeline.
    """

    record_id: int
    source: str
    target: str | None
    exception: BaseException


class FanOutError(MinikafkaError):
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
    """A stored message returned by ``Topic.iter_new`` / ``Topic.iter_handled``.

    Records wrap the decoded Pydantic payload with the storage metadata kept
    in SQLite. They are immutable; use ``Topic.set_handled`` to transition
    a record from ``new`` to ``handled``.

    Attributes:
        id: Auto-incrementing primary key, unique within the database.
        topic: Name of the topic the record belongs to.
        created_at: UTC timestamp of when the record was inserted.
        handled_at: UTC timestamp of when the record was acknowledged, or
            ``None`` if it is still in the ``new`` state.
        status: Either ``"new"`` or ``"handled"``.
        payload_hash: SHA-256 over the canonical JSON of the full payload.
        dedup_hash: SHA-256 over the dedup-field subset, or ``None`` when
            the topic has no dedup configuration.
        data: The decoded Pydantic model instance.

    Examples:
        ```python
        for record in topic.iter_new(records=True):
            print(record.id, record.created_at, record.data)
            topic.set_handled(record=record)
        ```
    """

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
    """SQLite-backed queue source — entry point to minikafka.

    A ``Source`` owns a single SQLite connection and tracks the Pydantic
    model classes registered to its topics. Use ``Source.topic`` to
    create or attach to a topic, and ``Source.full_pipeline`` to compose
    multi-topic pipelines.

    Pass ``on_event`` to observe activity. The callable is invoked as
    ``on_event(event_name, **kwargs)`` for these events:

    - ``topic_created`` — kwargs: ``name``, ``model``, ``dedup``
    - ``message_appended`` — kwargs: ``topic``, ``payload``
    - ``message_handled`` — kwargs: ``topic``, ``id``
    - ``pipeline_start`` — kwargs: ``source``, ``target``
    - ``pipeline_end`` — kwargs: ``source``, ``target``, ``count``, ``dry_run``

    Exceptions raised inside ``on_event`` are swallowed so logging cannot
    break the pipeline.

    Examples:
        ```python
        from pydantic import BaseModel
        from minikafka import Source

        class Video(BaseModel):
            url: str
            title: str

        with Source(":memory:") as source:
            videos = source.topic("videos", Video, dedup=("url",))
            videos.append({"url": "https://example.com", "title": "hello"})
        ```
    """

    def __init__(
        self,
        path: str | Path,
        *,
        on_event: Callable[..., None] | None = None,
    ):
        """Open or create a SQLite-backed source.

        Args:
            path: Path to the SQLite database file. Use ``":memory:"`` for
                an in-process database that is discarded on ``close()``.
            on_event: Optional observer callback invoked as
                ``on_event(event_name, **kwargs)``. See the class docstring
                for the event names and their kwargs.
        """
        self.path = str(path)
        self._on_event = on_event
        self._conn = sqlite3.connect(self.path)
        self._conn.row_factory = sqlite3.Row
        self._models: dict[str, type[BaseModel]] = {}
        self._init_db()

    def _emit(self, event: str, **kwargs: Any) -> None:
        if self._on_event is None:
            return
        try:
            self._on_event(event, **kwargs)
        except Exception:
            pass

    def topic(
        self,
        name: str,
        model: type[ModelT],
        *,
        dedup: Sequence[str] | None,
    ) -> Topic[ModelT]:
        """Create or attach to a typed topic.

        If the topic does not exist in the database it is created with the
        given model's JSON schema and dedup fields. If it already exists,
        both the schema hash and the dedup-field tuple must match — if
        either differs, ``SchemaMismatchError`` is raised.

        The model class is also remembered on this ``Source`` instance so
        that pipelines can resolve topics by name and return decoded
        Pydantic instances.

        Args:
            name: Topic name. Must be stable across reopens.
            model: Pydantic ``BaseModel`` subclass describing the payload.
            dedup: Tuple of field names (must all exist on ``model``) used
                to enforce uniqueness, or ``None`` to disable dedup.

        Returns:
            A ``Topic[ModelT]`` bound to this source.

        Raises:
            SchemaMismatchError: The topic exists with a different schema
                or different dedup configuration.
            TypeError: ``model`` is not a Pydantic ``BaseModel`` subclass.
            ValueError: ``dedup`` references fields that are not on
                ``model``, or contains duplicates.

        Emits:
            ``topic_created`` only when a new row is inserted in the
            ``topics`` table (not when re-attaching to an existing topic).
        """
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
            self._emit(
                "topic_created",
                name=name,
                model=model.__name__,
                dedup=dedup_fields,
            )
        else:
            existing_dedup = tuple(json.loads(existing["dedup_fields"]))
            if existing["schema_hash"] != schema_hash:
                raise SchemaMismatchError(
                    f"topic {name!r} exists with a different schema"
                )
            if existing_dedup != dedup_fields:
                raise SchemaMismatchError(
                    f"topic {name!r} exists with a different dedup config"
                )

        self._models[name] = model
        return Topic(self, name, model)

    def full_pipeline(self, *pipelines: Pipeline[Any, Any]) -> FullPipeline:
        """Compose multiple pipelines into a single executable DAG.

        Pipelines that share a source topic become **siblings** (fan-out);
        pipelines whose target is another pipeline's source form a chain
        (fan-in). The resulting ``FullPipeline`` runs source topics in
        topological order so that downstream pipelines see the rows
        produced upstream within the same ``run()`` call.

        Args:
            *pipelines: Any number of ``Pipeline`` instances built via
                ``topic.pipe(fn).to(target)``.

        Returns:
            A ``FullPipeline`` ready to ``run()`` or ``plot()``.

        Examples:
            ```python
            source.full_pipeline(
                raw.pipe(clean).to(clean_topic),
                clean_topic.pipe(score).to(feed),
            ).run()
            ```
        """
        return FullPipeline(list(pipelines))

    def __enter__(self) -> Source:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self.close()

    def close(self) -> None:
        """Close the underlying SQLite connection.

        After calling ``close`` the ``Source`` (and any ``Topic`` /
        ``Pipeline`` referencing it) can no longer be used.
        """
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
    """Typed, append-only queue backed by a Pydantic model.

    A ``Topic`` is a logical message log inside a ``Source``. Every record
    is validated against the topic's ``model`` before insert and may be
    deduplicated on a chosen tuple of fields. Records start in the ``new``
    state and transition to ``handled`` once acknowledged via
    ``set_handled`` or by being consumed by a pipeline.

    Use ``iter_new`` to stream pending work and ``set_handled`` to ack a
    record. To transform a topic into another, use
    ``topic.pipe(fn).to(target)`` and call ``run()`` on the resulting
    ``Pipeline``.

    Topics are created with ``Source.topic`` — do not construct directly.

    Attributes:
        source: The owning ``Source``.
        name: Topic name (matches the row in the ``topics`` SQLite table).
        model: The Pydantic ``BaseModel`` subclass for this topic.

    Examples:
        ```python
        videos = source.topic("videos", Video, dedup=("url",))
        videos.append({"url": "https://x", "title": "hi"})

        for record in videos.iter_new(records=True):
            print(record.id, record.data.title)
            videos.set_handled(record=record)
        ```
    """

    def __init__(self, source: Source, name: str, model: type[ModelT]):
        self.source = source
        self.name = name
        self.model = model

    def append(self, payload: ModelT | dict[str, Any]) -> ModelT:
        """Validate and insert a new record into the topic.

        The payload is validated against the topic's model (raising
        ``pydantic.ValidationError`` on mismatch) and then inserted as a
        new ``status='new'`` row. If the topic has dedup fields configured
        and another row already exists with the same dedup values,
        ``DuplicateMessageError`` is raised.

        Args:
            payload: Either a Pydantic model instance of the topic's type
                or a ``dict`` that can be coerced into one.

        Returns:
            The validated Pydantic model instance that was stored.

        Raises:
            DuplicateMessageError: A row with the same dedup fields already
                exists in this topic.
            pydantic.ValidationError: The payload does not match the topic's
                model.

        Emits:
            ``message_appended`` with kwargs ``topic`` and ``payload``
            (the canonical dict, not the model instance).
        """
        model = self._validate(payload)
        payload_dict = _model_to_payload(model)
        self._insert_payload(payload_dict)
        return model

    @overload
    def iter_new(
        self, *, records: Literal[True], as_dict: bool = ...
    ) -> Iterator[Record[ModelT]]: ...
    @overload
    def iter_new(self, *, as_dict: Literal[True]) -> Iterator[dict[str, Any]]: ...
    @overload
    def iter_new(self, *, records: bool = ..., as_dict: bool = ...) -> Iterator[ModelT]: ...

    def iter_new(
        self, *, records: bool = False, as_dict: bool = False
    ) -> Iterator[ModelT | Record[ModelT] | dict[str, Any]]:
        """Iterate the topic's ``new`` (unhandled) rows in insertion order.

        Args:
            records: If ``True``, yield ``Record`` instances with storage
                metadata. Defaults to ``False`` (yield decoded models).
            as_dict: If ``True``, yield raw payload dicts and skip Pydantic
                validation. Useful for ``to_polars``. Defaults to ``False``.

        Yields:
            Decoded ``ModelT`` instances, ``Record[ModelT]`` wrappers, or
            plain ``dict`` payloads depending on the flags above.

        Raises:
            ValueError: ``records`` and ``as_dict`` are both ``True``.

        Examples:
            ```python
            for video in topic.iter_new():
                print(video.title)

            for record in topic.iter_new(records=True):
                print(record.id, record.created_at, record.data)
            ```
        """
        yield from self._iter(status="new", records=records, as_dict=as_dict)

    @overload
    def iter_handled(
        self, *, records: Literal[True], as_dict: bool = ...
    ) -> Iterator[Record[ModelT]]: ...
    @overload
    def iter_handled(self, *, as_dict: Literal[True]) -> Iterator[dict[str, Any]]: ...
    @overload
    def iter_handled(self, *, records: bool = ..., as_dict: bool = ...) -> Iterator[ModelT]: ...

    def iter_handled(
        self, *, records: bool = False, as_dict: bool = False
    ) -> Iterator[ModelT | Record[ModelT] | dict[str, Any]]:
        """Iterate the topic's ``handled`` rows in insertion order.

        Identical to ``iter_new`` but yields rows whose status has been
        transitioned to ``handled``. Useful for auditing or replays.

        Args:
            records: If ``True``, yield ``Record`` instances.
            as_dict: If ``True``, yield raw payload dicts.

        Yields:
            Decoded ``ModelT`` instances, ``Record[ModelT]`` wrappers, or
            plain ``dict`` payloads.
        """
        yield from self._iter(status="handled", records=records, as_dict=as_dict)

    def set_handled(
        self, _id: int | None = None, *, record: Record[Any] | None = None
    ) -> None:
        """Mark a record as ``handled``.

        Pass either the integer record id positionally or a ``Record``
        instance via the ``record=`` keyword. The transition is idempotent:
        calling ``set_handled`` on an already-handled row simply refreshes
        ``handled_at``.

        Args:
            _id: Numeric primary key of the record to ack.
            record: Alternative — a ``Record`` returned by ``iter_new`` /
                ``iter_handled`` with ``records=True``.

        Raises:
            ValueError: Neither ``_id`` nor ``record`` was provided.

        Emits:
            ``message_handled`` with kwargs ``topic`` and ``id``.
        """
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
        self.source._emit("message_handled", topic=self.name, id=message_id)

    def count(self, status: str | None = None) -> int:
        """Return the number of records in this topic.

        Args:
            status: Optional filter — ``"new"``, ``"handled"``, or ``None``
                (default) to count all records regardless of status.

        Returns:
            The record count as an integer.

        Examples:
            ```python
            topic.count()            # all records
            topic.count("new")       # only unhandled
            topic.count("handled")   # only acknowledged
            ```
        """
        if status is not None:
            row = self.source._conn.execute(
                "SELECT COUNT(*) FROM messages WHERE topic = ? AND status = ?",
                (self.name, status),
            ).fetchone()
        else:
            row = self.source._conn.execute(
                "SELECT COUNT(*) FROM messages WHERE topic = ?",
                (self.name,),
            ).fetchone()
        return row[0]

    def pipe(self, fn: Callable[[ModelT], Any]) -> Pipeline[ModelT, Any]:
        """Start building a pipeline that transforms this topic.

        Returns a ``Pipeline`` with ``self`` as the source and ``fn`` as
        the transformation. Chain ``.to(target)`` to designate a target
        topic, then call ``.run()`` to execute. Without ``.to(...)`` the
        pipeline simply collects the return values of ``fn``.

        Args:
            fn: Callable that takes a model instance of this topic's type
                and returns either a model instance for the target topic,
                a ``dict`` that validates against the target's model, or
                ``None`` (which acks the source record without writing).

        Returns:
            A ``Pipeline[ModelT, Any]`` ready for ``.to(target)``.

        Examples:
            ```python
            raw.pipe(clean).to(clean_topic).run()
            ```
        """
        return Pipeline(self, fn)

    def migrate(
        self,
        new_model: type[OutputT],
        migration_function: Callable[[ModelT], OutputT | dict[str, Any]],
    ) -> Topic[OutputT]:
        """Rewrite every stored payload using a new model.

        Runs ``migration_function`` on each row's decoded payload, validates
        the result against ``new_model``, and rewrites the row in place.
        The topic's recorded schema and schema hash are updated atomically
        with the rows. The dedup-field tuple is preserved and must still
        be present on ``new_model``.

        Args:
            new_model: Target Pydantic ``BaseModel`` subclass.
            migration_function: Callable that converts an old model instance
                into a new model instance (or a dict that validates as one).

        Returns:
            A new ``Topic[OutputT]`` bound to the same name and source.

        Raises:
            ValueError: The previous dedup fields are not present on
                ``new_model``.
            TypeError: ``new_model`` is not a Pydantic ``BaseModel`` subclass.
            pydantic.ValidationError: A migrated payload does not match
                ``new_model``.
        """
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

    def to_polars(self) -> Any:
        """Return all rows in this topic as a Polars ``DataFrame``.

        Includes both ``new`` and ``handled`` rows. Only flat (non-nested)
        payloads are supported — nested ``dict`` / ``list`` / ``tuple``
        values raise ``ValueError``.

        Returns:
            A ``polars.DataFrame`` with one row per record.

        Raises:
            ImportError: ``polars`` is not installed. Install with
                ``pip install minikafka[polars]``.
            ValueError: A payload contains a nested collection.
        """
        try:
            import polars as pl
        except ImportError as exc:
            raise ImportError(
                "to_polars() requires the optional 'polars' dependency"
            ) from exc
        rows = [
            json.loads(row["payload_json"])
            for row in self.source._conn.execute(
                "SELECT payload_json FROM messages WHERE topic = ? ORDER BY id",
                (self.name,),
            )
        ]
        for row in rows:
            for key, value in row.items():
                if isinstance(value, (dict, list, tuple)):
                    raise ValueError(f"to_polars only supports flat payloads; {key!r} is nested")
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
        self.source._emit("message_appended", topic=self.name, payload=payload_dict)

    def _dedup_hash_for_payload(
        self, payload_dict: dict[str, Any], dedup_fields: Sequence[str]
    ) -> str | None:
        if not dedup_fields:
            return None
        return _stable_hash({field: payload_dict[field] for field in dedup_fields})


class Pipeline(Generic[ModelT, OutputT]):
    """Single-source transformation pipeline between two topics.

    Build with ``topic.pipe(fn)`` and optionally ``.to(target)``. When run,
    the pipeline iterates the source topic's ``new`` rows, applies ``fn``
    to each decoded payload, writes the result to the target (if any), and
    marks the source row as ``handled``. Each row is processed inside a
    single SQLite transaction.

    If ``fn`` returns ``None`` for a row, the source record is still
    acked but nothing is written to the target.

    Attributes:
        source_topic: The topic rows are read from.
        fn: The transformation callable.
        target_topic: The target topic, or ``None`` until ``.to(...)`` is
            called.

    Examples:
        ```python
        pipeline = raw.pipe(clean).to(clean_topic)
        results = pipeline.run()
        print(pipeline.plot())  # Mermaid graph
        ```
    """

    def __init__(self, source_topic: Topic[ModelT], fn: Callable[[ModelT], OutputT]):
        self.source_topic = source_topic
        self.fn = fn
        self.target_topic: Topic[Any] | None = None

    def to(self, target: Topic[OutputT] | str) -> Pipeline[ModelT, OutputT]:
        """Designate the target topic for this pipeline.

        Args:
            target: Either a ``Topic`` instance or the string name of a
                topic already registered on the source's ``Source``.

        Returns:
            ``self`` — so calls can be chained
            (``topic.pipe(fn).to(target).run()``).

        Raises:
            KeyError: ``target`` is a string and no topic by that name is
                registered on the underlying ``Source`` (call
                ``Source.topic(name, model)`` first to register it).
        """
        if isinstance(target, str):
            target = self.source_topic.source._registered_topic(target)
        self.target_topic = target
        return self

    def run(self, *, dry_run: bool = False) -> list[Any]:
        """Execute the pipeline over every ``new`` row in the source topic.

        For each row: decode the payload, call ``fn``, validate the result
        against the target's model (if any), then within a single
        transaction insert the result and mark the source row as
        ``handled``. Returns the list of transformed outputs in
        insertion order.

        Args:
            dry_run: If ``True``, run ``fn`` and validate the results but
                do not write anything to the target topic or transition
                source rows. Useful for previewing.

        Returns:
            The list of outputs in source-row order. If the pipeline has
            no target, this is the raw return value of ``fn``; with a
            target, it is the validated target-model instance (or ``None``
            for rows where ``fn`` returned ``None``).

        Raises:
            DuplicateMessageError: An output row would violate the target
                topic's dedup constraint.
            pydantic.ValidationError: ``fn`` returned a value that does not
                validate against the target topic's model.

        Emits:
            ``pipeline_start`` before processing and ``pipeline_end`` after
            (with ``count`` and ``dry_run``). Each handled source row also
            emits ``message_handled``.
        """
        source = self.source_topic.source
        source._emit(
            "pipeline_start",
            source=self.source_topic.name,
            target=self.target_name,
        )
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
            with source._conn:
                self.target_topic._insert_payload_in_current_transaction(target_payload)
                source._conn.execute(
                    """
                    UPDATE messages
                    SET handled_at = ?, status = 'handled'
                    WHERE id = ? AND topic = ?
                    """,
                    (_utc_now(), record.id, self.source_topic.name),
                )
            source._emit(
                "message_handled", topic=self.source_topic.name, id=record.id
            )
        source._emit(
            "pipeline_end",
            source=self.source_topic.name,
            target=self.target_name,
            count=len(results),
            dry_run=dry_run,
        )
        return results

    @property
    def source_name(self) -> str:
        """Name of the source topic."""
        return self.source_topic.name

    @property
    def target_name(self) -> str | None:
        """Name of the target topic, or ``None`` if no target was set."""
        return self.target_topic.name if self.target_topic is not None else None

    def plot(self) -> str:
        """Return a one-edge Mermaid ``graph TD`` diagram for this pipeline.

        Equivalent to wrapping ``self`` in a ``FullPipeline`` and calling
        ``plot`` on it. Drop the returned string into a fenced
        ` ```mermaid ` block in markdown to render the diagram.
        """
        return FullPipeline([self]).plot()


class FullPipeline:
    """Multi-pipeline DAG runner with fan-out and fan-in support.

    Build with ``Source.full_pipeline(p1, p2, ...)``. Pipelines that share
    a source topic become **siblings** (fan-out): every source row is fed
    to each sibling's ``fn``. Pipelines whose source topic is another
    pipeline's target form a **chain** (fan-in): source topics are run in
    topological order so that the same call processes the new rows
    produced upstream.

    Two execution strategies are available:

    - ``"strict"`` (default) — sibling transforms are computed for a row,
      then the inserts and the source-row ack happen inside one SQLite
      transaction. Any exception aborts the whole run with no partial
      state.
    - ``"best_effort"`` — each sibling runs in isolation; failures are
      collected into ``FanOutFailure`` records. The parent row is marked
      ``handled`` only when **every** sibling succeeded. After the run
      finishes, any failures raise ``FanOutError`` so they cannot be
      ignored, but the successful sibling writes are preserved.

    Attributes:
        pipelines: The list of pipelines, in the order they were passed in.
    """

    def __init__(self, pipelines: list[Pipeline[Any, Any]]):
        self.pipelines = pipelines

    def run(
        self,
        *,
        dry_run: bool = False,
        strategy: Literal["strict", "best_effort"] = "strict",
    ) -> list[list[Any]]:
        """Execute the DAG.

        Args:
            dry_run: If ``True``, run all transforms and validate their
                outputs but do not write to any target topic or transition
                source rows.
            strategy: Either ``"strict"`` (transactional, abort on any
                error) or ``"best_effort"`` (collect failures, ack parent
                only when all siblings succeed, raise at the end).

        Returns:
            A list of result lists — one inner list per input pipeline,
            in the same order that ``pipelines`` was constructed in.

        Raises:
            ValueError: ``strategy`` is not one of ``"strict"`` or
                ``"best_effort"``. Also raised if the DAG contains a cycle.
            FanOutError: One or more siblings failed during a
                ``best_effort`` run.

        Examples:
            ```python
            results = source.full_pipeline(
                raw.pipe(clean).to(clean_topic),
                clean_topic.pipe(score).to(feed),
            ).run(strategy="best_effort")
            ```
        """
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
        """Return a Mermaid ``graph TD`` representation of the DAG.

        Each pipeline contributes one edge ``source --> target``;
        pipelines without a target produce a bare node. Embed the
        returned string in a fenced ` ```mermaid ` block to render.

        Returns:
            The Mermaid graph as a single string.
        """
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
