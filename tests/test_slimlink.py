from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import BaseModel, ValidationError

from slimlink import (
    DuplicateMessageError,
    Record,
    SchemaMismatchError,
    Source,
)


class Video(BaseModel):
    creator: str
    url: str
    video_length_seconds: int


class ShortVideo(BaseModel):
    creator: str
    url: str


class Creator(BaseModel):
    name: str


class NestedVideo(BaseModel):
    creator: str
    tags: list[str]


def test_topic_creates_reregisters_and_rejects_schema_mismatch():
    src = Source(":memory:")
    topic = src.topic("videos", Video, dedup=("creator", "url"))

    recreated = src.topic("videos", Video, dedup=("url", "creator"))

    assert recreated.name == "videos"
    assert recreated.model is Video
    assert topic.name == recreated.name

    with pytest.raises(SchemaMismatchError):
        src.topic("videos", ShortVideo, dedup=("creator", "url"))


def test_topic_requires_python_model_class():
    src = Source(":memory:")

    with pytest.raises(TypeError, match="BaseModel class"):
        src.topic("videos", Video(creator="a", url="u", video_length_seconds=1))


def test_topic_requires_model_argument():
    src = Source(":memory:")

    with pytest.raises(TypeError):
        src.topic("videos")


def test_reopened_topic_requires_matching_model_class(tmp_path: Path):
    db = tmp_path / "queue.sqlite"
    Source(db).topic("videos", Video)

    reopened = Source(db)
    assert reopened.topic("videos", Video).name == "videos"


def test_reopened_topic_rejects_incompatible_model_class_early(tmp_path: Path):
    db = tmp_path / "queue.sqlite"
    Source(db).topic("videos", Video)

    reopened = Source(db)

    with pytest.raises(SchemaMismatchError, match="different schema"):
        reopened.topic("videos", ShortVideo)


def test_reopened_topic_rejects_dedup_mismatch_early(tmp_path: Path):
    db = tmp_path / "queue.sqlite"
    Source(db).topic("videos", Video, dedup=("creator", "url"))

    reopened = Source(db)

    assert reopened.topic("videos", Video, dedup=("url", "creator")).name == "videos"
    with pytest.raises(SchemaMismatchError, match="dedup config"):
        reopened.topic("videos", Video, dedup=("creator",))


def test_topic_rejects_invalid_dedup_fields_early():
    src = Source(":memory:")

    with pytest.raises(ValueError, match="not present"):
        src.topic("videos", Video, dedup=("missing",))

    with pytest.raises(ValueError, match="unique"):
        src.topic("other-videos", Video, dedup=("url", "url"))


def test_append_validation_and_dedup():
    src = Source(":memory:")
    topic = src.topic("videos", Video, dedup=("creator", "url"))

    topic.append({"creator": "a", "url": "u", "video_length_seconds": 10})

    with pytest.raises(ValidationError):
        topic.append({"creator": "a"})

    with pytest.raises(DuplicateMessageError):
        topic.append({"creator": "a", "url": "u", "video_length_seconds": 20})


def test_iteration_shapes_and_handled_filtering():
    src = Source(":memory:")
    topic = src.topic("videos", Video)
    topic.append({"creator": "a", "url": "u", "video_length_seconds": 10})

    assert list(topic.iter_new())[0] == Video(creator="a", url="u", video_length_seconds=10)
    assert list(topic.iter_new(as_dict=True))[0]["creator"] == "a"

    record = list(topic.iter_new(records=True))[0]
    assert isinstance(record, Record)
    assert record.data.creator == "a"

    topic.set_handled(record=record)

    assert list(topic.iter_new()) == []
    assert list(topic.iter_handled())[0].creator == "a"


def test_pipeline_without_target_returns_results_without_mutation():
    src = Source(":memory:")
    topic = src.topic("videos", Video)
    topic.append({"creator": "a", "url": "u", "video_length_seconds": 10})

    result = topic.pipe(lambda video: video.video_length_seconds + 1).run()

    assert result == [11]
    assert len(list(topic.iter_new())) == 1


def test_pipeline_to_target_appends_and_marks_source_handled():
    src = Source(":memory:")
    videos = src.topic("videos", Video)
    creators = src.topic("creators", Creator, dedup=("name",))
    videos.append({"creator": "a", "url": "u", "video_length_seconds": 10})

    videos.pipe(lambda video: Creator(name=video.creator)).to(creators).run()

    assert list(videos.iter_new()) == []
    assert list(videos.iter_handled())[0].creator == "a"
    assert list(creators.iter_new()) == [Creator(name="a")]


def test_pipeline_to_registered_target_name():
    src = Source(":memory:")
    videos = src.topic("videos", Video)
    src.topic("creators", Creator)
    videos.append({"creator": "a", "url": "u", "video_length_seconds": 10})

    videos.pipe(lambda video: Creator(name=video.creator)).to("creators").run()

    assert list(src.topic("creators", Creator).iter_new()) == [Creator(name="a")]


def test_pipeline_failure_leaves_source_new():
    src = Source(":memory:")
    videos = src.topic("videos", Video)
    creators = src.topic("creators", Creator)
    videos.append({"creator": "a", "url": "u", "video_length_seconds": 10})

    with pytest.raises(ValidationError):
        videos.pipe(lambda video: {"not_name": video.creator}).to(creators).run()

    assert len(list(videos.iter_new())) == 1
    assert list(creators.iter_new()) == []


def test_dry_run_validates_without_writes_or_handled_flags():
    src = Source(":memory:")
    videos = src.topic("videos", Video)
    creators = src.topic("creators", Creator)
    videos.append({"creator": "a", "url": "u", "video_length_seconds": 10})

    result = videos.pipe(lambda video: Creator(name=video.creator)).to(creators).run(dry_run=True)

    assert result == [Creator(name="a")]
    assert len(list(videos.iter_new())) == 1
    assert list(creators.iter_new()) == []


def test_migration_commits_only_after_all_rows_validate():
    src = Source(":memory:")
    videos = src.topic("videos", Video, dedup=("creator", "url"))
    videos.append({"creator": "a", "url": "u1", "video_length_seconds": 10})
    videos.append({"creator": "b", "url": "u2", "video_length_seconds": 20})

    migrated = videos.migrate(
        ShortVideo,
        lambda video: {"creator": video.creator, "url": video.url},
    )

    assert list(migrated.iter_new()) == [
        ShortVideo(creator="a", url="u1"),
        ShortVideo(creator="b", url="u2"),
    ]


def test_failed_migration_leaves_payloads_unchanged():
    src = Source(":memory:")
    videos = src.topic("videos", Video)
    videos.append({"creator": "a", "url": "u1", "video_length_seconds": 10})

    with pytest.raises(ValidationError):
        videos.migrate(ShortVideo, lambda video: {"creator": video.creator})

    assert list(videos.iter_new()) == [Video(creator="a", url="u1", video_length_seconds=10)]


def test_file_backed_sqlite(tmp_path: Path):
    db = tmp_path / "queue.sqlite"
    src = Source(db)
    src.topic("videos", Video).append(
        {"creator": "a", "url": "u", "video_length_seconds": 10}
    )

    reopened = Source(db)
    assert list(reopened.topic("videos", Video).iter_new())[0].creator == "a"


def test_to_polars_rejects_nested_payloads():
    src = Source(":memory:")
    topic = src.topic("videos", NestedVideo)
    topic.append({"creator": "a", "tags": ["x"]})

    with pytest.raises(ValueError, match="flat payloads"):
        topic.to_polars()


def test_to_polars_for_flat_payloads():
    src = Source(":memory:")
    topic = src.topic("videos", Video)
    topic.append({"creator": "a", "url": "u", "video_length_seconds": 10})

    frame = topic.to_polars()

    assert frame.shape == (1, 3)
    assert frame["creator"].to_list() == ["a"]


def test_on_event_callback_fires_for_pipeline_run():
    events: list[tuple[str, dict]] = []

    def listener(event, **kwargs):
        events.append((event, kwargs))

    src = Source(":memory:", on_event=listener)
    videos = src.topic("videos", Video)
    creators = src.topic("creators", Creator)
    videos.append({"creator": "a", "url": "u", "video_length_seconds": 10})

    videos.pipe(lambda v: Creator(name=v.creator)).to(creators).run()

    names = [name for name, _ in events]
    assert names.count("topic_created") == 2
    assert ("message_appended", {"topic": "videos", "payload": {"creator": "a", "url": "u", "video_length_seconds": 10}}) in events

    pipeline_starts = [kw for name, kw in events if name == "pipeline_start"]
    assert pipeline_starts == [{"source": "videos", "target": "creators"}]

    pipeline_ends = [kw for name, kw in events if name == "pipeline_end"]
    assert pipeline_ends == [{"source": "videos", "target": "creators", "count": 1, "dry_run": False}]

    handled = [kw for name, kw in events if name == "message_handled"]
    assert handled == [{"topic": "videos", "id": 1}]

    appended_topics = [kw["topic"] for name, kw in events if name == "message_appended"]
    assert appended_topics == ["videos", "creators"]


def test_on_event_callback_errors_are_swallowed():
    def bad(event, **kwargs):
        raise RuntimeError("boom")

    src = Source(":memory:", on_event=bad)
    videos = src.topic("videos", Video)
    videos.append({"creator": "a", "url": "u", "video_length_seconds": 10})

    assert list(videos.iter_new()) == [Video(creator="a", url="u", video_length_seconds=10)]


def test_full_pipeline_sorts_and_plots():
    src = Source(":memory:")
    videos = src.topic("videos", Video)
    short = src.topic("short", ShortVideo)
    creators = src.topic("creators", Creator)
    videos.append({"creator": "a", "url": "u", "video_length_seconds": 10})

    p1 = videos.pipe(lambda video: ShortVideo(creator=video.creator, url=video.url)).to(short)
    p2 = short.pipe(lambda video: Creator(name=video.creator)).to(creators)

    result = src.full_pipeline(p2, p1).run()

    assert len(result) == 2
    assert list(creators.iter_new()) == [Creator(name="a")]
    assert src.full_pipeline(p1, p2).plot() == (
        "graph TD\n    videos --> short\n    short --> creators"
    )
