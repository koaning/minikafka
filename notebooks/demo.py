import marimo

__generated_with = "0.23.6"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo

    return (mo,)


@app.cell
def _(mo):
    mo.md(r"""
    # slimlink demo

    This notebook demonstrates the v1 queue API against an in-memory SQLite
    database: topic creation, validation, deduplication, record metadata,
    pipeline execution, migration, and Polars export.
    """)
    return


@app.cell
def _():
    from pydantic import BaseModel

    from slimlink import DuplicateMessageError, Source

    class YouTubeVideo(BaseModel):
        creator: str
        url: str
        video_length_seconds: int

    class Creator(BaseModel):
        name: str

    class VideoSummary(BaseModel):
        creator: str
        url: str

    return Creator, DuplicateMessageError, Source, VideoSummary, YouTubeVideo


@app.cell
def _(Creator, Source, YouTubeVideo):
    src = Source(":memory:")
    videos = src.topic(
        "videos",
        YouTubeVideo,
        dedup=("creator", "url"),
    )
    creators = src.topic("creators", Creator, dedup=("name",))
    return creators, src, videos


@app.cell
def _(DuplicateMessageError, videos):
    sample_payloads = [
        {
            "creator": "Nina",
            "url": "https://example.com/nina/sqlite-queues",
            "video_length_seconds": 420,
        },
        {
            "creator": "Omar",
            "url": "https://example.com/omar/pydantic",
            "video_length_seconds": 275,
        },
        {
            "creator": "Nina",
            "url": "https://example.com/nina/sqlite-queues",
            "video_length_seconds": 999,
        },
    ]

    append_results = []
    for payload in sample_payloads:
        try:
            video = videos.append(payload)
            append_results.append({"status": "appended", **video.model_dump()})
        except DuplicateMessageError as exc:
            append_results.append(
                {
                    "status": "duplicate",
                    "creator": payload["creator"],
                    "url": payload["url"],
                    "error": str(exc),
                }
            )

    append_results
    return (append_results,)


@app.cell
def _(append_results, mo):
    mo.md("## Append and deduplication")
    append_results
    return


@app.cell
def _(videos):
    new_video_dicts = list(videos.iter_new(as_dict=True))
    new_video_records = list(videos.iter_new(records=True))
    return new_video_dicts, new_video_records


@app.cell
def _(mo, new_video_dicts):
    mo.md("## Default iteration returns Pydantic models")
    new_video_dicts
    return


@app.cell
def _(new_video_records):
    record_preview = [
        {
            "id": record.id,
            "topic": record.topic,
            "status": record.status,
            "created_at": record.created_at.isoformat(),
            "handled_at": record.handled_at,
            "data": record.data.model_dump(),
        }
        for record in new_video_records
    ]
    record_preview
    return


@app.cell
def _(Creator, creators, videos):
    dry_run_result = videos.pipe(lambda video: Creator(name=video.creator)).to(creators).run(
        dry_run=True
    )
    dry_run_state = {
        "dry_run_result": [creator.model_dump() for creator in dry_run_result],
        "videos_still_new": len(list(videos.iter_new())),
        "creators_written": len(list(creators.iter_new())),
    }
    dry_run_state
    return (dry_run_state,)


@app.cell
def _(Creator, creators, dry_run_state, videos):
    dry_run_state
    pipeline_result = videos.pipe(lambda video: Creator(name=video.creator)).to(creators).run()
    pipeline_state = {
        "pipeline_result": [creator.model_dump() for creator in pipeline_result],
        "videos_new": len(list(videos.iter_new())),
        "videos_handled": len(list(videos.iter_handled())),
        "creators_new": [creator.model_dump() for creator in creators.iter_new()],
    }
    pipeline_state
    return (pipeline_state,)


@app.cell
def _(Creator, VideoSummary, creators, src, videos):
    summaries = src.topic("summaries", VideoSummary, dedup=None)
    graph = src.full_pipeline(
        videos.pipe(lambda video: VideoSummary(creator=video.creator, url=video.url)).to(
            summaries
        ),
        summaries.pipe(lambda video: Creator(name=video.creator)).to(creators),
    ).plot()
    graph
    return (graph,)


@app.cell
def _(mo, pipeline_state):
    mo.md("## Pipeline writes target rows and marks source rows handled")
    pipeline_state
    return


@app.cell
def _(graph, mo):
    mo.md(f"""
    ## Pipeline graph\n\n```mermaid\n{graph}\n```
    """)
    return


@app.cell
def _(VideoSummary, pipeline_state, videos):
    pipeline_state
    migrated_videos = videos.migrate(
        VideoSummary,
        lambda video: {"creator": video.creator, "url": video.url},
    )
    migrated_payloads = list(migrated_videos.iter_handled(as_dict=True))
    migrated_payloads
    return (migrated_videos,)


@app.cell
def _(migrated_videos):
    polars_frame = migrated_videos.to_polars()
    polars_frame
    return


if __name__ == "__main__":
    app.run()
