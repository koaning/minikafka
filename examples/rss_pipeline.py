"""Demo: raw RSS topics -> clean -> merged feed -> filtered_feed.

Run with: uv run python examples/rss_pipeline.py
"""

from __future__ import annotations

import re
from typing import Any

from pydantic import BaseModel

from slimlink import Source


class RawYouTube(BaseModel):
    creator: str
    title: str
    url: str
    length_seconds: int
    raw_description: str


class RawBlog(BaseModel):
    author: str
    title: str
    url: str
    html_body: str


class RawHackerNews(BaseModel):
    user: str
    title: str
    url: str
    points: int
    comments: int


class CleanItem(BaseModel):
    source: str
    author: str
    title: str
    url: str
    summary: str


class FeedItem(BaseModel):
    source: str
    author: str
    title: str
    url: str
    summary: str
    score: int


def log(event: str, **kwargs: Any) -> None:
    if event == "topic_created":
        print(f"  topic: {kwargs['name']}  dedup={kwargs['dedup']}")
    elif event == "message_appended":
        title = kwargs["payload"].get("title", "")[:60]
        print(f"  + {kwargs['topic']:30s}  {title}")
    elif event == "pipeline_start":
        print(f"\n-> {kwargs['source']} -> {kwargs['target']}")
    elif event == "pipeline_end":
        print(f"   done ({kwargs['count']} records)")


TAG_RE = re.compile(r"<[^>]+>")


def clean_youtube(item: RawYouTube) -> CleanItem:
    summary = item.raw_description.strip().split("\n", 1)[0][:120]
    return CleanItem(
        source="youtube",
        author=item.creator,
        title=item.title.strip(),
        url=item.url,
        summary=summary,
    )


def clean_blog(item: RawBlog) -> CleanItem:
    text = TAG_RE.sub("", item.html_body).strip()
    return CleanItem(
        source="blog",
        author=item.author,
        title=item.title.strip(),
        url=item.url,
        summary=text[:120],
    )


def clean_hn(item: RawHackerNews) -> CleanItem:
    return CleanItem(
        source="hackernews",
        author=item.user,
        title=item.title.strip(),
        url=item.url,
        summary=f"{item.points} points, {item.comments} comments",
    )


def youtube_to_feed(item: CleanItem) -> FeedItem:
    # crude score: longer summaries treated as richer content
    return FeedItem(**item.model_dump(), score=min(len(item.summary), 100))


def blog_to_feed(item: CleanItem) -> FeedItem:
    return FeedItem(**item.model_dump(), score=min(len(item.summary), 100))


def hn_to_feed(item: CleanItem) -> FeedItem:
    points = int(item.summary.split(" ", 1)[0])
    return FeedItem(**item.model_dump(), score=points)


SCORE_THRESHOLD = 50


def score_filter(item: FeedItem) -> FeedItem | None:
    return item if item.score >= SCORE_THRESHOLD else None


def main() -> None:
    print("== setting up topics ==")
    src = Source(":memory:", on_event=log)

    raw_yt = src.topic("raw.rss.youtube", RawYouTube, dedup=("url",))
    raw_blog = src.topic("raw.rss.blogs", RawBlog, dedup=("url",))
    raw_hn = src.topic("raw.rss.hackernews", RawHackerNews, dedup=("url",))

    clean_yt = src.topic("clean.rss.youtube", CleanItem, dedup=("url",))
    clean_blog_t = src.topic("clean.rss.blogs", CleanItem, dedup=("url",))
    clean_hn_t = src.topic("clean.rss.hackernews", CleanItem, dedup=("url",))

    feed = src.topic("feed", FeedItem, dedup=("url",))
    filtered_feed = src.topic("filtered_feed", FeedItem, dedup=("url",))

    print("\n== seeding raw topics ==")
    raw_yt.append(
        RawYouTube(
            creator="3blue1brown",
            title="  But what is a Fourier series?  ",
            url="https://yt/3b1b-fourier",
            length_seconds=1320,
            raw_description="A visual intro to Fourier series.\nMore details below...",
        )
    )
    raw_yt.append(
        RawYouTube(
            creator="Veritasium",
            title="The mystery of spinning ice",
            url="https://yt/veritasium-ice",
            length_seconds=900,
            raw_description="short",
        )
    )

    raw_blog.append(
        RawBlog(
            author="Simon Willison",
            title="Notes on running local LLMs",
            url="https://simonw/local-llms",
            html_body="<p>I have been running <b>local</b> LLMs all week and the results are surprisingly usable.</p>",
        )
    )
    raw_blog.append(
        RawBlog(
            author="Random",
            title="hello",
            url="https://blog/hello",
            html_body="<p>hi</p>",
        )
    )

    raw_hn.append(
        RawHackerNews(
            user="pg",
            title="Show HN: a tiny SQLite queue",
            url="https://hn/sqlite-queue",
            points=412,
            comments=87,
        )
    )
    raw_hn.append(
        RawHackerNews(
            user="anon",
            title="Ask HN: best chairs?",
            url="https://hn/chairs",
            points=12,
            comments=3,
        )
    )

    print("\n== running the full pipeline ==")
    src.full_pipeline(
        raw_yt.pipe(clean_youtube).to(clean_yt),
        raw_blog.pipe(clean_blog).to(clean_blog_t),
        raw_hn.pipe(clean_hn).to(clean_hn_t),
        clean_yt.pipe(youtube_to_feed).to(feed),
        clean_blog_t.pipe(blog_to_feed).to(feed),
        clean_hn_t.pipe(hn_to_feed).to(feed),
        feed.pipe(score_filter).to(filtered_feed),
    ).run()

    print("\n== filtered_feed contents ==")
    for item in filtered_feed.iter_new():
        print(f"  [{item.score:3d}] {item.source:10s} {item.title}")


if __name__ == "__main__":
    main()
