# minikafka

`minikafka` is a tiny typed queue backed by SQLite.

```python
from pydantic import BaseModel
from minikafka import Source


class YouTubeVideo(BaseModel):
    creator: str
    url: str
    video_length_seconds: int


with Source(":memory:") as src:
    videos = src.topic("videos", YouTubeVideo, dedup=("creator", "url"))

    videos.append(
        {
            "creator": "example",
            "url": "https://example.com/video",
            "video_length_seconds": 120,
        }
    )

    for video in videos.iter_new():
        print(video.creator)
```

After reopening a database in a new Python process, pass the model class when
retrieving a topic:

```python
with Source("queue.sqlite") as src:
    videos = src.topic("videos", YouTubeVideo, dedup=("creator", "url"))
```

The database stores the Pydantic JSON schema and uses it to reject incompatible
models, but it does not recreate Python classes from that schema. If a topic
uses deduplication, pass the same dedup fields when reopening it.
