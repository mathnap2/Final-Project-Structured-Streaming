"""
Final Project Part II - Kafka producer
Music streaming service: users playing, skipping and liking songs.

Simulates the event feed of a music app. Every event is one JSON message:
    {"event_id": "...", "event_type": "play" | "skip" | "like",
     "user_id": 7, "song_id": 10203, "ms_played": 184000,
     "device": "mobile", "event_ts_ms": 1795370000000}

The catalog (artists, albums, songs, users) comes from catalog/*.csv, the same
music_stream dataset we used in Labs 04 and 06 (+ songs.csv / users.csv from
build_catalog.py). Messages only carry ids: the consumer joins them back to the
catalog, like a real app where the client only knows ids.

To make it feel real:
  - every user has 1-2 favorite genres: they mostly play those and skip them less
  - "skip" = stopped before 30 s, "play" = listened 30 s or more
  - after a play the user may "like" the song (more likely in a favorite genre)
  - ~10% of events arrive late (event time up to 90 s in the past)
  - a small % of messages are dirty on purpose (bad ids, negative ms, unknown
    event types, exact duplicates), so the consumer has something to clean

Run it inside the spark-notebook container (kafka-python is already installed):
    python3 producer.py --broker kafka:9093 --topic music-events
"""
import argparse
import csv
import json
import os
import random
import time
import uuid

from kafka import KafkaProducer

HERE = os.path.dirname(os.path.abspath(__file__))
CATALOG = os.path.join(HERE, "catalog")

DEVICES = ["mobile", "mobile", "mobile", "desktop", "web", "smart_speaker"]  # mobile shows up more
SKIP_LIMIT_MS = 30_000


def parse_args():
    parser = argparse.ArgumentParser(description="Music streaming events -> Kafka")
    parser.add_argument("--broker", default="kafka:9093", help="Kafka bootstrap server")
    parser.add_argument("--topic", default="music-events", help="Kafka topic")
    parser.add_argument("--rate", type=float, default=5.0, help="events per second (roughly)")
    parser.add_argument("--max-events", type=int, default=0, help="stop after N events (0 = run until Ctrl+C)")
    parser.add_argument("--dirty-rate", type=float, default=0.03, help="fraction of dirty messages (0 = all clean)")
    parser.add_argument("--late-rate", type=float, default=0.10, help="fraction of events sent with an older timestamp")
    parser.add_argument("--seed", type=int, default=None, help="random seed, for repeatable runs")
    parser.add_argument("--quiet", action="store_true", help="don't print every event")
    return parser.parse_args()


# ---------------------------------------------------------------------------
# Catalog
# ---------------------------------------------------------------------------
def read_csv(name):
    with open(os.path.join(CATALOG, name), newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def load_catalog():
    """Returns songs (with artist + genres attached), users, and songs grouped by genre."""
    artists = {}
    for row in read_csv("artists.csv"):
        info = json.loads(row["artist_info"])
        artists[int(row["artist_id"])] = {"name": info["artist_name"], "genres": info["genres"]}

    album_artist = {int(r["album_id"]): int(r["artist_id"]) for r in read_csv("albums.csv")}

    songs = []
    for row in read_csv("songs.csv"):
        artist = artists[album_artist[int(row["album_id"])]]
        songs.append({
            "song_id": int(row["song_id"]),
            "title": row["title"],
            "duration_ms": int(row["duration_ms"]),
            "artist": artist["name"],
            "genres": artist["genres"],
        })

    by_genre = {}
    for song in songs:
        for genre in song["genres"]:
            by_genre.setdefault(genre, []).append(song)

    users = [{"user_id": int(r["user_id"]), "name": r["user_name"]} for r in read_csv("users.csv")]
    return songs, users, by_genre


# ---------------------------------------------------------------------------
# Event simulation
# ---------------------------------------------------------------------------
def pick_song(rng, user, songs, by_genre):
    # 70% of the time the user goes for one of their favorite genres
    if rng.random() < 0.7:
        return rng.choice(by_genre[rng.choice(user["favorites"])])
    return rng.choice(songs)


def listen(rng, user, song):
    """One listening session -> list of events (play or skip, and maybe a like)."""
    is_favorite = any(g in user["favorites"] for g in song["genres"])
    skip_prob = 0.15 if is_favorite else 0.55
    now_ms = int(time.time() * 1000)
    device = rng.choice(DEVICES)

    base = {"user_id": user["user_id"], "song_id": song["song_id"], "device": device}
    events = []
    if rng.random() < skip_prob:
        events.append({**base, "event_type": "skip", "ms_played": rng.randint(2_000, SKIP_LIMIT_MS - 1)})
    else:
        # most plays go to the end, some stop halfway (still counts as a play)
        played = song["duration_ms"] if rng.random() < 0.7 else rng.randint(SKIP_LIMIT_MS, song["duration_ms"])
        events.append({**base, "event_type": "play", "ms_played": played})
        like_prob = 0.30 if is_favorite else 0.05
        if rng.random() < like_prob:
            events.append({**base, "event_type": "like", "ms_played": 0})

    for e in events:
        e["event_id"] = str(uuid.uuid4())
        e["event_ts_ms"] = now_ms
    return events


def make_late(rng, event):
    late = dict(event)
    late["event_ts_ms"] -= rng.randint(10_000, 90_000)
    return late


def make_dirty(rng, event):
    """Breaks an event in one of a few realistic ways."""
    dirty = dict(event)
    kind = rng.choice(["no_user", "bad_song", "negative_ms", "bad_type"])
    if kind == "no_user":
        dirty["user_id"] = None
    elif kind == "bad_song":
        dirty["song_id"] = 99999           # not in the catalog
    elif kind == "negative_ms":
        dirty["ms_played"] = -rng.randint(1, 5000)
    else:
        dirty["event_type"] = "pause"      # not an event type we track
    return dirty, kind


def main():
    args = parse_args()
    rng = random.Random(args.seed)

    songs, users, by_genre = load_catalog()
    genres = sorted(by_genre)
    for user in users:
        user["favorites"] = rng.sample(genres, k=rng.choice([1, 2]))
    names = {u["user_id"]: u["name"] for u in users}
    song_by_id = {s["song_id"]: s for s in songs}

    producer = KafkaProducer(
        bootstrap_servers=args.broker,
        key_serializer=lambda k: str(k).encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",     # wait for the broker to confirm every message
        linger_ms=20,
    )

    print(f"Catalog: {len(songs)} songs, {len(users)} users, {len(genres)} genres")
    print(f"Sending to topic '{args.topic}' on {args.broker} (~{args.rate} events/s). Ctrl+C to stop.")
    print("-" * 70)

    sent = dirty_count = dup_count = late_count = 0
    counts = {"play": 0, "skip": 0, "like": 0}
    try:
        while not args.max_events or sent < args.max_events:
            user = rng.choice(users)
            song = pick_song(rng, user, songs, by_genre)

            for event in listen(rng, user, song):
                note = ""
                if rng.random() < args.late_rate:
                    event = make_late(rng, event)
                    late_count += 1
                    note = " (late)"
                if rng.random() < args.dirty_rate:
                    event, kind = make_dirty(rng, event)
                    dirty_count += 1
                    note = f" (DIRTY: {kind})"

                # key = user id, so all events of one user land in the same partition (in order)
                producer.send(args.topic, key=event["user_id"], value=event)
                sent += 1
                if event["event_type"] in counts:
                    counts[event["event_type"]] += 1

                # sometimes the app retries and the same event arrives twice
                if rng.random() < args.dirty_rate:
                    producer.send(args.topic, key=event["user_id"], value=event)
                    sent += 1
                    dup_count += 1
                    note += " (+duplicate)"

                if not args.quiet:
                    s = song_by_id[song["song_id"]]
                    print(f"[{sent:>5}] {names[user['user_id']]:<18} {event['event_type'].upper():<5} "
                          f"{s['title']} - {s['artist']} ({'/'.join(s['genres'])}){note}")

            time.sleep(rng.expovariate(args.rate))   # random gaps, like real traffic

    except KeyboardInterrupt:
        print("\nStopped by user")
    finally:
        producer.flush()
        producer.close()
        print("-" * 70)
        print(f"Sent {sent:,} messages | plays {counts['play']:,} | skips {counts['skip']:,} | "
              f"likes {counts['like']:,} | late {late_count:,} | dirty {dirty_count:,} | duplicates {dup_count:,}")


if __name__ == "__main__":
    main()
