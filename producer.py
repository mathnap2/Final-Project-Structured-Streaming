"""
Final Project Part II - Kafka producer
Simulates a music app: users play, skip and like songs.
Every event is sent as one JSON message to the Kafka topic "music-events".

Run it inside the spark-notebook container:
    python3 producer.py --rate 5
"""
import argparse
import csv
import json
import os
import random
import time
import uuid

from kafka import KafkaProducer

BROKER = "kafka:9093"
TOPIC = "music-events"
CATALOG = os.path.join(os.path.dirname(os.path.abspath(__file__)), "catalog")

parser = argparse.ArgumentParser()
parser.add_argument("--rate", type=float, default=5, help="events per second (more or less)")
args = parser.parse_args()


# ---------------------------------------------------------------------------
# 1. Read the catalog: every song gets its artist and genres
# ---------------------------------------------------------------------------
def read_csv(name):
    with open(f"{CATALOG}/{name}", encoding="utf-8") as f:
        return list(csv.DictReader(f))

artists = {}
for row in read_csv("artists.csv"):
    info = json.loads(row["artist_info"])
    artists[row["artist_id"]] = {"name": info["artist_name"], "genres": info["genres"]}

album_artist = {row["album_id"]: row["artist_id"] for row in read_csv("albums.csv")}

songs = []
for row in read_csv("songs.csv"):
    artist = artists[album_artist[row["album_id"]]]
    songs.append({
        "song_id": int(row["song_id"]),
        "title": row["title"],
        "duration_ms": int(row["duration_ms"]),
        "artist": artist["name"],
        "genres": artist["genres"],
    })

users = [{"user_id": int(r["user_id"]), "name": r["user_name"]} for r in read_csv("users.csv")]

# every user gets 1 or 2 favorite genres
all_genres = sorted({g for s in songs for g in s["genres"]})
for user in users:
    user["favorites"] = random.sample(all_genres, k=random.choice([1, 2]))


# ---------------------------------------------------------------------------
# 2. Kafka producer: key = user_id, value = JSON
# ---------------------------------------------------------------------------
producer = KafkaProducer(
    bootstrap_servers=BROKER,
    key_serializer=lambda k: str(k).encode("utf-8"),
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    acks="all",   # wait until Kafka confirms every message
)

print(f"Sending events to '{TOPIC}' (~{args.rate} per second). Ctrl+C to stop.")
sent = 0

try:
    while True:
        # --- 3. pick a user and a song (70% from a favorite genre) ---
        user = random.choice(users)
        if random.random() < 0.7:
            genre = random.choice(user["favorites"])
            song = random.choice([s for s in songs if genre in s["genres"]])
        else:
            song = random.choice(songs)
        is_favorite = any(g in user["favorites"] for g in song["genres"])

        # --- 4. decide what happened: skip, or play (+ maybe like) ---
        events = []
        if random.random() < (0.15 if is_favorite else 0.55):
            events.append({"event_type": "skip", "ms_played": random.randint(2000, 29999)})
        else:
            if random.random() < 0.7:
                played = song["duration_ms"]                              # listened to the end
            else:
                played = random.randint(30000, song["duration_ms"])      # stopped halfway
            events.append({"event_type": "play", "ms_played": played})
            if random.random() < (0.30 if is_favorite else 0.05):
                events.append({"event_type": "like", "ms_played": 0})

        # --- 5. send every event ---
        for e in events:
            message = {
                "event_id": str(uuid.uuid4()),
                "event_type": e["event_type"],
                "user_id": user["user_id"],
                "song_id": song["song_id"],
                "ms_played": e["ms_played"],
                "device": random.choice(["mobile", "mobile", "mobile", "desktop", "web", "smart_speaker"]),
                "event_ts_ms": int(time.time() * 1000),
            }
            note = ""

            # 10% late: the time is 10 to 90 seconds in the past
            if random.random() < 0.10:
                message["event_ts_ms"] -= random.randint(10000, 90000)
                note = " (late)"

            # 3% dirty on purpose, so the consumer has something to clean
            if random.random() < 0.03:
                kind = random.choice(["no_user", "bad_song", "negative_ms", "bad_type"])
                if kind == "no_user":
                    message["user_id"] = None
                elif kind == "bad_song":
                    message["song_id"] = 99999
                elif kind == "negative_ms":
                    message["ms_played"] = -random.randint(1, 5000)
                else:
                    message["event_type"] = "pause"
                note = f" (DIRTY: {kind})"

            producer.send(TOPIC, key=message["user_id"], value=message)
            sent += 1

            # 3% sent two times, like an app that retries
            if random.random() < 0.03:
                producer.send(TOPIC, key=message["user_id"], value=message)
                sent += 1
                note += " (+duplicate)"

            print(f"[{sent:>5}] {user['name']:<18} {message['event_type'].upper():<5} "
                  f"{song['title']} - {song['artist']} ({'/'.join(song['genres'])}){note}")

        # random wait between listens, about <rate> events per second
        time.sleep(random.expovariate(args.rate))

except KeyboardInterrupt:
    print("Stopped")
finally:
    producer.flush()
    producer.close()
    print(f"Total messages sent: {sent}")
