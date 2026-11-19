"""
Final Project Part II - catalog builder (run once, before the producer)

The music_stream dataset from Labs 04/06 has artists, albums and listeners,
but no individual songs. This script fills that gap:
  - songs.csv: 5 made-up tracks per album, with a duration
  - users.csv: one user per listener in streams.csv (name, country, plan)

It's seeded, so running it again always gives the exact same catalog.
Both the producer and the Spark consumer read these CSVs, so ids always match.

    python build_catalog.py
"""
import csv
import json
import os
import random

HERE = os.path.dirname(os.path.abspath(__file__))
CATALOG = os.path.join(HERE, "catalog")

TRACKS_PER_ALBUM = 5

# word lists to make song titles that sound kind of real
WORDS_A = ["Midnight", "Electric", "Golden", "Broken", "Silent", "Burning", "Paper",
           "Velvet", "Neon", "Lonely", "Crystal", "Wild", "Distant", "Summer", "Hollow"]
WORDS_B = ["Heart", "City", "Rain", "Echo", "Signal", "Shadows", "Waves", "Fire",
           "Dreams", "Lights", "Road", "Skin", "Sky", "Memories", "Pulse"]


def build_songs(rng):
    songs = []
    with open(os.path.join(CATALOG, "albums.csv"), newline="", encoding="utf-8") as f:
        for album in csv.DictReader(f):
            album_id = int(album["album_id"])
            for n in range(1, TRACKS_PER_ALBUM + 1):
                songs.append({
                    "song_id": album_id * 100 + n,          # 10101, 10102, ...
                    "album_id": album_id,
                    "track_number": n,
                    "title": f"{rng.choice(WORDS_A)} {rng.choice(WORDS_B)}",
                    "duration_ms": rng.randint(150, 300) * 1000,  # 2:30 to 5:00
                })
    return songs


def build_users():
    users = []
    with open(os.path.join(CATALOG, "streams.csv"), newline="", encoding="utf-8") as f:
        for i, row in enumerate(csv.DictReader(f), start=1):
            info = json.loads(row["listener_info"])
            users.append({
                "user_id": i,
                "user_name": info["listener_name"],
                "country": info["country"],
                "plan": info["subscription"]["plan"],
            })
    return users


def write_csv(path, rows):
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def main():
    rng = random.Random(2026)
    songs = build_songs(rng)
    users = build_users()
    write_csv(os.path.join(CATALOG, "songs.csv"), songs)
    write_csv(os.path.join(CATALOG, "users.csv"), users)
    print(f"songs.csv: {len(songs)} songs | users.csv: {len(users)} users")


if __name__ == "__main__":
    main()
