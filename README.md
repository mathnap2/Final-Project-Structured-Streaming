# Final Project Part II — Music Streaming with Structured Streaming

Music streaming service: users playing, skipping and liking songs, by artist and genre.

```
producer.py ──► Kafka topic "music-events" ──► consumer.py (PySpark Structured Streaming) ──► Neo4j
 (simulated app)        (3 partitions)            3 queries, foreachBatch                 (neo4j-iteso)
                                                   + static catalog (csv)
```

| File | What it does |
|---|---|
| `build_catalog.py` | Creates `catalog/songs.csv` (5 songs per album) and `catalog/users.csv` (from the listeners in streams.csv). Already run, only needed again if you delete them |
| `catalog/` | music_stream dataset from Labs 04/06 (artists, albums, streams) + songs and users |
| `producer.py` | Simulates users playing / skipping / liking songs and sends each event as JSON to Kafka |
| `consumer.py` | Structured Streaming: cleaning, dedup with watermark, stream-static joins, tumbling and sliding windows, writes to Neo4j with foreachBatch |
| `spark_utils.py` | Copy of my `SparkUtils` class (session + `generate_schema`), so the repo works on its own |
| `validation.cypher` | Cypher queries that validate the sink |
| `neo4j.env.example` | Template for the Neo4j credentials (the real `neo4j.env` is in .gitignore) |

## Event message

```json
{"event_id": "b6a0eadb-...", "event_type": "play", "user_id": 13, "song_id": 11103,
 "ms_played": 272000, "device": "desktop", "event_ts_ms": 1794863154471}
```
`play` = listened 30 s or more, `skip` = stopped before 30 s, `like` = user liked the song.

## Graph schema

Nodes:
- `(:User {id, name, country, plan})`
- `(:Song {id, title, duration_ms, track_number})`, `(:Album {id, title, release_year, label})`
- `(:Artist {id, name, country, formed_year})`, `(:Genre {name})`
- `(:GenreWindow {id, genre, window_start, window_end, plays, skips, likes, listeners, minutes_listened, avg_completion_pct, skip_rate})`
- `(:ArtistTrend {id, artist, window_start, window_end, plays, skips, likes, listeners, trend_score})`

Relationships:
- catalog (batch, once): `(Song)-[:IN_ALBUM]->(Album)-[:BY]->(Artist)-[:PLAYS_GENRE]->(Genre)`
- stream: `(User)-[:LISTENED {plays, skips, ms_played, first_event_time, last_event_time, last_device, last_batch}]->(Song)`
- stream: `(User)-[:LIKED {liked_at}]->(Song)`
- stream: `(GenreWindow)-[:FOR_GENRE]->(Genre)`, `(ArtistTrend)-[:FOR_ARTIST]->(Artist)`

## How to run (Docker, course setup)

Check the container names first with `docker ps`. Below they are `spark-cluster-spark-notebook-1`, `kafka-kafka-1` and `neo4j-iteso`.

### 1. Start Kafka and create the topic
```bash
cd big_data_iteso/infra/kafka
docker compose up -d
docker exec -it kafka-kafka-1 kafka-topics.sh --create --topic music-events --partitions 3 --replication-factor 1 --bootstrap-server kafka:9093
```

### 2. Neo4j credentials
Copy `neo4j.env.example` to `neo4j.env` and put the real password in it. Make sure `neo4j-iteso` is running.

### 3. Consumer (terminal 1, from this folder)
```bash
docker exec -it --env-file neo4j.env spark-cluster-spark-notebook-1 python3 /opt/spark/work-dir/src/mathiasNapoles/final_project_part2/consumer.py --fresh
```
Wait for `Streaming started: ...`. `--fresh` wipes the checkpoints and the streamed data in Neo4j (use it for the demo). Without Neo4j: add `--sink console`.

### 4. Producer (terminal 2)
```bash
docker exec -it spark-cluster-spark-notebook-1 python3 /opt/spark/work-dir/src/mathiasNapoles/final_project_part2/producer.py --rate 5
```

### 5. Validate
Open Neo4j Browser (http://localhost:7474) and run the queries in `validation.cypher`.
