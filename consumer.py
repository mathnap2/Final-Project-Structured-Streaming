"""
Final Project Part II - Structured Streaming consumer
Kafka topic "music-events" -> PySpark Structured Streaming -> Neo4j

Three streaming queries run at the same time on the same Kafka source:

  1. listening_graph    every micro-batch updates (User)-[:LISTENED]->(Song) counters
                        (plays, skips, ms played) and adds (User)-[:LIKED]->(Song)
  2. genre_windows      1-minute tumbling windows per genre: plays, skips, likes,
                        skip rate, unique listeners, minutes listened
  3. trending_artists   5-minute windows sliding every minute per artist, with a
                        trend score = plays + 3*likes - skips

Before the streams start, the static catalog (genres, artists, albums, songs, users)
is written once as a batch job, so the streams only have to add relationships.

Neo4j credentials come from env vars (NEO4J_URL, NEO4J_USER, NEO4J_PASSWORD),
never from the code.

Run it inside the spark-notebook container:
    python3 consumer.py                 # normal run
    python3 consumer.py --fresh         # wipe checkpoints + streamed data, start over
    python3 consumer.py --sink console  # no Neo4j, just print every micro-batch
"""
import argparse
import os
import shutil
import sys

# same trick as in the labs: make src/ importable so we can use our SparkUtils
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..")))

from mathiasNapoles.spark_utils import SparkUtils
from pyspark.sql import functions as F

HERE = os.path.dirname(os.path.abspath(__file__))

KAFKA_CONNECTOR = "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.3"
NEO4J_CONNECTOR = "org.neo4j:neo4j-connector-apache-spark_2.13:5.3.10_for_spark_3"

VALID_EVENTS = ["play", "skip", "like"]

# schema of the JSON messages the producer sends
EVENT_SCHEMA = SparkUtils.generate_schema([
    ("event_id", "string"),
    ("event_type", "string"),
    ("user_id", "int"),
    ("song_id", "int"),
    ("ms_played", "long"),
    ("device", "string"),
    ("event_ts_ms", "long"),
])


def parse_args():
    parser = argparse.ArgumentParser(description="Music streaming consumer")
    parser.add_argument("--bootstrap", default="kafka:9093", help="Kafka bootstrap server (inside Docker)")
    parser.add_argument("--topic", default="music-events")
    parser.add_argument("--starting-offsets", default="latest", choices=["latest", "earliest"])
    parser.add_argument("--max-offsets", type=int, default=2000, help="max Kafka messages per micro-batch")
    parser.add_argument("--trigger", default="10 seconds", help="micro-batch interval")
    parser.add_argument("--watermark", default="2 minutes", help="how late an event can arrive")
    parser.add_argument("--checkpoint", default="/opt/spark/work-dir/checkpoints/music_fp2")
    parser.add_argument("--catalog", default=os.path.join(HERE, "catalog"), help="folder with the catalog csvs")
    parser.add_argument("--sink", default="neo4j", choices=["neo4j", "console"])
    parser.add_argument("--fresh", action="store_true",
                        help="delete checkpoints AND the streamed data in Neo4j, then start from zero")
    parser.add_argument("--test-dir", default=None,
                        help="(testing only) read JSON files from this folder instead of Kafka")
    return parser.parse_args()


# ---------------------------------------------------------------------------
# Neo4j helpers
# ---------------------------------------------------------------------------
def neo4j_options():
    """Connection options from env vars, so no password ends up in GitHub."""
    password = os.environ.get("NEO4J_PASSWORD")
    if not password:
        raise RuntimeError("NEO4J_PASSWORD is not set. Run with: docker exec -e NEO4J_PASSWORD=... (see README)")
    return {
        "url": os.environ.get("NEO4J_URL", "bolt://neo4j-iteso:7687"),
        "authentication.type": "basic",
        "authentication.basic.username": os.environ.get("NEO4J_USER", "neo4j"),
        "authentication.basic.password": password,
    }


def write_cypher(df, cypher, opts, script=None):
    """
    Writes a (static) DataFrame to Neo4j with a Cypher query.
    The connector wraps the query in "UNWIND $events AS event", so each row is "event".
    coalesce(1) = a single writer, so two tasks never MERGE the same node at the
    same time (that can deadlock). Our micro-batches are small, so it's fine.
    """
    writer = (
        df.coalesce(1).write
        .format("org.neo4j.spark.DataSource")
        .mode("Overwrite")
        .option("query", cypher)
    )
    for key, value in opts.items():
        writer = writer.option(key, value)
    if script:
        writer = writer.option("script", script)
    writer.save()


# ---------------------------------------------------------------------------
# Static catalog (batch)
# ---------------------------------------------------------------------------
def load_catalog(spark, folder):
    """
    Reads the music_stream csvs (Labs 04/06) + songs/users and returns one flat
    DataFrame per entity. artist_info and album_info are JSON strings, so we
    parse them with from_json like in Lab 06.
    """
    artist_info = SparkUtils.generate_schema([
        ("artist_name", "string"), ("country", "string"),
        ("genres", "array_string"), ("formed_year", "int"),
    ])
    album_info = SparkUtils.generate_schema([
        ("title", "string"), ("release_year", "int"),
        ("label", "struct", [("label_name", "string"), ("country", "string")]),
    ])

    def read(name):
        return spark.read.option("header", True).option("escape", '"').csv(os.path.join(folder, name))

    artists = (
        read("artists.csv")
        .withColumn("info", F.from_json("artist_info", artist_info))
        .select(
            F.col("artist_id").cast("int"),
            F.col("info.artist_name").alias("artist_name"),
            F.col("info.country").alias("artist_country"),
            F.col("info.genres").alias("genres"),
            F.col("info.formed_year").alias("formed_year"),
        )
    )
    albums = (
        read("albums.csv")
        .withColumn("info", F.from_json("album_info", album_info))
        .select(
            F.col("album_id").cast("int"),
            F.col("artist_id").cast("int"),
            F.col("info.title").alias("album_title"),
            F.col("info.release_year").alias("release_year"),
            F.col("info.label.label_name").alias("label"),
        )
    )
    songs = read("songs.csv").select(
        F.col("song_id").cast("int"), F.col("album_id").cast("int"),
        F.col("track_number").cast("int"), F.col("title").alias("song_title"),
        F.col("duration_ms").cast("long"),
    )
    users = read("users.csv").select(
        F.col("user_id").cast("int"), "user_name",
        F.col("country").alias("user_country"), "plan",
    )

    # one wide row per song: song + album + artist (+ genres array)
    song_dim = (
        songs.join(albums, "album_id")
        .join(artists, "artist_id")
        .select("song_id", "song_title", "duration_ms", "track_number",
                "album_id", "album_title", "artist_id", "artist_name", "genres")
    )
    return {"artists": artists, "albums": albums, "songs": songs, "users": users, "song_dim": song_dim}


CONSTRAINTS = ";".join([
    "CREATE CONSTRAINT user_id IF NOT EXISTS FOR (u:User) REQUIRE u.id IS UNIQUE",
    "CREATE CONSTRAINT song_id IF NOT EXISTS FOR (s:Song) REQUIRE s.id IS UNIQUE",
    "CREATE CONSTRAINT album_id IF NOT EXISTS FOR (a:Album) REQUIRE a.id IS UNIQUE",
    "CREATE CONSTRAINT artist_id IF NOT EXISTS FOR (a:Artist) REQUIRE a.id IS UNIQUE",
    "CREATE CONSTRAINT genre_name IF NOT EXISTS FOR (g:Genre) REQUIRE g.name IS UNIQUE",
    "CREATE CONSTRAINT genre_window_id IF NOT EXISTS FOR (w:GenreWindow) REQUIRE w.id IS UNIQUE",
    "CREATE CONSTRAINT artist_trend_id IF NOT EXISTS FOR (t:ArtistTrend) REQUIRE t.id IS UNIQUE",
])

# used with --fresh: the checkpoint and the streamed part of the graph go together,
# so if we throw away the checkpoint we also throw away what the streams wrote
WIPE_STREAMED_DATA = ";".join([
    "MATCH ()-[r:LISTENED|LIKED]->() DELETE r",
    "MATCH (w:GenreWindow) DETACH DELETE w",
    "MATCH (t:ArtistTrend) DETACH DELETE t",
])

ARTISTS_CYPHER = """
MERGE (a:Artist {id: event.artist_id})
SET a.name = event.artist_name, a.country = event.artist_country, a.formed_year = event.formed_year
WITH a, event
UNWIND event.genres AS genre_name
MERGE (g:Genre {name: genre_name})
MERGE (a)-[:PLAYS_GENRE]->(g)
"""

ALBUMS_CYPHER = """
MATCH (ar:Artist {id: event.artist_id})
MERGE (al:Album {id: event.album_id})
SET al.title = event.album_title, al.release_year = event.release_year, al.label = event.label
MERGE (al)-[:BY]->(ar)
"""

SONGS_CYPHER = """
MATCH (al:Album {id: event.album_id})
MERGE (s:Song {id: event.song_id})
SET s.title = event.song_title, s.duration_ms = event.duration_ms, s.track_number = event.track_number
MERGE (s)-[:IN_ALBUM]->(al)
"""

USERS_CYPHER = """
MERGE (u:User {id: event.user_id})
SET u.name = event.user_name, u.country = event.user_country, u.plan = event.plan
"""


def write_catalog(spark, catalog, opts, fresh):
    script = (WIPE_STREAMED_DATA + ";" + CONSTRAINTS) if fresh else CONSTRAINTS
    # order matters: artists -> albums -> songs, because each one MATCHes the previous
    write_cypher(catalog["artists"], ARTISTS_CYPHER, opts, script=script)
    write_cypher(catalog["albums"], ALBUMS_CYPHER, opts)
    write_cypher(catalog["songs"], SONGS_CYPHER, opts)
    write_cypher(catalog["users"], USERS_CYPHER, opts)
    print(f"Catalog written to Neo4j: {catalog['artists'].count()} artists, {catalog['albums'].count()} albums, "
          f"{catalog['songs'].count()} songs, {catalog['users'].count()} users")


# ---------------------------------------------------------------------------
# Streaming transformations (all lazy)
# ---------------------------------------------------------------------------
def read_events(spark, args):
    """Source: Kafka (or a folder of JSON files when testing) -> one column per field."""
    if args.test_dir:
        raw = (spark.readStream.schema(EVENT_SCHEMA).option("maxFilesPerTrigger", 1).json(args.test_dir)
               .select(F.to_json(F.struct("*")).alias("json"), F.current_timestamp().alias("kafka_ts")))
    else:
        raw = (
            spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", args.bootstrap)
            .option("subscribe", args.topic)
            .option("startingOffsets", args.starting_offsets)
            .option("maxOffsetsPerTrigger", args.max_offsets)
            .option("failOnDataLoss", "false")
            .load()
            .select(F.col("value").cast("string").alias("json"), F.col("timestamp").alias("kafka_ts"))
        )

    return (
        # a broken JSON just turns into nulls, and the cleaning step drops it
        raw.select(F.from_json("json", EVENT_SCHEMA).alias("e"), "kafka_ts")
        .select("e.*", "kafka_ts")
        .withColumn("event_time", F.timestamp_millis("event_ts_ms"))
    )


def clean_events(df, watermark):
    """
    Data cleaning:
      - drop events with missing ids or timestamp, unknown event types or negative ms
      - watermark on event_time: up to <watermark> late is fine, older gets dropped
        (and the state for old windows / old ids gets cleaned up)
      - dropDuplicatesWithinWatermark on event_id: the producer sometimes sends the
        same event twice, this keeps only the first one
    """
    return (
        df
        .filter(F.col("event_id").isNotNull()
                & F.col("user_id").isNotNull()
                & F.col("song_id").isNotNull()
                & F.col("event_time").isNotNull())
        .withColumn("event_type", F.lower(F.trim("event_type")))
        .filter(F.col("event_type").isin(VALID_EVENTS))
        .filter(F.col("ms_played") >= 0)
        .withWatermark("event_time", watermark)
        .dropDuplicatesWithinWatermark(["event_id"])
    )


def enrich_events(df, catalog):
    """
    Stream-static joins with the catalog + derived columns.
    Inner join with songs drops events for songs that don't exist (song_id 99999).
    """
    return (
        df.join(F.broadcast(catalog["song_dim"]), "song_id", "inner")
        .join(F.broadcast(catalog["users"]), "user_id", "inner")
        .withColumn("is_play", (F.col("event_type") == "play").cast("int"))
        .withColumn("is_skip", (F.col("event_type") == "skip").cast("int"))
        .withColumn("is_like", (F.col("event_type") == "like").cast("int"))
        .withColumn("listen_seconds", F.round(F.col("ms_played") / 1000, 1))
        .withColumn("completion_pct",
                    F.round(F.least(F.col("ms_played") / F.col("duration_ms"), F.lit(1.0)) * 100, 1))
        .withColumn("ingest_delay_s",
                    F.col("kafka_ts").cast("double") - F.col("event_time").cast("double"))
    )


def genre_windows(df):
    """1-minute tumbling windows per genre. explode() because an artist has 2 genres."""
    return (
        df.withColumn("genre", F.explode("genres"))
        .groupBy(F.window("event_time", "1 minute"), "genre")
        .agg(
            F.sum("is_play").alias("plays"),
            F.sum("is_skip").alias("skips"),
            F.sum("is_like").alias("likes"),
            F.approx_count_distinct("user_id").alias("listeners"),
            F.round(F.sum("ms_played") / 60000, 2).alias("minutes_listened"),
            F.round(F.avg(F.when(F.col("is_play") == 1, F.col("completion_pct"))), 1).alias("avg_completion_pct"),
        )
        .withColumn("skip_rate",
                    F.round(F.col("skips") / F.greatest(F.col("plays") + F.col("skips"), F.lit(1)) * 100, 1))
        .withColumn("window_start", F.col("window.start"))
        .withColumn("window_end", F.col("window.end"))
        .withColumn("id", F.concat_ws("|", "genre", F.date_format("window_start", "yyyy-MM-dd'T'HH:mm")))
        .drop("window")
    )


def trending_artists(df):
    """5-minute windows that slide every minute, per artist, with a simple trend score."""
    return (
        df.groupBy(F.window("event_time", "5 minutes", "1 minute"), "artist_id", "artist_name")
        .agg(
            F.sum("is_play").alias("plays"),
            F.sum("is_skip").alias("skips"),
            F.sum("is_like").alias("likes"),
            F.approx_count_distinct("user_id").alias("listeners"),
        )
        .withColumn("trend_score", F.col("plays") + 3 * F.col("likes") - F.col("skips"))
        .withColumn("window_start", F.col("window.start"))
        .withColumn("window_end", F.col("window.end"))
        .withColumn("id", F.concat_ws("|", F.col("artist_id").cast("string"),
                                      F.date_format("window_start", "yyyy-MM-dd'T'HH:mm")))
        .drop("window")
    )


# ---------------------------------------------------------------------------
# foreachBatch sinks
# ---------------------------------------------------------------------------
# One row per (user, song) per micro-batch. The "last_batch" check makes it
# idempotent: if Spark replays a batch after a crash, the counters are not added twice.
LISTENED_CYPHER = """
MATCH (u:User {id: event.user_id})
MATCH (s:Song {id: event.song_id})
MERGE (u)-[r:LISTENED]->(s)
  ON CREATE SET r.plays = 0, r.skips = 0, r.ms_played = 0, r.last_batch = -1,
                r.first_event_time = event.first_event_time
WITH u, s, r, event
WHERE r.last_batch < event.batch_id
SET r.plays = r.plays + event.plays,
    r.skips = r.skips + event.skips,
    r.ms_played = r.ms_played + event.ms_played,
    r.last_event_time = event.last_event_time,
    r.last_device = event.last_device,
    r.last_batch = event.batch_id
FOREACH (_ IN CASE WHEN event.likes > 0 THEN [1] ELSE [] END |
    MERGE (u)-[l:LIKED]->(s)
      ON CREATE SET l.liked_at = event.last_event_time)
"""

GENRE_WINDOW_CYPHER = """
MATCH (g:Genre {name: event.genre})
MERGE (w:GenreWindow {id: event.id})
SET w.genre = event.genre, w.window_start = event.window_start, w.window_end = event.window_end,
    w.plays = event.plays, w.skips = event.skips, w.likes = event.likes,
    w.listeners = event.listeners, w.minutes_listened = event.minutes_listened,
    w.avg_completion_pct = event.avg_completion_pct, w.skip_rate = event.skip_rate,
    w.last_batch = event.batch_id
MERGE (w)-[:FOR_GENRE]->(g)
"""

ARTIST_TREND_CYPHER = """
MATCH (a:Artist {id: event.artist_id})
MERGE (t:ArtistTrend {id: event.id})
SET t.artist = event.artist_name, t.window_start = event.window_start, t.window_end = event.window_end,
    t.plays = event.plays, t.skips = event.skips, t.likes = event.likes,
    t.listeners = event.listeners, t.trend_score = event.trend_score,
    t.last_batch = event.batch_id
MERGE (t)-[:FOR_ARTIST]->(a)
"""


def listening_sink(opts, use_neo4j):
    def sink(batch_df, batch_id):
        # inside foreachBatch the micro-batch is a normal (static) DataFrame
        per_pair = (
            batch_df.groupBy("user_id", "song_id")
            .agg(
                F.sum("is_play").alias("plays"),
                F.sum("is_skip").alias("skips"),
                F.sum("is_like").alias("likes"),
                F.sum("ms_played").alias("ms_played"),
                F.min("event_time").alias("first_event_time"),
                F.max("event_time").alias("last_event_time"),
                F.max_by("device", "event_time").alias("last_device"),
            )
            .withColumn("batch_id", F.lit(batch_id))
        ).persist()

        n_pairs = per_pair.count()
        if n_pairs > 0:
            totals = per_pair.agg(F.sum("plays"), F.sum("skips"), F.sum("likes")).first()
            if use_neo4j:
                write_cypher(per_pair, LISTENED_CYPHER, opts)
            print(f"[listening_graph] batch {batch_id}: {totals[0]} plays, {totals[1]} skips, "
                  f"{totals[2]} likes -> {n_pairs} LISTENED relationships updated")
            if not use_neo4j:
                per_pair.show(5, truncate=False)
        per_pair.unpersist()
    return sink


def window_sink(name, cypher, opts, use_neo4j, sort_col):
    def sink(batch_df, batch_id):
        rows = batch_df.withColumn("batch_id", F.lit(batch_id)).persist()
        n = rows.count()
        if n > 0:
            if use_neo4j:
                write_cypher(rows, cypher, opts)
            print(f"[{name}] batch {batch_id}: {n} windows upserted. Top 5 by {sort_col}:")
            (rows.orderBy(F.desc("window_start"), F.desc(sort_col))
                 .drop("id", "batch_id", "window_end")
                 .show(5, truncate=False))
        rows.unpersist()
    return sink


def start_query(df, name, mode, sink, args):
    return (
        df.writeStream
        .queryName(name)
        .outputMode(mode)
        .foreachBatch(sink)
        .option("checkpointLocation", os.path.join(args.checkpoint, name))
        .trigger(processingTime=args.trigger)
        .start()
    )


def main():
    args = parse_args()
    use_neo4j = args.sink == "neo4j"
    opts = neo4j_options() if use_neo4j else {}

    # only download the connectors we actually need
    packages = []
    if not args.test_dir:
        packages.append(KAFKA_CONNECTOR)
    if use_neo4j:
        packages.append(NEO4J_CONNECTOR)
    su = SparkUtils("music-streaming-fp2", "local[*]",
                    spark_packages=",".join(packages) or None,
                    shuffle_partitions=4)
    spark = su.spark
    spark.sparkContext.setLogLevel("WARN")

    if args.fresh and os.path.isdir(args.checkpoint):
        shutil.rmtree(args.checkpoint)
        print(f"Checkpoints deleted: {args.checkpoint}")

    catalog = load_catalog(spark, args.catalog)
    for df in catalog.values():
        df.cache()
    if use_neo4j:
        write_catalog(spark, catalog, opts, args.fresh)

    events = enrich_events(clean_events(read_events(spark, args), args.watermark), catalog)

    queries = [
        start_query(events, "listening_graph", "append", listening_sink(opts, use_neo4j), args),
        start_query(genre_windows(events), "genre_windows", "update",
                    window_sink("genre_windows", GENRE_WINDOW_CYPHER, opts, use_neo4j, "plays"), args),
        start_query(trending_artists(events), "trending_artists", "update",
                    window_sink("trending_artists", ARTIST_TREND_CYPHER, opts, use_neo4j, "trend_score"), args),
    ]
    print("Streaming started: " + ", ".join(q.name for q in queries) + ". Ctrl+C to stop.")

    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        print("Stopping queries...")
        for q in queries:
            q.stop()


if __name__ == "__main__":
    main()
