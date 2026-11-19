"""
Final Project Part II - Structured Streaming consumer
Kafka topic "music-events" -> PySpark Structured Streaming -> Neo4j

Three streaming queries:
  1. listening_graph   (User)-[:LISTENED]->(Song) and (User)-[:LIKED]->(Song)
  2. genre_windows     1-minute tumbling windows per genre
  3. trending_artists  5-minute windows sliding every minute, per artist

The Neo4j password comes from env vars (NEO4J_URL, NEO4J_USER, NEO4J_PASSWORD).

Run it inside the spark-notebook container:
    python3 consumer.py --fresh     (--fresh = start from zero)
"""
import os
import shutil
import sys

from spark_utils import SparkUtils   # first: it runs findspark, so pyspark can be imported
from pyspark.sql import functions as F
HERE = os.path.dirname(os.path.abspath(__file__))
CATALOG = os.path.join(HERE, "catalog")
CHECKPOINT = "/opt/spark/work-dir/checkpoints/music_fp2"
PACKAGES = ("org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.3,"
            "org.neo4j:neo4j-connector-apache-spark_2.13:5.3.10_for_spark_3")

# Neo4j connection, the password is NOT in the code
opts = {
    "url": os.environ.get("NEO4J_URL", "bolt://neo4j-iteso:7687"),
    "authentication.type": "basic",
    "authentication.basic.username": os.environ.get("NEO4J_USER", "neo4j"),
    "authentication.basic.password": os.environ["NEO4J_PASSWORD"],
}

spark = SparkUtils("music-streaming-fp2", "local[*]", spark_packages=PACKAGES, shuffle_partitions=4).spark
spark.sparkContext.setLogLevel("WARN")


def write_cypher(df, cypher, opts):
    """Saves a DataFrame in Neo4j with a Cypher query (each row is called 'event')."""
    (df.coalesce(1).write
       .format("org.neo4j.spark.DataSource")
       .mode("Overwrite")
       .option("query", cypher)
       .options(**opts)
       .save())


# ---------------------------------------------------------------------------
# 1. Catalog (static): artists, albums, songs and users
# ---------------------------------------------------------------------------
def read_csv(name):
    return spark.read.option("header", True).option("escape", '"').csv(f"{CATALOG}/{name}")

artist_schema = SparkUtils.generate_schema([
    ("artist_name", "string"), ("country", "string"),
    ("genres", "array_string"), ("formed_year", "int"),
])
album_schema = SparkUtils.generate_schema([
    ("title", "string"), ("release_year", "int"),
    ("label", "struct", [("label_name", "string")]),
])

artists = (read_csv("artists.csv")
           .withColumn("info", F.from_json("artist_info", artist_schema))
           .select(F.col("artist_id").cast("int"),
                   F.col("info.artist_name").alias("artist_name"),
                   F.col("info.country").alias("artist_country"),
                   F.col("info.genres").alias("genres"),
                   F.col("info.formed_year").alias("formed_year")))

albums = (read_csv("albums.csv")
          .withColumn("info", F.from_json("album_info", album_schema))
          .select(F.col("album_id").cast("int"), F.col("artist_id").cast("int"),
                  F.col("info.title").alias("album_title"),
                  F.col("info.release_year").alias("release_year"),
                  F.col("info.label.label_name").alias("label")))

songs = read_csv("songs.csv").select(
    F.col("song_id").cast("int"), F.col("album_id").cast("int"),
    F.col("track_number").cast("int"), F.col("title").alias("song_title"),
    F.col("duration_ms").cast("long"))

users = read_csv("users.csv").select(
    F.col("user_id").cast("int"), "user_name",
    F.col("country").alias("user_country"), "plan")

# one row per song with its album, artist and genres
song_dim = (songs.join(albums, "album_id").join(artists, "artist_id")
            .select("song_id", "song_title", "duration_ms", "artist_id", "artist_name", "genres"))

# --fresh: delete the checkpoints and what the streams saved before, so we start from zero
if "--fresh" in sys.argv:
    shutil.rmtree(CHECKPOINT, ignore_errors=True)
    one_row = spark.range(1)
    write_cypher(one_row, "MATCH ()-[r:LISTENED|LIKED]->() DELETE r", opts)
    write_cypher(one_row, "MATCH (w:GenreWindow) DETACH DELETE w", opts)
    write_cypher(one_row, "MATCH (t:ArtistTrend) DETACH DELETE t", opts)

# save the catalog in Neo4j only once (artists -> albums -> songs -> users)
write_cypher(artists, """
    MERGE (a:Artist {id: event.artist_id})
    SET a.name = event.artist_name, a.country = event.artist_country, a.formed_year = event.formed_year
    WITH a, event UNWIND event.genres AS genre_name
    MERGE (g:Genre {name: genre_name})
    MERGE (a)-[:PLAYS_GENRE]->(g)""", opts)

write_cypher(albums, """
    MATCH (ar:Artist {id: event.artist_id})
    MERGE (al:Album {id: event.album_id})
    SET al.title = event.album_title, al.release_year = event.release_year, al.label = event.label
    MERGE (al)-[:BY]->(ar)""", opts)

write_cypher(songs, """
    MATCH (al:Album {id: event.album_id})
    MERGE (s:Song {id: event.song_id})
    SET s.title = event.song_title, s.duration_ms = event.duration_ms, s.track_number = event.track_number
    MERGE (s)-[:IN_ALBUM]->(al)""", opts)

write_cypher(users, """
    MERGE (u:User {id: event.user_id})
    SET u.name = event.user_name, u.country = event.user_country, u.plan = event.plan""", opts)

print("Catalog written to Neo4j: 10 artists, 20 albums, 100 songs, 52 users")


# ---------------------------------------------------------------------------
# 2. Read from Kafka
# ---------------------------------------------------------------------------
EVENT_SCHEMA = SparkUtils.generate_schema([
    ("event_id", "string"), ("event_type", "string"), ("user_id", "int"),
    ("song_id", "int"), ("ms_played", "long"), ("device", "string"), ("event_ts_ms", "long"),
])

raw = (spark.readStream.format("kafka")
       .option("kafka.bootstrap.servers", "kafka:9093")
       .option("subscribe", "music-events")
       .option("startingOffsets", "latest")
       .option("maxOffsetsPerTrigger", 2000)
       .load())
events = (raw.select(F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("e"))
              .select("e.*")
              .withColumn("event_time", F.timestamp_millis("event_ts_ms")))


# ---------------------------------------------------------------------------
# 3. Clean: bad messages, late messages (watermark) and repeated messages
# ---------------------------------------------------------------------------
clean = (events
         .filter(F.col("event_id").isNotNull() & F.col("user_id").isNotNull()
                 & F.col("song_id").isNotNull() & F.col("event_time").isNotNull())
         .filter(F.col("event_type").isin("play", "skip", "like"))
         .filter(F.col("ms_played") >= 0)
         .withWatermark("event_time", "2 minutes")
         .dropDuplicatesWithinWatermark(["event_id"]))


# ---------------------------------------------------------------------------
# 4. Add the names (joins with the catalog) + 1/0 columns to add up later
# ---------------------------------------------------------------------------
enriched = (clean.join(F.broadcast(song_dim), "song_id", "inner")
                 .join(F.broadcast(users), "user_id", "inner")
                 .withColumn("is_play", (F.col("event_type") == "play").cast("int"))
                 .withColumn("is_skip", (F.col("event_type") == "skip").cast("int"))
                 .withColumn("is_like", (F.col("event_type") == "like").cast("int"))
                 .withColumn("completion_pct",
                             F.least(F.col("ms_played") / F.col("duration_ms"), F.lit(1.0)) * 100))


# ---------------------------------------------------------------------------
# 5. Windows
# ---------------------------------------------------------------------------
genre_windows = (enriched.withColumn("genre", F.explode("genres"))
    .groupBy(F.window("event_time", "1 minute"), "genre")
    .agg(F.sum("is_play").alias("plays"), F.sum("is_skip").alias("skips"),
         F.sum("is_like").alias("likes"),
         F.approx_count_distinct("user_id").alias("listeners"),
         F.round(F.sum("ms_played") / 60000, 2).alias("minutes_listened"),
         F.round(F.avg(F.when(F.col("is_play") == 1, F.col("completion_pct"))), 1).alias("avg_completion_pct"))
    .withColumn("skip_rate", F.round(F.col("skips") / F.greatest(F.col("plays") + F.col("skips"), F.lit(1)) * 100, 1))
    .withColumn("window_start", F.col("window.start"))
    .withColumn("window_end", F.col("window.end"))
    .drop("window"))

trending = (enriched
    .groupBy(F.window("event_time", "5 minutes", "1 minute"), "artist_id", "artist_name")
    .agg(F.sum("is_play").alias("plays"), F.sum("is_skip").alias("skips"),
         F.sum("is_like").alias("likes"),
         F.approx_count_distinct("user_id").alias("listeners"))
    .withColumn("trend_score", F.col("plays") + 3 * F.col("likes") - F.col("skips"))
    .withColumn("window_start", F.col("window.start"))
    .withColumn("window_end", F.col("window.end"))
    .drop("window"))


# ---------------------------------------------------------------------------
# 6. foreachBatch functions: what to do with every micro-batch
# ---------------------------------------------------------------------------
def listening_sink(batch_df, batch_id):
    # one row per (user, song) in this micro-batch
    pairs = (batch_df.groupBy("user_id", "song_id")
             .agg(F.sum("is_play").alias("plays"), F.sum("is_skip").alias("skips"),
                  F.sum("is_like").alias("likes"), F.sum("ms_played").alias("ms_played"),
                  F.min("event_time").alias("first_event_time"),
                  F.max("event_time").alias("last_event_time"),
                  F.max_by("device", "event_time").alias("last_device"))
             .withColumn("batch_id", F.lit(batch_id)))
    if pairs.isEmpty():
        return
    # last_batch: if Spark runs the same batch again, the numbers are not added two times
    write_cypher(pairs, """
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
            MERGE (u)-[l:LIKED]->(s) ON CREATE SET l.liked_at = event.last_event_time)""", opts)
    t = pairs.agg(F.sum("plays"), F.sum("skips"), F.sum("likes")).first()
    print(f"[listening_graph] batch {batch_id}: {t[0]} plays, {t[1]} skips, {t[2]} likes")


def genre_sink(batch_df, batch_id):
    if batch_df.isEmpty():
        return
    rows = batch_df.withColumn("id", F.concat_ws("|", "genre", F.date_format("window_start", "yyyy-MM-dd'T'HH:mm")))
    write_cypher(rows, """
        MATCH (g:Genre {name: event.genre})
        MERGE (w:GenreWindow {id: event.id})
        SET w.genre = event.genre, w.window_start = event.window_start, w.window_end = event.window_end,
            w.plays = event.plays, w.skips = event.skips, w.likes = event.likes,
            w.listeners = event.listeners, w.minutes_listened = event.minutes_listened,
            w.avg_completion_pct = event.avg_completion_pct, w.skip_rate = event.skip_rate
        MERGE (w)-[:FOR_GENRE]->(g)""", opts)
    print(f"[genre_windows] batch {batch_id}:")
    batch_df.orderBy(F.desc("window_start"), F.desc("plays")).drop("window_end").show(5, truncate=False)


def trending_sink(batch_df, batch_id):
    if batch_df.isEmpty():
        return
    rows = batch_df.withColumn("id", F.concat_ws("|", F.col("artist_id").cast("string"),
                                                 F.date_format("window_start", "yyyy-MM-dd'T'HH:mm")))
    write_cypher(rows, """
        MATCH (a:Artist {id: event.artist_id})
        MERGE (t:ArtistTrend {id: event.id})
        SET t.artist = event.artist_name, t.window_start = event.window_start, t.window_end = event.window_end,
            t.plays = event.plays, t.skips = event.skips, t.likes = event.likes,
            t.listeners = event.listeners, t.trend_score = event.trend_score
        MERGE (t)-[:FOR_ARTIST]->(a)""", opts)
    print(f"[trending_artists] batch {batch_id}:")
    batch_df.orderBy(F.desc("window_start"), F.desc("trend_score")).drop("window_end").show(5, truncate=False)


# ---------------------------------------------------------------------------
# 7. Start the 3 queries (every 10 seconds)
# ---------------------------------------------------------------------------
query = (enriched.writeStream.queryName("listening_graph")
         .outputMode("append").foreachBatch(listening_sink)
         .option("checkpointLocation", f"{CHECKPOINT}/listening_graph")
         .trigger(processingTime="10 seconds").start())

(genre_windows.writeStream.queryName("genre_windows")
 .outputMode("update").foreachBatch(genre_sink)
 .option("checkpointLocation", f"{CHECKPOINT}/genre_windows")
 .trigger(processingTime="10 seconds").start())

(trending.writeStream.queryName("trending_artists")
 .outputMode("update").foreachBatch(trending_sink)
 .option("checkpointLocation", f"{CHECKPOINT}/trending_artists")
 .trigger(processingTime="10 seconds").start())

print("Streaming started: listening_graph, genre_windows, trending_artists. Ctrl+C to stop.")
spark.streams.awaitAnyTermination()
