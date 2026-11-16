// Final Project Part II - Cypher queries to validate the Neo4j sink
// Run them in Neo4j Browser (http://localhost:7474) while producer + consumer are running.

// 1) Nodes per label (run it a few times: GenreWindow / ArtistTrend keep growing)
MATCH (n)
RETURN labels(n)[0] AS label, count(*) AS total
ORDER BY total DESC;

// 2) Relationships per type (LISTENED / LIKED come from the stream)
MATCH ()-[r]->()
RETURN type(r) AS relationship, count(*) AS total
ORDER BY total DESC;

// 3) Main validation query: plays, skips, likes and skip rate per genre, from the graph
MATCH (u:User)-[r:LISTENED]->(s:Song)-[:IN_ALBUM]->(:Album)-[:BY]->(a:Artist)-[:PLAYS_GENRE]->(g:Genre)
OPTIONAL MATCH (u)-[l:LIKED]->(s)
WITH g.name AS genre, sum(r.plays) AS plays, sum(r.skips) AS skips, count(l) AS likes,
     count(DISTINCT u) AS listeners
RETURN genre, plays, skips, likes, listeners,
       round(100.0 * skips / (plays + skips), 1) AS skip_rate_pct
ORDER BY plays DESC;

// 4) Top 10 songs by plays
MATCH (u:User)-[r:LISTENED]->(s:Song)-[:IN_ALBUM]->(al:Album)-[:BY]->(a:Artist)
RETURN s.title AS song, a.name AS artist, al.title AS album,
       sum(r.plays) AS plays, sum(r.skips) AS skips, count(DISTINCT u) AS listeners
ORDER BY plays DESC
LIMIT 10;

// 5) Most recent listens (proves records arrive in real time)
MATCH (u:User)-[r:LISTENED]->(s:Song)-[:IN_ALBUM]->(:Album)-[:BY]->(a:Artist)
RETURN r.last_event_time AS last_event, u.name AS user, s.title AS song, a.name AS artist,
       r.plays AS plays, r.skips AS skips, r.last_device AS device, r.last_batch AS batch
ORDER BY last_event DESC
LIMIT 15;

// 6) Latest 1-minute genre windows (written by the windowed aggregation)
MATCH (w:GenreWindow)
RETURN w.window_start AS window_start, w.genre AS genre, w.plays AS plays, w.skips AS skips,
       w.likes AS likes, w.listeners AS listeners, w.skip_rate AS skip_rate_pct
ORDER BY window_start DESC, plays DESC
LIMIT 20;

// 7) Trending artists in the latest sliding window
MATCH (t:ArtistTrend)
WITH max(t.window_start) AS latest
MATCH (t:ArtistTrend {window_start: latest})
RETURN t.artist AS artist, t.plays AS plays, t.likes AS likes, t.skips AS skips, t.trend_score AS score
ORDER BY score DESC;

// 8) Graph bonus: "users who liked this song also liked..." (simple recommendation)
MATCH (s:Song)<-[:LIKED]-(:User)-[:LIKED]->(other:Song)-[:IN_ALBUM]->(:Album)-[:BY]->(a:Artist)
WHERE s <> other
WITH s, other, a, count(*) AS shared_likes
ORDER BY shared_likes DESC
RETURN s.title AS liked_song, other.title AS also_liked, a.name AS by_artist, shared_likes
LIMIT 10;

// 9) Small graph to screenshot (Graph view in Neo4j Browser)
MATCH p = (u:User)-[:LIKED]->(s:Song)-[:IN_ALBUM]->(:Album)-[:BY]->(:Artist)-[:PLAYS_GENRE]->(:Genre)
RETURN p
LIMIT 25;
