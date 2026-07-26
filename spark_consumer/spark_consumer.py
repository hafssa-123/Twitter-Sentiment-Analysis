"""
SPARK CONSUMER : Lit les tweets depuis Kafka avec Spark Structured Streaming,
                  analyse le sentiment, extrait hashtags/mots, puis écrit
                  chaque micro-batch dans Cassandra et Elasticsearch.

Remplace le consumer.py (kafka-python) par un traitement en streaming
distribué avec PySpark.
"""
import os
import re
import time
import uuid
import logging
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType, ArrayType, FloatType

from cassandra.cluster import Cluster
from cassandra.policies import RetryPolicy
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [SPARK-CONSUMER] %(message)s"
)
log = logging.getLogger(__name__)

KAFKA_BROKER   = os.getenv("KAFKA_BROKER", "kafka:9092")
TOPIC_NAME     = os.getenv("TOPIC_NAME", "tweets")
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "cassandra")
ES_HOST        = os.getenv("ES_HOST", "http://elasticsearch:9200")
CHECKPOINT_DIR = os.getenv("CHECKPOINT_DIR", "/tmp/checkpoints/tweets")
GROQ_API_KEY   = os.getenv("GROQ_API_KEY")
USE_GROQ       = os.getenv("USE_GROQ", "false").lower() == "true"

groq_client = None
if USE_GROQ and GROQ_API_KEY:
    try:
        from groq import Groq
        groq_client = Groq(api_key=GROQ_API_KEY)
        log.info("✅ Groq activé (llama-3.3-70b-versatile) — utilisé en plus de Spark pour le sentiment")
    except Exception as e:
        log.warning(f"Groq désactivé: {e}")
else:
    if USE_GROQ and not GROQ_API_KEY:
        log.warning("⚠️  USE_GROQ=true mais GROQ_API_KEY manquant !")

# ════════════════════════════════════════════════════════════
# ANALYSE DE SENTIMENT LOCALE (identique à sentiment_local.py)
# ════════════════════════════════════════════════════════════
POSITIVE_WORDS = [
    "adore", "aime", "aimer", "adorer", "content", "heureux", "heureuse",
    "ravi", "ravie", "satisfait", "enchanté", "fier", "fière", "bravo",
    "félicitations", "merci", "excellent", "magnifique", "impressionnant",
    "incroyable", "extraordinaire", "formidable", "fantastique", "génial",
    "super", "cool", "top", "parfait", "parfaite", "sympa", "agréable",
    "plaisant", "réussi", "réussie", "réussite", "victoire", "gagner",
    "champion", "meilleur", "bien", "bon", "beau", "belle", "utile",
    "pratique", "facile", "rapide", "efficace", "puissant", "innovation",
    "innovant", "nouveau", "progrès", "avancée", "révolution", "succès",
    "productif", "créatif", "intelligent", "brillant", "prometteur",
    "espoir", "optimiste", "positif", "favorable", "bénéfique",
    "j'aime", "j adore", "trop bien", "waouh", "ouais",
    "love", "like", "happy", "great", "amazing", "awesome", "excellent",
    "fantastic", "wonderful", "brilliant", "outstanding", "perfect",
    "best", "good", "nice", "beautiful", "incredible", "impressive",
    "excited", "thrilled", "pleased", "glad", "joy", "enjoy",
    "win", "winner", "success", "helpful", "useful", "easy", "fast",
    "powerful", "smart", "innovative", "thanks", "thank", "grateful",
    "proud", "hope", "optimistic", "positive", "benefit", "progress",
    "revolutionary", "advanced", "promising", "opportunity", "achieve",
    "wow", "superb", "remarkable", "exceptional", "delightful"
]

NEGATIVE_WORDS = [
    "bug", "panne", "crash", "erreur", "problème", "échec", "raté",
    "nul", "inutile", "mauvais", "mauvaise", "horrible", "terrible",
    "catastrophe", "désastre", "galère", "marre", "ras-le-bol",
    "déçu", "déçue", "déception", "frustré", "frustrée", "frustration",
    "énervé", "énervée", "agacé", "agacée", "colère", "fâché",
    "triste", "malheureux", "malheureuse", "dommage", "honte",
    "scandale", "inacceptable", "pathétique", "médiocre", "minable",
    "lent", "lente", "latence", "impossible", "épuisant", "difficile",
    "dangereux", "risque", "menace", "peur", "crainte", "inquiet",
    "perdre", "perte", "défaillance", "dysfonctionnement", "plantage",
    "malheureusement", "hélas", "encore ce", "toujours pareil",
    "n'arrive pas", "ne fonctionne pas", "ne marche pas",
    "hate", "dislike", "worst", "terrible", "awful", "bad", "horrible",
    "broken", "useless", "ugly", "slow", "crash", "fail", "failed",
    "failure", "error", "bug", "issue", "problem", "disappointed",
    "disappointing", "frustrating", "annoying", "angry", "sad", "boring",
    "waste", "poor", "stupid", "ridiculous", "wrong", "dangerous",
    "threat", "risk", "fear", "worried", "concern", "harmful", "damage",
    "lose", "loss", "never works", "always fails", "pointless",
    "overrated", "scam", "nightmare", "disaster"
]

STOP_WORDS = {
    "avec", "dans", "pour", "les", "des", "une", "est", "qui", "que",
    "sur", "this", "that", "with", "from", "have", "mais", "plus",
    "mon", "mes", "son", "encore", "très", "tout", "bien", "cette", "notre"
}


def analyze_sentiment(text):
    if not text:
        return "NEUTRE", 0.0
    t = text.lower()
    pos = sum(1 for w in POSITIVE_WORDS if w in t)
    neg = sum(1 for w in NEGATIVE_WORDS if w in t)
    total = pos + neg
    if total == 0:
        return "NEUTRE", 0.0
    score = (pos - neg) / total
    if score > 0.1:
        sentiment = "POSITIF"
    elif score < -0.1:
        sentiment = "NEGATIF"
    else:
        sentiment = "NEUTRE"
    return sentiment, round(score, 2)


def extract_hashtags(text):
    return re.findall(r"#\w+", text or "")


def extract_words(text):
    words = re.findall(r"\b[a-zA-ZÀ-ÿ]{4,}\b", text or "")
    return [w.lower() for w in words if w.lower() not in STOP_WORDS]


sentiment_udf = F.udf(lambda t: analyze_sentiment(t)[0], StringType())
score_udf = F.udf(lambda t: float(analyze_sentiment(t)[1]), FloatType())
hashtags_udf = F.udf(extract_hashtags, ArrayType(StringType()))
words_udf = F.udf(extract_words, ArrayType(StringType()))


def get_sentiment_groq(text):
    """Sentiment via Groq/LLaMA 3. Retombe sur l'analyse locale en cas d'erreur."""
    try:
        response = groq_client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[
                {
                    "role": "system",
                    "content": "You are a sentiment classifier. Reply with ONLY one word: POSITIF, NEGATIF, or NEUTRE. No explanation, no punctuation, just the single word."
                },
                {"role": "user", "content": f"Tweet: {text}"}
            ],
            max_tokens=5,
            temperature=0
        )
        raw = response.choices[0].message.content.strip().upper()
        if "POSITIF" in raw or "POSITIVE" in raw:
            sentiment = "POSITIF"
        elif "NEGATIF" in raw or "NEGATIVE" in raw or "NÉGATIF" in raw:
            sentiment = "NEGATIF"
        else:
            sentiment = "NEUTRE"
        score = 1.0 if sentiment == "POSITIF" else (-1.0 if sentiment == "NEGATIF" else 0.0)
        return sentiment, score
    except Exception as e:
        log.warning(f"Groq error: {e} → fallback local")
        return analyze_sentiment(text)


def get_sentiment(text):
    if groq_client:
        return get_sentiment_groq(text)
    return analyze_sentiment(text)


# ════════════════════════════════════════════════════════════
# CASSANDRA / ELASTICSEARCH — schéma et connexions
# ════════════════════════════════════════════════════════════
INSERT_TWEET = """
    INSERT INTO tweets (id, text, username, sentiment, hashtags, words, score, likes, retweets, ts)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""
UPDATE_HASHTAG = "UPDATE hashtag_stats SET count = count + 1 WHERE hashtag = %s"
UPDATE_SENTIMENT = "UPDATE sentiment_stats SET count = count + 1 WHERE sentiment = %s"


def ensure_cassandra_schema():
    for attempt in range(1, 15):
        try:
            log.info(f"[{attempt}] Connexion Cassandra ({CASSANDRA_HOST})...")
            cluster = Cluster([CASSANDRA_HOST], connect_timeout=20, default_retry_policy=RetryPolicy())
            session = cluster.connect()
            session.execute("""
                CREATE KEYSPACE IF NOT EXISTS twitter
                WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
            """)
            session.set_keyspace("twitter")
            session.execute("""
                CREATE TABLE IF NOT EXISTS tweets (
                    id        UUID PRIMARY KEY,
                    text      TEXT,
                    username  TEXT,
                    sentiment TEXT,
                    hashtags  LIST<TEXT>,
                    words     LIST<TEXT>,
                    score     FLOAT,
                    likes     INT,
                    retweets  INT,
                    ts        TIMESTAMP
                )
            """)
            session.execute("""
                CREATE TABLE IF NOT EXISTS hashtag_stats (
                    hashtag TEXT PRIMARY KEY,
                    count   COUNTER
                )
            """)
            session.execute("""
                CREATE TABLE IF NOT EXISTS sentiment_stats (
                    sentiment TEXT PRIMARY KEY,
                    count     COUNTER
                )
            """)
            log.info("✅ Cassandra connecté et tables créées !")
            cluster.shutdown()
            return
        except Exception as e:
            wait = min(30, 5 * attempt)
            log.warning(f"Cassandra erreur: {e}. Attente {wait}s...")
            time.sleep(wait)
    raise RuntimeError("Impossible de se connecter à Cassandra")


def ensure_es_index():
    for attempt in range(1, 15):
        try:
            log.info(f"[{attempt}] Connexion Elasticsearch ({ES_HOST})...")
            es = Elasticsearch(ES_HOST)
            info = es.info()
            log.info(f"✅ Elasticsearch connecté ! Version: {info['version']['number']}")
            if not es.indices.exists(index="tweets"):
                es.indices.create(index="tweets", body={
                    "mappings": {
                        "properties": {
                            "text":      {"type": "text", "analyzer": "french"},
                            "username":  {"type": "keyword"},
                            "sentiment": {"type": "keyword"},
                            "hashtags":  {"type": "keyword"},
                            "words":     {"type": "keyword"},
                            "score":     {"type": "float"},
                            "likes":     {"type": "integer"},
                            "retweets":  {"type": "integer"},
                            "timestamp": {"type": "date"}
                        }
                    }
                })
                log.info("✅ Index 'tweets' créé dans Elasticsearch")
            return
        except Exception as e:
            wait = min(30, 5 * attempt)
            log.warning(f"Elasticsearch erreur: {e}. Attente {wait}s...")
            time.sleep(wait)
    raise RuntimeError("Impossible de se connecter à Elasticsearch")


def parse_ts(ts_raw):
    if isinstance(ts_raw, str) and ts_raw:
        try:
            return datetime.fromisoformat(ts_raw)
        except ValueError:
            return datetime.now()
    return datetime.now()


def write_batch(batch_df, batch_id):
    """Appelé par Spark pour chaque micro-batch : écrit dans Cassandra + ES."""
    rows = batch_df.collect()
    if not rows:
        log.info(f"Batch {batch_id} vide, rien à écrire.")
        return

    cluster = Cluster([CASSANDRA_HOST], connect_timeout=20, default_retry_policy=RetryPolicy())
    session = cluster.connect("twitter")
    es = Elasticsearch(ES_HOST)

    es_actions = []
    count = 0

    for row in rows:
        try:
            tweet_id = uuid.UUID(str(row["id"])) if row["id"] else uuid.uuid4()
        except ValueError:
            tweet_id = uuid.uuid4()

        text = row["text"] or ""
        username = row["user"] or "unknown"
        if groq_client:
            sentiment, score = get_sentiment_groq(text)
        else:
            sentiment, score = row["local_sentiment"], float(row["local_score"] or 0.0)
        score = float(score)
        hashtags = row["hashtags"] or []
        words = row["words"] or []
        likes = int(row["likes"] or 0)
        retweets = int(row["retweets"] or 0)
        ts = parse_ts(row["timestamp"])

        session.execute(INSERT_TWEET, (
            tweet_id, text, username, sentiment,
            hashtags, words, score, likes, retweets, ts
        ))
        session.execute(UPDATE_SENTIMENT, (sentiment,))
        for tag in hashtags:
            session.execute(UPDATE_HASHTAG, (tag,))

        es_actions.append({
            "_index": "tweets",
            "_id": str(tweet_id),
            "_source": {
                "text": text, "username": username, "sentiment": sentiment,
                "hashtags": hashtags, "words": words, "score": score,
                "likes": likes, "retweets": retweets, "timestamp": ts.isoformat()
            }
        })

        count += 1
        emoji = "😊" if sentiment == "POSITIF" else ("😡" if sentiment == "NEGATIF" else "😐")
        log.info(
            f"[batch {batch_id} #{count}] {emoji} {sentiment:8s} | @{username:12s} | "
            f"#{len(hashtags)} hashtags | score={score:+.2f} | {text[:50]}..."
        )

    if es_actions:
        bulk(es, es_actions)

    session.shutdown()
    cluster.shutdown()
    log.info(f"✅ Batch {batch_id} : {count} tweets écrits dans Cassandra + Elasticsearch")


def main():
    ensure_cassandra_schema()
    ensure_es_index()

    spark = SparkSession.builder.appName("TwitterSentimentSparkConsumer").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    schema = StructType([
        StructField("id", StringType()),
        StructField("text", StringType()),
        StructField("user", StringType()),
        StructField("timestamp", StringType()),
        StructField("likes", LongType()),
        StructField("retweets", LongType()),
    ])

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", TOPIC_NAME)
        .option("startingOffsets", "earliest")
        .load()
    )

    tweets = (
        raw.selectExpr("CAST(value AS STRING) AS json_str")
        .select(F.from_json("json_str", schema).alias("t"))
        .select("t.*")
    )

    # sentiment/score calculés par Spark (lexique local) : servent de valeur
    # par défaut, et de fallback si Groq est activé mais indisponible.
    enriched = (
        tweets
        .withColumn("local_sentiment", sentiment_udf(F.col("text")))
        .withColumn("local_score", score_udf(F.col("text")))
        .withColumn("hashtags", hashtags_udf(F.col("text")))
        .withColumn("words", words_udf(F.col("text")))
    )

    log.info(f"🎧 Spark Structured Streaming en écoute sur le topic '{TOPIC_NAME}'...")

    query = (
        enriched.writeStream
        .foreachBatch(write_batch)
        .option("checkpointLocation", CHECKPOINT_DIR)
        .outputMode("append")
        .trigger(processingTime="5 seconds")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
