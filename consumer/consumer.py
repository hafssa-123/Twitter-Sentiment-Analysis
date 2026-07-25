"""
CONSUMER : Lit les tweets Kafka, analyse le sentiment,
           stocke dans Cassandra et indexe dans Elasticsearch
"""
import json
import os
import re
import time
import uuid
import logging
from dotenv import load_dotenv
from datetime import datetime

from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
from cassandra.cluster import Cluster
from cassandra.policies import RetryPolicy
from elasticsearch import Elasticsearch
from sentiment_local import analyze_sentiment
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [CONSUMER] %(message)s"
)
log = logging.getLogger(__name__)

KAFKA_BROKER   = os.getenv("KAFKA_BROKER", "kafka:9092")
TOPIC_NAME     = os.getenv("TOPIC_NAME", "tweets")
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "cassandra")
ES_HOST        = os.getenv("ES_HOST", "http://elasticsearch:9200")
GROQ_API_KEY   = os.getenv("GROQ_API_KEY")
USE_GROQ       = os.getenv("USE_GROQ", "false").lower() == "true"

groq_client = None
if USE_GROQ and GROQ_API_KEY:
    try:
        from groq import Groq
        groq_client = Groq(api_key=GROQ_API_KEY)
        log.info("✅ Groq activé (llama-3.3-70b-versatile)")
    except Exception as e:
        log.warning(f"Groq désactivé: {e}")
else:
    if USE_GROQ and not GROQ_API_KEY:
        log.warning("⚠️  USE_GROQ=true mais GROQ_API_KEY manquant dans .env !")

def extract_hashtags(text: str) -> list:
    return re.findall(r"#\w+", text)

def extract_words(text: str) -> list:
    words = re.findall(r"\b[a-zA-ZÀ-ÿ]{4,}\b", text)
    stop_words = {
        "avec", "dans", "pour", "les", "des", "une", "est", "qui", "que",
        "sur", "this", "that", "with", "from", "have", "mais", "plus",
        "mon", "mes", "son", "encore", "très", "tout", "bien", "cette", "notre"
    }
    return [w.lower() for w in words if w.lower() not in stop_words]

def safe_int(val) -> int:
    """Convertit n'importe quelle valeur en int sans erreur."""
    try:
        return int(float(str(val).strip()))
    except (TypeError, ValueError):
        return 0

def get_field(tweet: dict, *keys) -> any:
    """Récupère un champ du tweet en essayant plusieurs clés possibles
    (gère les clés avec espaces ou variations)."""
    for key in keys:
        # cherche la clé exacte
        if key in tweet:
            return tweet[key]
        # cherche en ignorant les espaces dans les clés
        for k, v in tweet.items():
            if k.replace(" ", "") == key.replace(" ", ""):
                return v
    return None

def get_sentiment_groq(text: str) -> tuple:
    try:
        response = groq_client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[
                {
                    "role": "system",
                    "content": "You are a sentiment classifier. Reply with ONLY one word: POSITIF, NEGATIF, or NEUTRE. No explanation, no punctuation, just the single word."
                },
                {
                    "role": "user",
                    "content": f"Tweet: {text}"
                }
            ],
            max_tokens=5,
            temperature=0
        )
        raw = response.choices[0].message.content.strip().upper()
        log.info(f">>> Groq raw: '{raw}'")

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

def get_sentiment(text: str) -> tuple:
    if groq_client:
        return get_sentiment_groq(text)
    return analyze_sentiment(text)

def connect_kafka():
    for attempt in range(1, 20):
        try:
            log.info(f"[{attempt}] Connexion Kafka ({KAFKA_BROKER})...")
            consumer = KafkaConsumer(
                TOPIC_NAME,
                bootstrap_servers=KAFKA_BROKER,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                auto_offset_reset="earliest",
                group_id="tweet-consumer-group",
                consumer_timeout_ms=-1,
            )
            log.info("✅ Kafka connecté !")
            return consumer
        except NoBrokersAvailable:
            wait = min(30, 5 * attempt)
            log.warning(f"Kafka indisponible. Attente {wait}s...")
            time.sleep(wait)
    raise RuntimeError("Impossible de se connecter à Kafka")

def connect_cassandra():
    for attempt in range(1, 15):
        try:
            log.info(f"[{attempt}] Connexion Cassandra ({CASSANDRA_HOST})...")
            cluster = Cluster(
                [CASSANDRA_HOST],
                connect_timeout=20,
                default_retry_policy=RetryPolicy()
            )
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
            return session
        except Exception as e:
            wait = min(30, 5 * attempt)
            log.warning(f"Cassandra erreur: {e}. Attente {wait}s...")
            time.sleep(wait)
    raise RuntimeError("Impossible de se connecter à Cassandra")

def connect_elasticsearch():
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
            return es
        except Exception as e:
            wait = min(30, 5 * attempt)
            log.warning(f"Elasticsearch erreur: {e}. Attente {wait}s...")
            time.sleep(wait)
    raise RuntimeError("Impossible de se connecter à Elasticsearch")

INSERT_TWEET = """
    INSERT INTO tweets (id, text, username, sentiment, hashtags, words, score, likes, retweets, ts)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""
UPDATE_HASHTAG   = "UPDATE hashtag_stats SET count = count + 1 WHERE hashtag = %s"
UPDATE_SENTIMENT = "UPDATE sentiment_stats SET count = count + 1 WHERE sentiment = %s"

def main():
    kafka     = connect_kafka()
    cassandra = connect_cassandra()
    es        = connect_elasticsearch()

    count = 0
    log.info(f"🎧 En écoute sur le topic '{TOPIC_NAME}'...")

    for message in kafka:
        tweet = message.value
        try:
            text     = str(tweet.get("text", ""))
            username = str(tweet.get("user", "unknown"))

            # get_field gère les clés avec espaces (ex: 'like  es')
            likes    = safe_int(get_field(tweet, "likes", "like s", "like  es") or 0)
            retweets = safe_int(get_field(tweet, "retweets", "retweet s") or 0)

            ts_raw = tweet.get("timestamp", "")
            if isinstance(ts_raw, str):
                try:
                    ts = datetime.fromisoformat(ts_raw)
                except ValueError:
                    ts = datetime.now()
            elif isinstance(ts_raw, (int, float)):
                ts = datetime.fromtimestamp(ts_raw)
            else:
                ts = datetime.now()

            sentiment, score = get_sentiment(text)
            hashtags = extract_hashtags(text)
            words    = extract_words(text)
            tweet_id = uuid.UUID(str(tweet.get("id", str(uuid.uuid4()))))

            cassandra.execute(INSERT_TWEET, (
                tweet_id, text, username, sentiment,
                hashtags, words, float(score), likes, retweets, ts
            ))
            cassandra.execute(UPDATE_SENTIMENT, (sentiment,))
            for tag in hashtags:
                cassandra.execute(UPDATE_HASHTAG, (tag,))

            es.index(index="tweets", id=str(tweet_id), document={
                "text":      text,
                "username":  username,
                "sentiment": sentiment,
                "hashtags":  hashtags,
                "words":     words,
                "score":     float(score),
                "likes":     likes,
                "retweets":  retweets,
                "timestamp": ts.isoformat()
            })

            count += 1
            emoji = "😊" if sentiment == "POSITIF" else ("😡" if sentiment == "NEGATIF" else "😐")
            log.info(
                f"[#{count}] {emoji} {sentiment:8s} | @{username:12s} | "
                f"#{len(hashtags)} hashtags | score={score:+.2f} | {text[:50]}..."
            )

        except Exception as e:
            log.error(f"Erreur traitement tweet: {e} | tweet={tweet}")

if __name__ == "__main__":
    main()
