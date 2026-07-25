"""
PRODUCER : Récupère les tweets directement depuis l'API Apify
           (dernier run réussi de 2 Actors), les fusionne, et les
           envoie dans Kafka en boucle continue.

           Si l'appel à l'API Apify échoue, affiche une erreur claire.
           Pas de fallback fichier JSON.
"""
import json
import time
import random
import os
import re
import uuid
import logging
from datetime import datetime

import requests
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [PRODUCER] %(message)s"
)
log = logging.getLogger(__name__)

# ─── Config Kafka ─────────────────────────
KAFKA_BROKER      = os.getenv("KAFKA_BROKER", "kafka:9092")
TOPIC_NAME        = os.getenv("TOPIC_NAME", "tweets")
TWEETS_PER_SEC    = float(os.getenv("TWEETS_PER_SECOND", "0.4"))
DELAY             = 1.0 / TWEETS_PER_SEC

# ─── Config recherche ─────────────────────
SCRAPING_KEYWORD  = os.getenv("SCRAPING_KEYWORD", "world cup")

# ─── Config Apify ─────────────────────────
APIFY_TOKEN       = os.getenv("APIFY_TOKEN", "")
APIFY_ACTOR_1     = os.getenv("APIFY_ACTOR_1", "")
APIFY_ACTOR_2     = os.getenv("APIFY_ACTOR_2", "")
APIFY_REFRESH_SEC = float(os.getenv("APIFY_REFRESH_SECONDS", "300"))


# ════════════════════════════════════════════════════════════
# SOURCE — APIFY
# ════════════════════════════════════════════════════════════
def extract_username(raw: dict) -> str:
    author = raw.get("author")
    if isinstance(author, dict):
        handle = author.get("userName") or author.get("username")
        if handle:
            return handle
    url = raw.get("twitterUrl") or raw.get("url") or ""
    match = re.search(r"(?:twitter|x)\.com/([^/]+)/status/", url)
    if match:
        return match.group(1)
    return "unknown_user"


def transform_apify_tweet(raw: dict) -> dict:
    return {
        "id":        str(raw.get("id") or uuid.uuid4()),
        "text":      raw.get("text", ""),
        "user":      extract_username(raw),
        "timestamp": raw.get("createdAt", ""),
        "likes":     raw.get("likeCount", 0) or 0,
        "retweets":  raw.get("retweetCount", 0) or 0,
    }


def fetch_actor_dataset(actor_id: str) -> list:
    if not actor_id or not APIFY_TOKEN:
        return []
    actor_path = actor_id.replace("/", "~")
    url = f"https://api.apify.com/v2/acts/{actor_path}/runs/last/dataset/items"
    try:
        resp = requests.get(
            url,
            params={"token": APIFY_TOKEN, "status": "SUCCEEDED"},
            timeout=20,
        )
        if resp.status_code != 200:
            log.warning(f"⚠️  Apify [{actor_id}] → HTTP {resp.status_code}: {resp.text[:200]}")
            return []
        data = resp.json()
        if not isinstance(data, list):
            log.warning(f"⚠️  Apify [{actor_id}] → réponse inattendue")
            return []
        log.info(f"✅ Apify [{actor_id}] → {len(data)} tweets récupérés")
        return data
    except requests.RequestException as e:
        log.warning(f"⚠️  Apify [{actor_id}] → erreur réseau: {e}")
        return []


def fetch_apify_tweets() -> list:
    log.info("📡 Récupération tweets depuis Apify...")
    all_raw = []
    for actor_id in (APIFY_ACTOR_1, APIFY_ACTOR_2):
        if actor_id:
            all_raw.extend(fetch_actor_dataset(actor_id))

    tweets = []
    seen_ids = set()
    for raw in all_raw:
        if raw.get("noResults"):
            continue
        if not raw.get("text"):
            continue
        tweet = transform_apify_tweet(raw)
        if tweet["id"] in seen_ids:
            continue
        seen_ids.add(tweet["id"])
        tweets.append(tweet)

    if tweets:
        log.info(f"✅ Apify → {len(tweets)} tweets disponibles")
    else:
        log.error("❌ Apify → 0 tweet récupéré ! Vérifie ton APIFY_TOKEN et tes Actor IDs.")
    return tweets


def parse_original_timestamp(ts_str) -> float:
    if not ts_str:
        return time.time()
    if isinstance(ts_str, (int, float)):
        return float(ts_str)
    try:
        dt = datetime.strptime(str(ts_str), "%a %b %d %H:%M:%S %z %Y")
        return dt.timestamp()
    except (ValueError, TypeError):
        pass
    try:
        ts_clean = str(ts_str).replace("Z", "+00:00")
        dt = datetime.fromisoformat(ts_clean)
        return dt.timestamp()
    except (ValueError, TypeError):
        return time.time()


# ─── Connexion Kafka avec retry ───────────
def create_producer():
    retries = 0
    while True:
        try:
            log.info(f"Connexion à Kafka ({KAFKA_BROKER})...")
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
                retries=3,
                acks="all"
            )
            log.info("✅ Connecté à Kafka !")
            return producer
        except NoBrokersAvailable:
            retries += 1
            wait = min(30, 5 * retries)
            log.warning(f"Kafka non disponible. Retry dans {wait}s... (tentative {retries})")
            time.sleep(wait)


# ─── Main ─────────────────────────────────
def main():
    producer = create_producer()
    count = 0

    log.info(f"📤 Envoi vers '{TOPIC_NAME}' | mot-clé: '{SCRAPING_KEYWORD}' | délai: {DELAY}s")

    TWEETS = fetch_apify_tweets()
    last_refresh = time.time()

    if not TWEETS:
        raise RuntimeError(
            "Aucun tweet disponible depuis Apify. "
            "Vérifie ton APIFY_TOKEN et que tes Actors ont des runs réussis."
        )

    while True:
        # Rafraîchissement périodique
        if time.time() - last_refresh > APIFY_REFRESH_SEC:
            log.info("🔄 Rafraîchissement Apify...")
            fresh = fetch_apify_tweets()
            if fresh:
                TWEETS = fresh
            last_refresh = time.time()

        tweet_template = random.choice(TWEETS)

        tweet = {
            "id":        str(uuid.uuid4()),  # nouvel ID à chaque envoi
            "text":      tweet_template["text"],
            "user":      tweet_template["user"],
            "timestamp": datetime.now().isoformat(),  # timestamp actuel
            "likes":     tweet_template.get("likes", 0),
            "retweets":  tweet_template.get("retweets", 0),
        }

        future = producer.send(TOPIC_NAME, tweet)
        future.get(timeout=10)

        count += 1
        log.info(f"[#{count}] @{tweet['user']} → {tweet['text'][:60]}...")
        time.sleep(DELAY)


if __name__ == "__main__":
    main()
