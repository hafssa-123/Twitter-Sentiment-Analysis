"""
TRANSFORM : Convertit le JSON brut exporté d'Apify (Tweet Scraper)
            vers le format attendu par producer.py / consumer.py

Usage :
    python transform_tweets.py raw_tweets.json tweets_data.json
    (tu peux donner PLUSIEURS fichiers bruts en entrée, ils seront fusionnés)
"""
import json
import re
import sys
import uuid


def extract_username(tweet: dict) -> str:
    """Extrait le nom d'utilisateur depuis l'URL du tweet (Apify ne donne pas
    toujours un champ 'author' direct, mais l'URL contient toujours le handle)."""
    url = tweet.get("twitterUrl") or tweet.get("url") or ""
    match = re.search(r"(?:twitter|x)\.com/([^/]+)/status/", url)
    if match:
        return match.group(1)
    return "unknown_user"


def transform_tweet(raw: dict) -> dict:
    """Reformate un tweet brut Apify vers le format de producer.py :
    {id, text, user, timestamp, likes, retweets}"""
    return {
        "id": raw.get("id", str(uuid.uuid4())),
        "text": raw.get("text", ""),
        "user": extract_username(raw),
        "timestamp": raw.get("createdAt", ""),  # on garde la date d'origine
        "likes": raw.get("likeCount", 0),
        "retweets": raw.get("retweetCount", 0),
    }


def main():
    if len(sys.argv) < 3:
        print("Usage: python transform_tweets.py <fichier1.json> [fichier2.json ...] <sortie.json>")
        sys.exit(1)

    input_files = sys.argv[1:-1]
    output_file = sys.argv[-1]

    all_tweets = []
    seen_ids = set()

    for path in input_files:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)

        for raw in data:
            # Ignore les entrées "noResults": true (recherches vides)
            if raw.get("noResults"):
                continue
            # Ignore si pas de texte
            if not raw.get("text"):
                continue

            tweet = transform_tweet(raw)

            # Évite les doublons (même tweet récupéré dans 2 runs différents)
            if tweet["id"] in seen_ids:
                continue
            seen_ids.add(tweet["id"])

            all_tweets.append(tweet)

    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(all_tweets, f, ensure_ascii=False, indent=2)

    print(f"✅ {len(all_tweets)} tweets fusionnés et sauvegardés dans '{output_file}'")
    if all_tweets:
        print("\nExemple de tweet transformé :")
        print(json.dumps(all_tweets[0], ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
