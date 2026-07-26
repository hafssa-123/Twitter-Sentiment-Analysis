# Twitter/X Sentiment Analysis

Twitter (aujourd'hui appele X) recoit des milliers de nouveaux messages chaque seconde. Ce projet est un programme qui lit automatiquement ces messages des qu'ils sont publies, et determine pour chacun s'il exprime quelque chose de positif, negatif, ou neutre. Les resultats s'affichent ensuite sous forme de graphiques qui se mettent a jour tout seuls, pour visualiser en un coup d'oeil la tendance generale (par exemple : "aujourd'hui, 70% des messages sur ce sujet sont positifs").

Projet realise dans le cadre du programme MLAIM.

## 🏗️ Architecture

```
Apify (scraping tweets)
        │
        ▼
   producer.py ──────► Kafka (topic "tweets")
                           │
              ┌────────────┴────────────┐
              ▼                         ▼
       consumer.py              spark_consumer/
     (kafka-python,          (PySpark Structured
      message par message)     Streaming, micro-batchs)
              │                         │
              └────────────┬────────────┘
                            ▼
                 Cassandra + Elasticsearch
                            │
                            ▼
                         Kibana
                   (dashboards temps réel)
```

## Fonctionnement

1. Les tweets sont recuperes via l'API Apify (avec fallback sur un fichier JSON local en cas d'echec)
2. Ils sont envoyes vers Kafka par le producer
3. Un consumer les lit, analyse leur sentiment (localement par mots-cles, ou via le LLM Groq) et les ecrit dans Cassandra et Elasticsearch
4. Kibana affiche les resultats sous forme de dashboards

Deux versions du consumer sont disponibles :
- `consumer/` : version classique avec kafka-python
- `spark_consumer/` : version avec PySpark Structured Streaming (traitement par micro-batchs, pour le passage a l'echelle)

## Utilite

- Surveillance de marque : savoir en direct si les gens parlent positivement ou negativement d'une entreprise ou d'un produit
- Gestion de crise : reperer tres vite qu'un sujet devient negatif pour reagir a temps
- Veille concurrentielle : comparer le sentiment autour d'une marque a celui de ses concurrents
- Analyse politique/sociale : suivre l'opinion publique sur un sujet ou un evenement
- Support client : identifier automatiquement les clients mecontents pour les traiter en priorite

## Stack technique

Kafka, Cassandra, Elasticsearch, Kibana, Docker Compose, Apify, Groq (llama-3.3-70b-versatile), PySpark

## Structure

```
twitter-analysis/
├── docker-compose.yml
├── .env                  (non versionne)
├── producer/              -> lit les tweets et les envoie vers Kafka
├── consumer/               -> consumer classique
├── spark_consumer/         -> consumer Spark Structured Streaming
└── init-scripts/           -> schema Cassandra
```

## Demarrage

```bash
docker compose up --build
```

Interfaces disponibles :
- Kibana : http://localhost:5601
- Kafka UI : http://localhost:8080
- Elasticsearch : http://localhost:9200

## Licence

MIT
