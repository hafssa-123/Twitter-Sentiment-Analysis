# 🐦 Analyse des Tweets en Temps Réel
### Stack : Apache Kafka · Cassandra · Elasticsearch · Kibana · OpenAI (optionnel)

---

## 📁 Structure du projet

```
twitter-analysis/
├── docker-compose.yml          ← Tous les services
├── .env                        ← Configuration (clés API, etc.)
├── setup_kibana.sh             ← Script de config Kibana (optionnel)
├── transform_tweets.py         ← Convertit un export brut Apify vers tweets_data.json
│
├── producer/
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── tweets_data.json        ← Vrais tweets extraits via Apify
│   └── producer.py             ← Lit tweets_data.json → Kafka
│
├── consumer/
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── sentiment_local.py      ← Analyse sentiment sans OpenAI
│   └── consumer.py             ← Kafka → Analyse → Cassandra + ES
│
└── init-scripts/
    └── init.cql                ← Schéma Cassandra
```

---

## 🚀 DÉMARRAGE — DE A À Z

### Étape 1 : Prérequis

```bash
# Vérifier que Docker et Docker Compose sont installés
docker --version
docker compose version

# Si pas installé → https://docs.docker.com/get-docker/
```

### Étape 2 : Cloner / Créer le projet

```bash
# Créer le dossier et aller dedans
mkdir twitter-analysis && cd twitter-analysis

# (Copier tous les fichiers fournis dans ce dossier)
```

### Étape 2 bis : Récupérer de vrais tweets avec Apify

Le producer peut fonctionner de **deux façons** :

#### Mode A — Connexion directe à l'API Apify (recommandé)

Le producer interroge directement l'API Apify et récupère le **dernier run
réussi** de 2 Actors configurés. Aucune action manuelle d'export n'est
nécessaire.

1. Configure le `.env` :
```bash
APIFY_TOKEN=apify_api_xxxxxxxxxxxx       # Console -> Settings -> API & Integrations
APIFY_ACTOR_1=apidojo/twitter-scraper-lite
APIFY_ACTOR_2=apidojo/tweet-scraper
APIFY_REFRESH_SECONDS=300                 # relit Apify toutes les 5 min
```

2. Lance (ou relance manuellement) un scraping sur tes 2 Actors depuis la
   console Apify quand tu veux des données fraîches. **Le scraping n'est
   pas déclenché automatiquement** par le producer — c'est volontaire,
   pour ne pas consommer de crédits Apify à chaque cycle.

3. Le producer va automatiquement :
   - récupérer le dernier dataset de chaque Actor au démarrage
   - les fusionner (déduplication par ID)
   - se reconnecter périodiquement (`APIFY_REFRESH_SECONDS`) pour prendre
     en compte un nouveau run que tu aurais lancé entre-temps
   - **basculer automatiquement sur `tweets_data.json`** (fichier local)
     si l'API Apify ne renvoie rien (pas de run, token invalide, erreur
     réseau...) — le projet ne plante jamais faute de données

#### Mode B — Fichier local statique (fallback / mode hors-ligne)

Si `APIFY_TOKEN` ou les Actor IDs ne sont pas renseignés dans le `.env`,
le producer lit directement `producer/tweets_data.json`. Un fichier
d'exemple est déjà fourni, mais tu peux le régénérer :

```bash
# 1. Exporte le résultat d'un run Apify en JSON (raw_tweets.json)
# 2. Convertis-le au format attendu par le producer :
python transform_tweets.py raw_tweets.json producer/tweets_data.json

# Tu peux fusionner plusieurs exports en une seule commande :
python transform_tweets.py raw_tweets_1.json raw_tweets_2.json producer/tweets_data.json
```

⚠️ Si tu modifies `tweets_data.json` après le premier `docker compose up`,
pense à reconstruire l'image du producer : `docker compose up producer --build`

⚠️ Ne partage/commite jamais ton `APIFY_TOKEN` (équivalent à un mot de
passe d'accès à ton compte Apify).

### Étape 3 : Configurer le `.env`

```bash
# Sans OpenAI (analyse locale, fonctionne immédiatement)
USE_OPENAI=false
OPENAI_API_KEY=    # laisser vide

# Avec OpenAI (analyse plus précise)
USE_OPENAI=true
OPENAI_API_KEY=sk-VOTRE_VRAIE_CLE
```

### Étape 4 : Lancer tous les services

```bash
docker compose up --build
```

⏳ **Première fois : attendre ~3-5 minutes** que tous les services démarrent.

Tu dois voir dans les logs :
```
tweet-producer | ✅ Connecté à Kafka !
tweet-producer | 📤 Envoi de tweets...
tweet-consumer | ✅ Kafka connecté !
tweet-consumer | ✅ Cassandra connecté !
tweet-consumer | ✅ Elasticsearch connecté !
tweet-consumer | 🎧 En écoute sur le topic 'tweets'...
tweet-consumer | [#1] 😊 POSITIF  | @alice_dev   | score=+0.80 | J'adore #Python...
```

### Étape 5 : Accéder aux interfaces

| Interface | URL | Description |
|---|---|---|
| **Kibana** | http://localhost:5601 | Dashboards principaux |
| **Kafka UI** | http://localhost:8080 | Voir les messages Kafka |
| **Elasticsearch** | http://localhost:9200 | API REST |

---

## 📊 CONFIGURER KIBANA (Dashboards)

### 1. Créer l'Index Pattern

1. Ouvrir http://localhost:5601
2. Menu burger → **Stack Management**
3. **Index Patterns** → **Create index pattern**
4. Name: `tweets*`
5. Timestamp field: `timestamp`
6. **Save**

### 2. Explorer les données

Menu → **Analytics** → **Discover**
→ Sélectionner `tweets*`
→ Tu vois les tweets en temps réel !

### 3. Créer les Visualisations

Menu → **Analytics** → **Dashboard** → **Create dashboard**

#### 🥧 Pie Chart : Répartition des sentiments
- **Aggregation**: Terms
- **Field**: `sentiment`
- **Size**: 3

#### ☁️ Tag Cloud : Hashtags les plus utilisés
- **Aggregation**: Terms
- **Field**: `hashtags`
- **Max**: 50

#### 📊 Bar Chart : Mots les plus fréquents
- **Aggregation**: Terms
- **Field**: `words`
- **Size**: 20

#### 📈 Line Chart : Volume de tweets par temps
- **X-axis**: Date Histogram → `timestamp`
- **Interval**: Auto

#### 🏆 Table : Meilleurs tweets
- Ajouter filtre: `sentiment: POSITIF`
- Trier par: `likes` (descending)

#### 💀 Table : Pires tweets
- Ajouter filtre: `sentiment: NÉGATIF`
- Trier par: `score` (ascending)

---

## 🔍 Vérifier que tout fonctionne

### Vérifier Kafka
```bash
# Voir les messages dans le topic
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic tweets \
  --from-beginning \
  --max-messages 5
```

### Vérifier Cassandra
```bash
# Se connecter à Cassandra
docker exec -it cassandra cqlsh

# Dans cqlsh :
USE twitter;
SELECT * FROM tweets LIMIT 5;
SELECT * FROM sentiment_stats;
SELECT * FROM hashtag_stats;
```

### Vérifier Elasticsearch
```bash
# Voir les tweets indexés
curl http://localhost:9200/tweets/_count

# Voir un tweet
curl http://localhost:9200/tweets/_search?pretty&size=1

# Stats par sentiment
curl -X GET "http://localhost:9200/tweets/_search?pretty" \
  -H "Content-Type: application/json" \
  -d '{
    "size": 0,
    "aggs": {
      "sentiments": {
        "terms": { "field": "sentiment" }
      }
    }
  }'
```

---

## 🛑 Arrêter le projet

```bash
# Arrêter les conteneurs
docker compose down

# Arrêter ET supprimer les données (reset complet)
docker compose down -v
```

---

## 🐛 Problèmes courants

| Problème | Solution |
|---|---|
| Cassandra lent à démarrer | Normal, attendre 60-90s |
| Consumer crashe au démarrage | Il va se relancer automatiquement (restart: on-failure) |
| Pas de mémoire | Allouer 4GB+ RAM à Docker |
| Port déjà utilisé | Changer le port dans docker-compose.yml |

---

## 🔧 Avec OpenAI (optionnel)

```bash
# Dans .env :
USE_OPENAI=true
OPENAI_API_KEY=sk-VOTRE_CLE

# Relancer le consumer
docker compose up consumer --build
```

L'analyse sera plus précise mais utilise des tokens OpenAI.
Sans OpenAI, l'analyse locale par mots-clés fonctionne très bien.
