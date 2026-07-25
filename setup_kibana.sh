#!/bin/bash
# ============================================================
# setup_kibana.sh : Configure automatiquement les dashboards
# Lancer APRÈS que Kibana soit démarré (attendre 2-3 minutes)
# ============================================================

ES_URL="http://localhost:9200"
KIBANA_URL="http://localhost:5601"

echo "⏳ Attente d'Elasticsearch..."
until curl -s "$ES_URL/_cluster/health" | grep -q '"status"'; do
  sleep 3
done
echo "✅ Elasticsearch OK"

echo "⏳ Attente de Kibana..."
until curl -s "$KIBANA_URL/api/status" | grep -q '"level":"available"'; do
  sleep 5
done
echo "✅ Kibana OK"

echo ""
echo "📊 Création de l'index pattern 'tweets*' dans Kibana..."
curl -s -X POST "$KIBANA_URL/api/saved_objects/index-pattern/tweets-pattern" \
  -H "kbn-xsrf: true" \
  -H "Content-Type: application/json" \
  -d '{
    "attributes": {
      "title": "tweets*",
      "timeFieldName": "timestamp"
    }
  }' | python3 -m json.tool

echo ""
echo "✅ Index pattern créé !"
echo ""
echo "🎯 Prochaines étapes dans Kibana (http://localhost:5601) :"
echo "   1. Menu → Analytics → Discover"
echo "      → Sélectionner 'tweets*' → Explorer les données"
echo ""
echo "   2. Menu → Analytics → Dashboard → Create dashboard"
echo "      Ajouter ces visualisations :"
echo ""
echo "   [PIE CHART] Répartition des sentiments"
echo "      → Field: sentiment, Split slices"
echo ""
echo "   [TAG CLOUD] Hashtags les plus utilisés"
echo "      → Field: hashtags, Max: 50"
echo ""
echo "   [BAR CHART] Mots les plus fréquents"
echo "      → Field: words, Top 20"
echo ""
echo "   [LINE CHART] Tweets dans le temps"
echo "      → X-axis: timestamp (auto interval)"
echo ""
echo "   [DATA TABLE] Meilleurs tweets (par likes)"
echo "      → Filter: sentiment = POSITIF, Sort: likes desc"
echo ""
echo "   [DATA TABLE] Pires tweets (par score négatif)"
echo "      → Filter: sentiment = NÉGATIF, Sort: score asc"
