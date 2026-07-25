"""
Analyse de sentiment LOCAL (fallback si Groq échoue)
"""

POSITIVE_WORDS = [
    # Français — émotions positives
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
    # Anglais — émotions positives
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
    # Français — émotions négatives
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
    # Anglais — émotions négatives
    "hate", "dislike", "worst", "terrible", "awful", "bad", "horrible",
    "broken", "useless", "ugly", "slow", "crash", "fail", "failed",
    "failure", "error", "bug", "issue", "problem", "disappointed",
    "disappointing", "frustrating", "annoying", "angry", "sad", "boring",
    "waste", "poor", "stupid", "ridiculous", "wrong", "dangerous",
    "threat", "risk", "fear", "worried", "concern", "harmful", "damage",
    "lose", "loss", "never works", "always fails", "broken", "useless",
    "pointless", "overrated", "scam", "terrible", "nightmare", "disaster"
]

def analyze_sentiment(text: str) -> tuple[str, float]:
    """
    Retourne (sentiment, score)
    - sentiment: POSITIF / NEGATIF / NEUTRE
    - score: -1.0 à 1.0
    """
    text_lower = text.lower()

    pos_count = sum(1 for w in POSITIVE_WORDS if w in text_lower)
    neg_count = sum(1 for w in NEGATIVE_WORDS if w in text_lower)

    total = pos_count + neg_count
    if total == 0:
        return "NEUTRE", 0.0

    score = (pos_count - neg_count) / total

    if score > 0.1:
        sentiment = "POSITIF"
    elif score < -0.1:
        sentiment = "NEGATIF"
    else:
        sentiment = "NEUTRE"

    return sentiment, round(score, 2)
