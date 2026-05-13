#!/usr/bin/env python3
"""
sync_ia.py — Génération automatique des points essentiels via Claude API
Exécuté par GitHub Actions toutes les 4h (6h, 10h, 14h, 18h).

Pour chaque événement actif ayant des articles liés, le script :
1. Récupère les 15 derniers articles
2. Appelle l'API Claude pour générer 6 points de synthèse
3. Remplace les anciens points IA dans Firestore (conserve les points manuels)

Variables d'environnement requises :
  FIREBASE_CREDENTIALS  — JSON du service account Firebase
  ANTHROPIC_API_KEY     — Clé API Claude (sk-ant-...)
"""

import os
import sys
import json
import logging
from datetime import datetime, timezone

import requests
import firebase_admin
from firebase_admin import credentials, firestore

# ── Config ───────────────────────────────────────────────────────────
CLAUDE_MODEL = "claude-haiku-4-5-20251001"
MAX_ARTICLES = 15
NUM_POINTS = 6

# ── Logging ──────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger("sync_ia")

# ── Firebase init ────────────────────────────────────────────────────
def init_firebase():
    cred_json = os.environ.get("FIREBASE_CREDENTIALS")
    if not cred_json:
        log.error("FIREBASE_CREDENTIALS non défini")
        sys.exit(1)
    cred_dict = json.loads(cred_json)
    cred = credentials.Certificate(cred_dict)
    firebase_admin.initialize_app(cred)
    return firestore.client()

# ── Claude API ───────────────────────────────────────────────────────
def call_claude(api_key, articles, event_title, context=""):
    """Appelle l'API Claude pour générer les points essentiels."""

    articles_list = "\n".join(
        f"{i+1}. {a.get('title','')} — {a.get('author','')} ({a.get('date','')})"
        for i, a in enumerate(articles)
    )

    prompt = f"""Tu es un journaliste senior spécialisé dans l'industrie du tourisme français pour TourMaG.com, le média de référence des professionnels du tourisme.

Voici les {len(articles)} articles les plus récents liés à l'événement « {event_title} » :

{articles_list}

{f"Contexte éditorial : {context}" if context else ""}

À partir de ces articles, génère exactement {NUM_POINTS} POINTS DE SYNTHÈSE qui résument les tendances clés, annonces majeures et faits marquants pour les professionnels du tourisme (agents de voyages, tour-opérateurs, DMC).

RÈGLES STRICTES :
- Chaque point doit avoir un TAG thématique court en MAJUSCULES (1-2 mots max, ex: DISTRIBUTION, AÉRIEN, HÔTELLERIE, CROISIÈRE, TECH, DESTINATIONS, ÉCONOMIE, DURABLE)
- Chaque point fait 1-2 phrases percutantes, factuelles et informatives
- Les points doivent couvrir des thématiques variées
- Style TourMaG : professionnel, concis, orienté business/trade
- Ne pas inventer de chiffres ou de faits non présents dans les articles

Réponds UNIQUEMENT avec un tableau JSON, sans backticks, sans texte avant ou après :
[
  {{"tag": "AÉRIEN", "text": "Le texte du point..."}},
  {{"tag": "DISTRIBUTION", "text": "Le texte du point..."}}
]"""

    log.info(f"   → Appel API Claude ({CLAUDE_MODEL})...")

    response = requests.post(
        "https://api.anthropic.com/v1/messages",
        headers={
            "x-api-key": api_key,
            "anthropic-version": "2023-06-01",
            "content-type": "application/json"
        },
        json={
            "model": CLAUDE_MODEL,
            "max_tokens": 2000,
            "messages": [{"role": "user", "content": prompt}]
        },
        timeout=60
    )

    if response.status_code != 200:
        log.error(f"   ✗ API Claude erreur {response.status_code}: {response.text[:300]}")
        return None

    data = response.json()
    text = ""
    for block in data.get("content", []):
        if block.get("type") == "text":
            text += block.get("text", "")

    # Parser le JSON
    text = text.strip()
    # Nettoyer les éventuels backticks
    if text.startswith("```"):
        text = text.split("\n", 1)[-1]
    if text.endswith("```"):
        text = text.rsplit("```", 1)[0]
    text = text.strip()

    try:
        points = json.loads(text)
        if not isinstance(points, list):
            log.error(f"   ✗ Réponse n'est pas un tableau JSON")
            return None
        log.info(f"   ✓ {len(points)} points générés")
        return points
    except json.JSONDecodeError as e:
        log.error(f"   ✗ JSON invalide: {e}")
        log.debug(f"   Réponse brute: {text[:500]}")
        return None

# ── Traitement d'un événement ────────────────────────────────────────
def process_event(db, api_key, event_ref, event_data):
    event_id = event_ref.id
    event_title = event_data.get('title', event_id)
    context = event_data.get('ia_context', '')

    log.info(f"🧠 Événement « {event_title} »")

    # Récupérer les articles
    articles_snap = event_ref.collection('articles').stream()
    articles = []
    for doc in articles_snap:
        articles.append(doc.to_dict())

    if not articles:
        log.info(f"   ⚠ Aucun article — skip")
        return False

    # Trier par date (les plus récents d'abord) puis limiter
    articles = articles[:MAX_ARTICLES]
    log.info(f"   {len(articles)} article(s) à analyser")

    # Appeler Claude
    points = call_claude(api_key, articles, event_title, context)
    if not points:
        return False

    # Supprimer les anciens points IA (garder les manuels)
    essential_ref = event_ref.collection('essential')
    old_ia = essential_ref.where('source', '==', 'ia').stream()
    batch = db.batch()
    old_count = 0
    for doc in old_ia:
        batch.delete(doc.reference)
        old_count += 1
    if old_count:
        batch.commit()
        log.info(f"   🗑 {old_count} anciens points IA supprimés")

    # Écrire les nouveaux points IA
    now = firestore.SERVER_TIMESTAMP
    for i, pt in enumerate(points):
        tag = pt.get('tag', 'GÉNÉRAL')
        text = pt.get('text', '')
        if not text:
            continue
        essential_ref.add({
            'tag': tag.upper().strip(),
            'text': text.strip(),
            'source': 'ia',
            'order': i,
            'generated_at': now
        })

    log.info(f"   ✓ {len(points)} nouveaux points IA écrits")
    return True

# ── Main ─────────────────────────────────────────────────────────────
def main():
    log.info("═══ sync_ia.py — Génération IA TourMaG Live ═══")

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        log.error("ANTHROPIC_API_KEY non défini")
        sys.exit(1)

    db = init_firebase()

    # Récupérer tous les événements actifs
    events = db.collection('events').stream()
    processed = 0
    success = 0

    for doc in events:
        data = doc.to_dict()
        status = data.get('status', '')

        # Uniquement les événements live ou upcoming
        if status == 'ended':
            continue

        processed += 1
        if process_event(db, api_key, doc.reference, data):
            success += 1

    log.info(f"═══ Terminé — {success}/{processed} événement(s) traité(s) ═══")

    # GitHub Actions summary
    summary_file = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary_file:
        with open(summary_file, "a") as f:
            f.write(f"## 🧠 Sync IA\n")
            f.write(f"- **{processed}** événement(s) actif(s)\n")
            f.write(f"- **{success}** synthèse(s) générée(s)\n")
            f.write(f"- Modèle : `{CLAUDE_MODEL}`\n")
            f.write(f"- Exécuté le {datetime.now(timezone.utc).strftime('%d/%m/%Y à %H:%M UTC')}\n")

if __name__ == '__main__':
    main()
