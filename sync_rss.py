#!/usr/bin/env python3
"""
sync_rss.py — Import automatique des flux RSS vers Firestore
Exécuté par GitHub Actions toutes les 15 minutes.

Pour chaque événement actif (status != 'ended') ayant des flux RSS configurés,
le script parse les flux, dédoublonne par URL, et ajoute les nouveaux articles
dans la sous-collection `articles` de l'événement.

Variables d'environnement requises :
  FIREBASE_CREDENTIALS  — JSON du service account Firebase (secret GitHub)
"""

import os
import sys
import json
import hashlib
import logging
from datetime import datetime, timezone

import feedparser
import firebase_admin
from firebase_admin import credentials, firestore

# ── Logging ──────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger("sync_rss")

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

# ── Extraction d'image depuis un item feedparser ─────────────────────
def extract_image(entry):
    """Cherche une image dans l'ordre : enclosure, media_content, media_thumbnail, img dans summary."""
    # Enclosures
    for enc in getattr(entry, 'enclosures', []):
        if enc.get('type', '').startswith('image'):
            return enc.get('href') or enc.get('url', '')

    # media:content
    for mc in getattr(entry, 'media_content', []):
        if mc.get('medium') == 'image' or mc.get('type', '').startswith('image'):
            return mc.get('url', '')

    # media:thumbnail
    for mt in getattr(entry, 'media_thumbnail', []):
        url = mt.get('url', '')
        if url:
            return url

    # Fallback : chercher <img> dans le summary/description
    summary = getattr(entry, 'summary', '') or getattr(entry, 'description', '')
    if summary:
        import re
        match = re.search(r'<img[^>]+src=["\']([^"\']+)', summary, re.IGNORECASE)
        if match:
            return match.group(1)

    return ''

# ── Extraction auteur ────────────────────────────────────────────────
def extract_author(entry, feed_label=''):
    """Cherche l'auteur dans l'ordre : author, dc:creator, feed label."""
    author = getattr(entry, 'author', '') or ''
    if not author:
        # feedparser met dc:creator dans author_detail ou authors
        authors = getattr(entry, 'authors', [])
        if authors and authors[0].get('name'):
            author = authors[0]['name']
    return author.strip() or feed_label or 'RSS'

# ── Date formatée ────────────────────────────────────────────────────
def format_date(entry):
    """Parse la date de publication et retourne un format lisible."""
    parsed = getattr(entry, 'published_parsed', None) or getattr(entry, 'updated_parsed', None)
    if parsed:
        try:
            dt = datetime(*parsed[:6], tzinfo=timezone.utc)
            # Format français
            months_fr = ['', 'janv.', 'févr.', 'mars', 'avr.', 'mai', 'juin',
                         'juil.', 'août', 'sept.', 'oct.', 'nov.', 'déc.']
            return f"{dt.day} {months_fr[dt.month]} {dt.year}"
        except Exception:
            pass
    return datetime.now(timezone.utc).strftime("%-d mai %Y")

# ── Traitement d'un événement ────────────────────────────────────────
def process_event(db, event_ref, event_data):
    event_id = event_ref.id
    event_title = event_data.get('title', event_id)
    rss_feeds = event_data.get('rssFeeds', [])

    if not rss_feeds:
        return 0

    log.info(f"📡 Événement « {event_title} » — {len(rss_feeds)} flux RSS")

    # Récupérer les URLs déjà en base pour dédoublonner
    articles_ref = event_ref.collection('articles')
    existing_docs = articles_ref.stream()
    existing_links = set()
    for doc in existing_docs:
        link = doc.to_dict().get('link', '')
        if link and link != '#':
            existing_links.add(link)

    log.info(f"   {len(existing_links)} articles existants en base")

    total_imported = 0

    for feed_conf in rss_feeds:
        feed_url = feed_conf.get('url', '')
        feed_label = feed_conf.get('label', '')
        if not feed_url:
            continue

        log.info(f"   → Parsing {feed_label or feed_url}")

        try:
            feed = feedparser.parse(feed_url)
            if feed.bozo and not feed.entries:
                log.warning(f"     ⚠ Feed invalide ou vide : {feed.bozo_exception}")
                continue

            new_count = 0
            for entry in feed.entries:
                link = getattr(entry, 'link', '') or ''
                title = getattr(entry, 'title', '') or ''

                if not title or not link:
                    continue
                if link in existing_links:
                    continue

                article = {
                    'title': title.strip(),
                    'link': link.strip(),
                    'author': extract_author(entry, feed_label),
                    'image': extract_image(entry),
                    'date': format_date(entry),
                    'source': 'rss',
                    'rss_feed': feed_url,
                    'imported_at': firestore.SERVER_TIMESTAMP
                }

                articles_ref.add(article)
                existing_links.add(link)
                new_count += 1
                total_imported += 1

            log.info(f"     ✓ {new_count} nouveaux articles (sur {len(feed.entries)} dans le flux)")

        except Exception as e:
            log.error(f"     ✗ Erreur sur {feed_url}: {e}")

    return total_imported

# ── Main ─────────────────────────────────────────────────────────────
def main():
    log.info("═══ sync_rss.py — Import RSS TourMaG Live ═══")
    db = init_firebase()

    # Récupérer tous les événements actifs (non terminés)
    events = db.collection('events').stream()
    total = 0
    event_count = 0

    for doc in events:
        data = doc.to_dict()
        status = data.get('status', '')

        # Ignorer les événements terminés
        if status == 'ended':
            continue

        # Ignorer les événements sans flux RSS
        if not data.get('rssFeeds'):
            continue

        event_count += 1
        imported = process_event(db, doc.reference, data)
        total += imported

    log.info(f"═══ Terminé — {total} articles importés sur {event_count} événement(s) ═══")

    # Écrire un summary pour GitHub Actions
    summary_file = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary_file:
        with open(summary_file, "a") as f:
            f.write(f"## 📡 Sync RSS\n")
            f.write(f"- **{event_count}** événement(s) traité(s)\n")
            f.write(f"- **{total}** article(s) importé(s)\n")
            f.write(f"- Exécuté le {datetime.now(timezone.utc).strftime('%d/%m/%Y à %H:%M UTC')}\n")

if __name__ == '__main__':
    main()
