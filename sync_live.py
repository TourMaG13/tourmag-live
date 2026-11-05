#!/usr/bin/env python3
"""
sync_live.py — Génération IA automatique pour TourMaG Live
Exécuté par GitHub Actions (cron toutes les 4h).

Pour chaque événement actif (status == 'live') :
1. Si iaEssential activé → génère les points essentiels via Claude
2. Si iaTimeline activé → génère la timeline via Claude
3. Import RSS automatique des flux configurés

Nécessite :
- FIREBASE_SERVICE_ACCOUNT (secret GitHub)
- ANTHROPIC_API_KEY (secret GitHub)
"""

import os
import sys
import json
import time
import re
import unicodedata
from datetime import datetime

import requests
import feedparser
import firebase_admin
from firebase_admin import credentials, firestore
from bs4 import BeautifulSoup

# ============================================================
# CONFIG
# ============================================================
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
ANTHROPIC_MODEL = "claude-sonnet-4-6"
MAX_TOKENS = 2000
AI_PAUSE = 3  # secondes entre appels API

# ============================================================
# FIREBASE INIT
# ============================================================
def init_firebase():
    sa_json = os.environ.get("FIREBASE_SERVICE_ACCOUNT", "")
    if not sa_json:
        print("❌ FIREBASE_SERVICE_ACCOUNT manquant", flush=True)
        sys.exit(1)
    sa = json.loads(sa_json)
    cred = credentials.Certificate(sa)
    firebase_admin.initialize_app(cred)
    return firestore.client()

# ============================================================
# ANTHROPIC API CALL
# ============================================================
def call_claude(prompt, retries=3):
    """Appelle Claude via l'API Anthropic et retourne le texte."""
    if not ANTHROPIC_API_KEY:
        print("  ⚠️ ANTHROPIC_API_KEY manquante, skip IA", flush=True)
        return None
    for attempt in range(retries):
        try:
            r = requests.post(
                "https://api.anthropic.com/v1/messages",
                headers={
                    "x-api-key": ANTHROPIC_API_KEY,
                    "anthropic-version": "2023-06-01",
                    "Content-Type": "application/json"
                },
                json={
                    "model": ANTHROPIC_MODEL,
                    "max_tokens": MAX_TOKENS,
                    "messages": [{"role": "user", "content": prompt}]
                },
                timeout=60
            )
            if r.status_code == 429:
                wait = max(int(r.headers.get("Retry-After", 30)), 30)
                print(f"  429 rate limit — attente {wait}s", flush=True)
                time.sleep(wait)
                continue
            r.raise_for_status()
            data = r.json()
            text = "".join(c.get("text", "") for c in data.get("content", []))
            return text
        except Exception as e:
            print(f"  ❌ Claude ERR ({attempt+1}/{retries}): {e}", flush=True)
            if attempt < retries - 1:
                time.sleep(10)
    return None

def parse_json_response(text):
    """Parse une réponse JSON, en nettoyant les backticks si présentes."""
    if not text:
        return None
    clean = re.sub(r'^```(?:json)?\s*', '', text.strip())
    clean = re.sub(r'\s*```$', '', clean)
    try:
        return json.loads(clean)
    except json.JSONDecodeError as e:
        print(f"  ❌ JSON parse error: {e}", flush=True)
        print(f"     Réponse brute: {text[:300]}", flush=True)
        return None

# ============================================================
# PARSING DE DATES (champ texte « date » des articles / jalons)
# ============================================================
# Table mois FR + EN. Clé = token normalisé (minuscule, sans accent, sans point).
_MONTHS = {}
for _names, _num in [
    (["janv", "jan", "janvier", "january"], 1),
    (["fevr", "fev", "fevrier", "feb", "february"], 2),
    (["mars", "mar", "march"], 3),
    (["avr", "avril", "apr", "april"], 4),
    (["mai", "may"], 5),
    (["juin", "jun", "june"], 6),           # juin ≠ juillet : on garde les tokens entiers
    (["juil", "juillet", "jul", "july"], 7),
    (["aout", "aou", "aug", "august"], 8),
    (["sept", "sep", "septembre", "september"], 9),
    (["oct", "octobre", "october"], 10),
    (["nov", "novembre", "november"], 11),
    (["dec", "decembre", "december"], 12),
]:
    for _n in _names:
        _MONTHS[_n] = _num

def _norm_token(s):
    s = unicodedata.normalize("NFKD", s or "")
    s = "".join(c for c in s if not unicodedata.combining(c))
    return s.lower().strip().strip(".")

def parse_fr_date(s):
    """Parse une date texte type « 27 nov. 2025 » (FR ou EN) → datetime, ou None.
    Sert au tri chronologique quand le champ Firestore created_at est absent."""
    if not s:
        return None
    m = re.search(r"(\d{1,2})\s+([^\s\d]+)\.?\s+(\d{4})", str(s))
    if not m:
        return None
    day = int(m.group(1))
    year = int(m.group(3))
    tok = _norm_token(m.group(2))
    num = _MONTHS.get(tok)
    if num is None:
        # repli : préfixe exact (évite l'ambiguïté juin/juil car ces tokens sont entiers)
        for k, v in _MONTHS.items():
            if tok == k:
                num = v
                break
    if num is None:
        return None
    try:
        return datetime(year, num, day)
    except Exception:
        return None

# ============================================================
# COLLECTE DE DONNÉES CONTEXTUELLES
# ============================================================
def collect_context(db, event_ref, event_data):
    """Récupère les données de l'événement pour construire le prompt IA."""
    ctx = {
        "title": event_data.get("title", ""),
        "location": event_data.get("location", ""),
        "dates": event_data.get("dates", ""),
        "live_texts": [],
        "article_titles": [],
        "article_count": 0,
        "essential_texts": [],
        "timeline_texts": []
    }

    # Fil live (30 dernières entrées)
    live_docs = event_ref.collection("live").order_by("time", direction=firestore.Query.DESCENDING).limit(30).stream()
    for doc in live_docs:
        d = doc.to_dict()
        text = re.sub(r'<[^>]+>', '', d.get("text", ""))
        ts = ""
        t = d.get("time")
        if t:
            try:
                dt = t if isinstance(t, datetime) else t.timestamp()
                ts = t.strftime("%d %b %H:%M") if hasattr(t, 'strftime') else ""
            except:
                pass
        ctx["live_texts"].append(f"[{ts}] {text}")

    # Articles liés : on récupère TOUS les articles puis on les trie côté Python par
    # created_at décroissant. On évite order_by("created_at") côté Firestore car cette
    # requête EXCLUT silencieusement les documents dépourvus du champ created_at (un
    # article ajouté/importé par une ancienne version ou touché à la main deviendrait
    # alors invisible pour la timeline, alors qu'il s'affiche dans le dashboard).
    def _art_ts(doc):
        d = doc.to_dict()
        ca = d.get("created_at")
        try:
            if hasattr(ca, "timestamp"):
                return ca.timestamp()
        except Exception:
            pass
        # Pas de created_at exploitable → on retombe sur le champ texte « date »
        pd = parse_fr_date(d.get("date", ""))
        if pd:
            return pd.timestamp()
        return 0.0
    all_art = list(event_ref.collection("articles").stream())
    all_art.sort(key=_art_ts, reverse=True)       # plus récent d'abord (date réelle)
    art_docs = all_art[:10][::-1]                  # 10 plus récents, remis en ordre croissant
    ctx["article_count"] = len(all_art)            # total réel, pour le diagnostic
    for doc in art_docs:
        d = doc.to_dict()
        ctx["article_titles"].append(f"- {d.get('title', '')} ({d.get('date', '')})")

    # Points essentiels existants
    ess_docs = event_ref.collection("essential").stream()
    for doc in ess_docs:
        d = doc.to_dict()
        tag = d.get("tag", "")
        text = re.sub(r'<[^>]+>', '', d.get("text", ""))
        ctx["essential_texts"].append(f"[{tag}] {text}")

    # Timeline existante
    tl_docs = event_ref.collection("timeline").order_by("order").stream()
    for doc in tl_docs:
        d = doc.to_dict()
        ctx["timeline_texts"].append(f"{d.get('date', '')}: {d.get('text', '')}")

    return ctx

# ============================================================
# GÉNÉRATION DES POINTS ESSENTIELS
# ============================================================
def generate_essential(db, event_ref, event_data, ctx):
    """Génère les points essentiels via IA et les écrit dans Firestore."""
    tags = event_data.get("tags", [])
    # Construire la liste de tags disponibles
    tag_names = []
    for t in tags:
        if isinstance(t, str):
            tag_names.append(t)
        elif isinstance(t, dict) and t.get("name"):
            tag_names.append(t["name"])

    live_block = "\n".join(ctx["live_texts"][:30]) or "(vide)"
    articles_block = "\n".join(ctx["article_titles"][:20]) or "(vide)"

    # Les deux sources (articles + posts du fil live) sont synthétisées ensemble.
    has_live = bool(ctx["live_texts"])
    has_articles = bool(ctx["article_titles"])
    sources_desc = []
    if has_articles:
        sources_desc.append("les articles liés")
    if has_live:
        sources_desc.append("les posts publiés dans le fil live")
    sources_line = " et ".join(sources_desc) if sources_desc else "les contenus disponibles"

    prompt = f"""Tu es un éditeur spécialisé dans le tourisme français pour TourMaG.com.
À partir de {sources_line} de l'événement "{ctx['title']}" ({ctx['location']}, {ctx['dates']}), génère 4 à 6 points essentiels de synthèse.

Les DEUX sources ci-dessous ont la même importance : traite les posts du fil live comme une matière première au même titre que les articles. Un point de synthèse peut provenir d'un article, d'un post du fil live, ou combiner les deux.

--- ARTICLES LIÉS ---
{articles_block}

--- POSTS DU FIL LIVE ---
{live_block}

Tags disponibles : {', '.join(tag_names)}

Chaque point doit :
- Avoir un tag parmi ceux disponibles
- Synthétiser un fait marquant issu des articles OU des posts du fil live
- Être bref et percutant (1-2 phrases, max 30 mots)
- Utiliser <strong>nom propre</strong> pour les entités clés
- Couvrir des aspects différents
- Ne pas inventer de faits ou de chiffres absents des deux sources

Réponds UNIQUEMENT en JSON, sans backticks :
[{{"tag":"TAG","text":"Texte avec <strong>entité</strong> en gras."}}, ...]"""

    print("  🤖 Génération des points essentiels...", flush=True)
    response = call_claude(prompt)
    points = parse_json_response(response)
    if not points or not isinstance(points, list):
        print("  ❌ Pas de points générés", flush=True)
        return 0

    # Supprimer les anciens points IA
    old_docs = event_ref.collection("essential").where("source", "==", "ia").stream()
    batch = db.batch()
    count_deleted = 0
    for doc in old_docs:
        batch.delete(doc.reference)
        count_deleted += 1
    if count_deleted:
        batch.commit()
        print(f"  🗑 {count_deleted} anciens points IA supprimés", flush=True)

    # Écrire les nouveaux
    for i, p in enumerate(points):
        event_ref.collection("essential").add({
            "tag": p.get("tag", "GÉNÉRAL"),
            "text": p.get("text", ""),
            "source": "ia",
            "order": i,
            "generated_at": firestore.SERVER_TIMESTAMP
        })

    print(f"  ✅ {len(points)} points essentiels IA générés", flush=True)
    return len(points)

# ============================================================
# GÉNÉRATION DE LA TIMELINE
# ============================================================
def generate_timeline(db, event_ref, event_data, ctx):
    """Génère la timeline via IA et écrit dans Firestore."""
    live_block = "\n".join(ctx["live_texts"][:30]) or "(vide)"
    articles_block = "\n".join(ctx["article_titles"][:20]) or "(vide)"
    essential_block = "\n".join(ctx["essential_texts"]) or "(vide)"

    prompt = f"""Tu es un éditeur spécialisé dans le tourisme français pour TourMaG.com.
À partir UNIQUEMENT des articles ci-dessous, crée une frise chronologique pour l'événement "{ctx['title']}" ({ctx['location']}, {ctx['dates']}).

RÈGLES STRICTES :
- Les articles sont fournis en ORDRE CHRONOLOGIQUE CROISSANT (du plus ancien au plus récent)
- Ne génère des jalons QUE pour des faits déjà survenus et mentionnés dans les articles
- N'anticipe JAMAIS des événements futurs, même si les dates sont connues
- N'invente aucune information qui n'est pas dans les articles
- Chaque jalon doit correspondre à un fait précis issu d'un article
- RÉCENCE EXCLUSIVE : utilise UNIQUEMENT les articles les plus récents (le bas de la liste). Le dernier jalon doit être l'article le plus récent. Ignore complètement les articles anciens, même s'ils semblent fondateurs ou importants.

--- ARTICLES (du plus ancien au plus récent) ---
{articles_block}

{f"--- POINTS ESSENTIELS (contexte) ---{chr(10)}{essential_block}" if ctx["essential_texts"] else ""}

Génère EXACTEMENT 6 jalons chronologiques basés UNIQUEMENT sur les faits des articles. Pas 5, pas 7 : 6. Chaque jalon :
- date courte (ex: "16 avr.", "11 mai", "26 juin")
- texte bref (max 20 mots) résumant le fait tel que rapporté dans l'article

Réponds UNIQUEMENT en JSON, sans backticks, classés du plus ancien au plus récent :
[{{"date":"16 avr.","text":"Annonce officielle du Forum des Pionniers 2026."}}, ...]"""

    print("  🤖 Génération de la timeline...", flush=True)
    response = call_claude(prompt)
    jalons = parse_json_response(response)
    if not jalons or not isinstance(jalons, list):
        print("  ❌ Pas de jalons générés", flush=True)
        return 0

    # Tri chronologique de sécurité : si TOUTES les dates des jalons sont lisibles,
    # on réordonne du plus ancien au plus récent. L'IA peut se tromper d'ordre ;
    # ça garantit un affichage chronologique et un « 6 plus récents » correct.
    _parsed = [parse_fr_date(j.get("date", "")) for j in jalons]
    if jalons and all(_parsed):
        jalons = [j for _, j in sorted(zip(_parsed, jalons), key=lambda p: p[0])]

    # Garantir exactement 6 jalons : tronquer aux 6 plus récents si trop,
    # avertir si moins (l'IA n'a pas suivi l'instruction)
    if len(jalons) > 6:
        print(f"  ⚠ {len(jalons)} jalons reçus, on garde les 6 plus récents", flush=True)
        jalons = jalons[-6:]  # les derniers de la liste = les plus récents (ordre chronologique croissant)
    elif len(jalons) < 6:
        print(f"  ⚠ Seulement {len(jalons)} jalons reçus (6 attendus)", flush=True)

    # Supprimer les anciens jalons IA
    old_docs = event_ref.collection("timeline").where("source", "==", "ia").stream()
    batch = db.batch()
    count_deleted = 0
    for doc in old_docs:
        batch.delete(doc.reference)
        count_deleted += 1
    if count_deleted:
        batch.commit()
        print(f"  🗑 {count_deleted} anciens jalons IA supprimés", flush=True)

    # Écrire les nouveaux
    for i, j in enumerate(jalons):
        event_ref.collection("timeline").add({
            "date": j.get("date", ""),
            "text": j.get("text", ""),
            "source": "ia",
            "order": i,
            "generated_at": firestore.SERVER_TIMESTAMP
        })

    print(f"  ✅ {len(jalons)} jalons timeline IA générés", flush=True)
    return len(jalons)

# ============================================================
# IMPORT RSS AUTOMATIQUE
# ============================================================
def import_rss(db, event_ref, event_data):
    """Importe les articles depuis les flux RSS configurés."""
    feeds = event_data.get("rssFeeds", [])
    if not feeds:
        return 0

    # Récupérer les liens existants pour dédoublonnage
    existing_links = set()
    for doc in event_ref.collection("articles").stream():
        link = doc.to_dict().get("link", "")
        if link:
            existing_links.add(link)

    imported = 0
    for feed_cfg in feeds:
        url = feed_cfg.get("url", "")
        label = feed_cfg.get("label", "RSS")
        if not url:
            continue

        print(f"  📡 RSS: {url}", flush=True)
        try:
            parsed = feedparser.parse(url)
            for entry in parsed.entries:
                title = getattr(entry, "title", "").strip()
                link = getattr(entry, "link", "").strip()
                if not title or not link or link in existing_links:
                    continue

                # Auteur
                author = getattr(entry, "author", "") or label

                # Date
                date_str = ""
                date_ts = None
                pub = getattr(entry, "published_parsed", None) or getattr(entry, "updated_parsed", None)
                if pub:
                    try:
                        dt = datetime(*pub[:6])
                        date_str = dt.strftime("%-d %b %Y")
                        date_ts = dt
                    except:
                        pass

                # Image : chercher "imagette" d'abord, puis photo:imgsrc, enclosures, puis og:image
                image = ""
                # Chercher dans le contenu brut
                raw_content = ""
                if hasattr(entry, "content"):
                    for c in entry.content:
                        raw_content += c.get("value", "")
                raw_content += getattr(entry, "summary", "")
                raw_content += getattr(entry, "description", "")

                # Priorité 1 : URL contenant "imagette"
                imagette_match = re.search(r'https?://[^"\'\s<>]*imagette[^"\'\s<>]*', raw_content, re.IGNORECASE)
                if imagette_match:
                    image = imagette_match.group(0)

                # Priorité 2 : balise photo:imgsrc (TourMaG custom RSS tag)
                if not image:
                    # feedparser expose les tags custom dans le namespace
                    photo_imgsrc = getattr(entry, "photo_imgsrc", None)
                    if photo_imgsrc:
                        image = photo_imgsrc.strip()
                    # Aussi chercher dans le raw XML via regex sur la description/summary
                    if not image:
                        photo_match = re.search(r'<photo:imgsrc[^>]*>([^<]+)</photo:imgsrc>', raw_content, re.IGNORECASE)
                        if photo_match:
                            image = photo_match.group(1).strip()

                # Enclosures
                if not image:
                    for enc in getattr(entry, "enclosures", []):
                        eu = enc.get("href", "") or enc.get("url", "")
                        if eu and re.search(r'\.(jpe?g|png|webp|gif)', eu, re.IGNORECASE):
                            image = eu
                            break

                # media_content
                if not image:
                    for mc in getattr(entry, "media_content", []):
                        mu = mc.get("url", "")
                        if mu:
                            image = mu
                            break

                # media_thumbnail
                if not image:
                    for mt in getattr(entry, "media_thumbnail", []):
                        mu = mt.get("url", "")
                        if mu:
                            image = mu
                            break

                # <img> dans le contenu
                if not image:
                    img_match = re.search(r'<img[^>]+src=["\']([^"\']+)', raw_content, re.IGNORECASE)
                    if img_match:
                        image = img_match.group(1)

                # Fallback : og:image depuis la page
                if not image and link:
                    try:
                        resp = requests.get(link, timeout=8, headers={"User-Agent": "TourMaG-Live-Bot/1.0"})
                        if resp.ok:
                            soup = BeautifulSoup(resp.text, "html.parser")
                            og = soup.find("meta", property="og:image")
                            if og and og.get("content"):
                                image = og["content"]
                    except:
                        pass

                # Sauvegarder
                article_data = {
                    "title": title,
                    "link": link,
                    "author": author,
                    "image": image,
                    "date": date_str or datetime.now().strftime("%-d %b %Y"),
                    "source": "rss"
                }
                if date_ts:
                    article_data["created_at"] = date_ts
                else:
                    article_data["created_at"] = firestore.SERVER_TIMESTAMP

                event_ref.collection("articles").add(article_data)
                existing_links.add(link)
                imported += 1

        except Exception as e:
            print(f"  ❌ RSS error {url}: {e}", flush=True)

    if imported:
        print(f"  ✅ {imported} articles RSS importés", flush=True)
    return imported

# ============================================================
# MAIN
# ============================================================
def main():
    print("=" * 60, flush=True)
    print(f"🚀 sync_live.py — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", flush=True)
    print("=" * 60, flush=True)

    db = init_firebase()

    # Récupérer TOUS les événements (pas seulement live)
    events = db.collection("events").stream()
    event_list = [(doc.id, doc.to_dict()) for doc in events]

    if not event_list:
        print("ℹ️ Aucun événement trouvé", flush=True)
        return

    for event_id, event_data in event_list:
        has_rss = bool(event_data.get("rssFeeds"))
        # Champ absent (None) = activé par défaut, comme l'admin (ev.iaX !== false).
        # Seule une valeur explicitement "fausse" désactive la génération.
        has_ia_ess = event_data.get("iaEssential") not in [False, "false", 0, "0"]
        has_ia_tl = event_data.get("iaTimeline") not in [False, "false", 0, "0"]

        # Skip events with nothing to do
        if not has_rss and not has_ia_ess and not has_ia_tl:
            continue

        print(f"\n📡 Événement: {event_data.get('title', event_id)} (status: {event_data.get('status','?')})", flush=True)
        event_ref = db.collection("events").document(event_id)

        # 1. Import RSS
        rss_count = 0
        if has_rss:
            rss_count = import_rss(db, event_ref, event_data)

        # Collecter le contexte (après import RSS pour inclure les nouveaux articles)
        ctx = collect_context(db, event_ref, event_data)

        # 2. Points essentiels IA (synthèse des articles ET des posts du fil live)
        if has_ia_ess:
            if ctx["article_titles"] or ctx["live_texts"]:
                ess_count = generate_essential(db, event_ref, event_data, ctx)
                if ess_count:
                    time.sleep(AI_PAUSE)
                    ctx = collect_context(db, event_ref, event_data)
            else:
                print("  ⏭ Aucun article ni post live à synthétiser", flush=True)
        else:
            print("  ⏭ IA Essentiel désactivée", flush=True)

        # 3. Timeline IA
        if has_ia_tl:
            if ctx["article_titles"]:
                generate_timeline(db, event_ref, event_data, ctx)
            else:
                print(f"  ⏭ Aucun article pour la timeline (0 article dans la collection 'articles' de cet événement ; total vu = {ctx.get('article_count', 0)})", flush=True)
        else:
            print("  ⏭ IA Timeline désactivée", flush=True)

    print(f"\n✅ Terminé — {datetime.now().strftime('%H:%M:%S')}", flush=True)

if __name__ == "__main__":
    main()
