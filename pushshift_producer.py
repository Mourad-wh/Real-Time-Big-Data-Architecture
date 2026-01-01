import feedparser
import requests
from kafka import KafkaProducer
import time
import json
import sqlite3
import logging

# ================================
# 1️⃣ Logging
# ================================
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# ================================
# 2️⃣ Kafka Producer simple (non SASL)
# ================================
producer = KafkaProducer(
    bootstrap_servers='192.168.11.108:9092',  # ton serveur Kafka
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    linger_ms=500
)
topic_name = 'rss_hn_feed'

# ================================
# 3️⃣ RSS Feeds avec catégorie
# ================================
rss_feeds = {
    "Actualité": [
        "https://www.lemonde.fr/rss/une.xml",
        "https://www.france24.com/fr/rss",
        "https://www.reutersagency.com/feed/?best-topics=business-finance&post_type=best",
        "https://feeds.bbci.co.uk/news/rss.xml",
    ],
    "Technologie": [
        "https://rss.nytimes.com/services/xml/rss/nyt/Technology.xml",
        "https://www.theverge.com/rss/index.xml",
        "https://feeds.arstechnica.com/arstechnica/technology",
        "https://techcrunch.com/feed/",
    ],
    "Finance": [
        "https://www.ft.com/?format=rss",
        "https://www.bloomberg.com/feed/podcast/etf-report.xml",
    ],
    "Science": [
        "https://www.sciencemag.org/rss/news_current.xml",
        "https://www.nature.com/nature.rss",
    ],
    "Startups": [
        "https://www.startupmagazine.fr/feed/",
        "https://www.eu-startups.com/feed/",
    ]
}

# ================================
# 4️⃣ HackerNews
# ================================
hn_newstories_url = "https://hacker-news.firebaseio.com/v0/newstories.json"
hn_item_url = "https://hacker-news.firebaseio.com/v0/item/{}.json"
hn_batch_size = 200

# ================================
# 5️⃣ SQLite DB pour doublons
# ================================
db_file = "sent_articles.db"
conn = sqlite3.connect(db_file)
cursor = conn.cursor()
cursor.execute("""
CREATE TABLE IF NOT EXISTS sent_articles (
    id TEXT PRIMARY KEY,
    source TEXT,
    title TEXT,
    link TEXT,
    timestamp INTEGER
)
""")
conn.commit()

def is_sent(article_id):
    cursor.execute("SELECT 1 FROM sent_articles WHERE id=?", (article_id,))
    return cursor.fetchone() is not None

def mark_as_sent(article_id, source, title, link):
    cursor.execute(
        "INSERT OR IGNORE INTO sent_articles (id, source, title, link, timestamp) VALUES (?, ?, ?, ?, ?)",
        (article_id, source, title, link, int(time.time()))
    )
    conn.commit()

# ================================
# 6️⃣ Pré-catégorisation basée sur mots-clés
# ================================
category_keywords = {
    "Actualité": ["actualité", "france", "monde", "news"],
    "Technologie": ["tech", "ai", "machine learning", "software", "startup", "innovation"],
    "Finance": ["finance", "stock", "market", "business", "deal", "investment"],
    "Science": ["science", "research", "space", "physics", "biology", "covid", "medical"],
    "Startups": ["startup", "innovation", "entrepreneur", "founder"]
}

def assign_category(text, default="General"):
    text_lower = text.lower()
    for cat, keywords in category_keywords.items():
        if any(k in text_lower for k in keywords):
            return cat
    return default

# ================================
# 7️⃣ Enrichissement article
# ================================
def enrich_article(entry, feed_category="General", source="rss"):
    title = entry.get('title', '')
    summary = entry.get('summary', '')
    text_combined = f"{title} {summary}".replace("\n", " ").replace("\r", " ").strip()
    category = assign_category(text_combined, default=feed_category)
    article_id = entry.get('id', entry.get('link'))
    
    return {
        "source": source,
        "id": article_id,
        "title": title,
        "link": entry.get('link', ''),
        "published": entry.get('published', None),
        "summary": summary,
        "text_combined": text_combined,
        "category": category,
        "fetch_timestamp": int(time.time())
    }

# ================================
# 8️⃣ Envoi Kafka
# ================================
def send_to_kafka(message):
    try:
        producer.send(topic_name, value=message)
    except Exception as e:
        logging.warning(f"Kafka error: {e}")

# ================================
# 9️⃣ Boucle principale
# ================================
logging.info("🚀 Streaming RSS + HackerNews en cours...")

while True:
    # ----- RSS FEEDS -----
    for feed_category, urls in rss_feeds.items():
        for rss_url in urls:
            try:
                feed = feedparser.parse(rss_url)
                for entry in feed.entries:
                    article = enrich_article(entry, feed_category=feed_category, source="rss")
                    if not is_sent(article['id']):
                        send_to_kafka(article)
                        logging.info(f"📩 RSS Envoyé [{article['category']}] → {article['title']}")
                        mark_as_sent(article['id'], "rss", article['title'], article['link'])
            except Exception as e:
                logging.warning(f"Erreur RSS ({rss_url}): {e}")

    # ----- HackerNews -----
    try:
        new_ids = requests.get(hn_newstories_url, timeout=5).json()[:hn_batch_size]
        for item_id in new_ids:
            if not is_sent(str(item_id)):
                try:
                    item = requests.get(hn_item_url.format(item_id), timeout=5).json()
                    if item:
                        article = {
                            "source": "hn",
                            "id": str(item.get("id")),
                            "title": item.get("title", ""),
                            "link": item.get("url", ""),
                            "by": item.get("by", ""),
                            "score": item.get("score", 0),
                            "time": item.get("time", 0),
                            "type": item.get("type"),
                            "text_combined": item.get("title", ""),
                            "category": assign_category(item.get("title", ""), default="HN"),
                            "fetch_timestamp": int(time.time())
                        }
                        send_to_kafka(article)
                        logging.info(f"📩 HN Envoyé [{article['category']}] → {article['title']}")
                        mark_as_sent(str(item_id), "hn", article['title'], article['link'])
                except Exception as e:
                    logging.warning(f"Erreur item HN: {e}")
    except Exception as e:
        logging.warning(f"Erreur récupération NEW STORIES: {e}")

    # ----- Flush Kafka -----
    producer.flush()

    # ----- Pause -----
    time.sleep(30)
