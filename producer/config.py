"""
Central configuration — all secrets come from environment variables.
Copy .env.example to .env and fill in your values. Never commit .env.
"""
import os
from dotenv import load_dotenv

load_dotenv()

# ── Confluent Kafka ────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS: str = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
KAFKA_API_KEY:           str = os.environ["KAFKA_API_KEY"]
KAFKA_API_SECRET:        str = os.environ["KAFKA_API_SECRET"]
SCHEMA_REGISTRY_URL:     str = os.environ["SCHEMA_REGISTRY_URL"]
SCHEMA_REGISTRY_KEY:     str = os.environ["SCHEMA_REGISTRY_KEY"]
SCHEMA_REGISTRY_SECRET:  str = os.environ["SCHEMA_REGISTRY_SECRET"]

# ── Topics ─────────────────────────────────────────────────────────────────────
TOPIC_RAW     = "transit.raw"
TOPIC_DELAYS  = "transit.delays"
TOPIC_ALERTS  = "transit.alerts"

# ── MTA GTFS-Realtime feed ─────────────────────────────────────────────────────
MTA_API_KEY:        str = os.environ["MTA_API_KEY"]          # free at api.mta.info
MTA_FEED_URL:       str = os.getenv(
    "MTA_FEED_URL",
    "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs"
)
POLL_INTERVAL_SEC:  int = int(os.getenv("POLL_INTERVAL_SEC", "10"))

# ── Dead-letter / error handling ───────────────────────────────────────────────
TOPIC_DEAD_LETTER = "transit.dead-letter"
MAX_RETRIES:      int = int(os.getenv("MAX_RETRIES", "3"))
