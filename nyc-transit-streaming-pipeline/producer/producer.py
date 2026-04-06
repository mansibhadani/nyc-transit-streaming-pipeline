"""
NYC MTA Kafka Producer
======================
Polls the MTA GTFS-Realtime feed every POLL_INTERVAL_SEC seconds,
parses trip updates into TransitEvent records, serialises them with
Avro + Confluent Schema Registry, and publishes to Kafka.

Run:
    python producer/producer.py

Requires:
    pip install -r requirements.txt
    .env file with credentials (see .env.example)
"""

import json
import logging
import time
import uuid
from pathlib import Path
from typing import Generator

import requests
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import (
    MessageField,
    SerializationContext,
    StringSerializer,
)
from google.transit import gtfs_realtime_pb2

import config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger(__name__)


# ── Schema Registry setup ─────────────────────────────────────────────────────

def _load_avro_schema() -> str:
    schema_path = Path(__file__).parent / "avro_schema.avsc"
    return schema_path.read_text()


def _build_serializer() -> AvroSerializer:
    sr_client = SchemaRegistryClient(
        {
            "url": config.SCHEMA_REGISTRY_URL,
            "basic.auth.user.info": f"{config.SCHEMA_REGISTRY_KEY}:{config.SCHEMA_REGISTRY_SECRET}",
        }
    )
    return AvroSerializer(
        schema_registry_client=sr_client,
        schema_str=_load_avro_schema(),
    )


# ── Kafka Producer setup ──────────────────────────────────────────────────────

def _build_producer() -> Producer:
    return Producer(
        {
            "bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS,
            "security.protocol": "SASL_SSL",
            "sasl.mechanisms": "PLAIN",
            "sasl.username": config.KAFKA_API_KEY,
            "sasl.password": config.KAFKA_API_SECRET,
            # Reliability settings
            "acks": "all",
            "retries": config.MAX_RETRIES,
            "retry.backoff.ms": 500,
            "enable.idempotence": True,
            # Performance
            "linger.ms": 5,
            "batch.size": 16384,
            "compression.type": "snappy",
        }
    )


# ── MTA feed parsing ──────────────────────────────────────────────────────────

def _fetch_feed() -> gtfs_realtime_pb2.FeedMessage:
    resp = requests.get(
        config.MTA_FEED_URL,
        headers={"x-api-key": config.MTA_API_KEY},
        timeout=15,
    )
    resp.raise_for_status()
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(resp.content)
    return feed


def _parse_events(feed: gtfs_realtime_pb2.FeedMessage) -> Generator[dict, None, None]:
    """Yield one TransitEvent dict per stop-time-update in the feed."""
    ingested_at = int(time.time() * 1000)

    for entity in feed.entity:
        if not entity.HasField("trip_update"):
            continue
        trip = entity.trip_update.trip
        route_id = trip.route_id or "UNKNOWN"

        for stu in entity.trip_update.stop_time_update:
            scheduled_ms = None
            actual_ms = None
            delay_sec = None
            status = "UNKNOWN"

            if stu.HasField("arrival"):
                arr = stu.arrival
                if arr.time:
                    actual_ms = arr.time * 1000
                if arr.delay:
                    delay_sec = arr.delay
                    status = "DELAYED" if delay_sec > 60 else "ON_TIME"
            if stu.HasField("departure"):
                dep = stu.departure
                if dep.time and scheduled_ms is None:
                    scheduled_ms = dep.time * 1000

            if scheduled_ms is None:
                scheduled_ms = ingested_at  # fallback

            yield {
                "event_id":       str(uuid.uuid4()),
                "route_id":       route_id,
                "stop_id":        stu.stop_id,
                "stop_name":      stu.stop_id,  # enriched in Spark via lookup
                "direction":      "N" if trip.direction_id == 0 else "S",
                "scheduled_time": scheduled_ms,
                "actual_time":    actual_ms,
                "delay_seconds":  delay_sec,
                "vehicle_id":     entity.trip_update.vehicle.id or None,
                "status":         status,
                "ingested_at":    ingested_at,
            }


# ── Delivery callback ─────────────────────────────────────────────────────────

def _delivery_report(err, msg):
    if err:
        log.error("Delivery failed | topic=%s partition=%s | %s",
                  msg.topic(), msg.partition(), err)
    else:
        log.debug("Delivered | topic=%s partition=%d offset=%d",
                  msg.topic(), msg.partition(), msg.offset())


# ── Main loop ─────────────────────────────────────────────────────────────────

def run():
    log.info("Starting NYC MTA Kafka producer")
    producer = _build_producer()
    avro_serializer = _build_serializer()
    string_serializer = StringSerializer("utf_8")

    poll_count = 0
    total_published = 0

    while True:
        poll_count += 1
        batch_count = 0
        try:
            feed = _fetch_feed()
            for event in _parse_events(feed):
                topic = (
                    config.TOPIC_ALERTS
                    if event["status"] == "CANCELLED"
                    else config.TOPIC_DELAYS
                    if event["status"] == "DELAYED"
                    else config.TOPIC_RAW
                )
                producer.produce(
                    topic=topic,
                    key=string_serializer(event["route_id"]),   # partition by route
                    value=avro_serializer(
                        event,
                        SerializationContext(topic, MessageField.VALUE),
                    ),
                    on_delivery=_delivery_report,
                )
                batch_count += 1

            producer.poll(0)  # trigger delivery callbacks
            producer.flush(timeout=10)
            total_published += batch_count
            log.info(
                "Poll #%d complete | batch=%d events | total=%d",
                poll_count, batch_count, total_published,
            )
        except requests.RequestException as exc:
            log.error("MTA feed fetch failed: %s — retrying next poll", exc)
        except Exception as exc:  # noqa: BLE001
            log.exception("Unexpected error in poll #%d: %s", poll_count, exc)

        time.sleep(config.POLL_INTERVAL_SEC)


if __name__ == "__main__":
    run()
