# ingestion/consumers/fx_consumer.py

import json
import logging
import os
import signal
from datetime import datetime, timezone
from dotenv import load_dotenv
import psycopg2
from kafka import KafkaConsumer

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

DB_CONFIG = {
    "host":     os.environ.get("POSTGRES_HOST", "localhost"),
    "port":     os.environ.get("POSTGRES_PORT", "5432"),
    "dbname":   os.environ.get("POSTGRES_DB", "lakehouse"),
    "user":     os.environ.get("POSTGRES_USER", "lakehouse_user"),
    "password": os.environ.get("POSTGRES_PASSWORD", ""),
}

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = "fx_rates"
KAFKA_GROUP_ID = "fx_consumer_group"


def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)


def create_table(conn):
    create_table_sql = """
        CREATE TABLE IF NOT EXISTS raw_fx_rates (
            id              SERIAL PRIMARY KEY,
            from_currency   VARCHAR(10)  NOT NULL,
            to_currency     VARCHAR(10)  NOT NULL,
            exchange_rate   NUMERIC(12,6),
            high            NUMERIC(12,6),
            low             NUMERIC(12,6),
            volume          BIGINT,
            last_refreshed  VARCHAR(50),
            ingested_at     TIMESTAMPTZ,
            created_at      TIMESTAMPTZ  DEFAULT NOW(),
            UNIQUE (from_currency, to_currency, last_refreshed)
        );
    """
    with conn.cursor() as cur:
        cur.execute(create_table_sql)
    conn.commit()
    logger.info("Table raw_fx_rates is ready")


def insert_record(conn, record: dict):
    insert_sql = """
        INSERT INTO raw_fx_rates (
            from_currency, to_currency, exchange_rate,
            high, low, volume, last_refreshed, ingested_at
        )
        VALUES (
            %(from_currency)s,
            %(to_currency)s,
            %(exchange_rate)s,
            %(high)s,
            %(low)s,
            %(volume)s,
            %(last_refreshed)s,
            %(ingested_at)s
        )
        ON CONFLICT DO NOTHING;
    """
    with conn.cursor() as cur:
        cur.execute(insert_sql, record)
    conn.commit()


def create_consumer() -> KafkaConsumer:
    return KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=KAFKA_GROUP_ID,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
    )


running = True

def handle_shutdown(signum, frame):
    global running
    logger.info("Shutdown signal received — stopping cleanly")
    running = False

signal.signal(signal.SIGINT, handle_shutdown)
signal.signal(signal.SIGTERM, handle_shutdown)


def run_consumer():
    logger.info("Starting FX consumer")
    conn = get_db_connection()
    create_table(conn)
    consumer = create_consumer()
    logger.info(f"Listening on topic: {KAFKA_TOPIC}")
    message_count = 0

    for message in consumer:
        if not running:
            break

        record = message.value

        try:
            insert_record(conn, record)
            message_count += 1
            logger.info(
                f"Stored {record['from_currency']}/{record['to_currency']} | "
                f"rate={record['exchange_rate']} | "
                f"offset={message.offset} | "
                f"total stored={message_count}"
            )
        except Exception as e:
            logger.error(f"Failed to insert record: {e}")
            conn.rollback()

    consumer.close()
    conn.close()
    logger.info(f"Consumer stopped. Total messages stored: {message_count}")


if __name__ == "__main__":
    run_consumer()