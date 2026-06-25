# ingestion/consumers/ftse_consumer.py

import json                      # to parse JSON messages coming out of Kafka
import logging                   # structured logs with timestamps
import os                        # to read environment variables
import signal                    # to handle Ctrl+C gracefully
import sys                       # to exit the program cleanly
from datetime import datetime, timezone
from dotenv import load_dotenv   # reads our .env file
import psycopg2                  # PostgreSQL driver — lets Python talk to PostgreSQL
import psycopg2.extras           # extra utilities, we use execute_values for bulk inserts
from kafka import KafkaConsumer  # Kafka client — the reading side

# ── Load environment variables ───────────────────────────────────────────────
load_dotenv()

# ── Logging setup ────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

# ── PostgreSQL connection settings ───────────────────────────────────────────
# We read all values from .env so no credentials are hardcoded in the script.
# If any variable is missing from .env, we fall back to the default shown.
DB_CONFIG = {
    "host":     os.environ.get("POSTGRES_HOST", "localhost"),
    "port":     os.environ.get("POSTGRES_PORT", "5432"),
    "dbname":   os.environ.get("POSTGRES_DB", "lakehouse"),
    "user":     os.environ.get("POSTGRES_USER", "lakehouse_user"),
    "password": os.environ.get("POSTGRES_PASSWORD", ""),
}

# ── Kafka settings ───────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = "ftse_prices"

# consumer_group is a Kafka concept — it tracks which messages this consumer
# has already read, so if the consumer restarts it picks up where it left off
# rather than reading every message from the beginning again.
# Think of it as a bookmark.
KAFKA_GROUP_ID = "ftse_consumer_group"


# ── Database functions ────────────────────────────────────────────────────────

def get_db_connection():
    """
    Opens and returns a connection to PostgreSQL.
    psycopg2.connect() takes our credentials and returns a connection object.
    Think of this as opening the filing cabinet.
    """
    return psycopg2.connect(**DB_CONFIG)
    # The ** unpacks the dict: host="localhost", port="5432", etc.


def create_table(conn):
    """
    Creates the bronze_ftse_prices table if it does not already exist.
    This runs every time the consumer starts — but IF NOT EXISTS means
    it is completely safe to run multiple times. If the table is already
    there, nothing happens.
    """
    create_table_sql = """
        CREATE TABLE IF NOT EXISTS bronze_ftse_prices (
            id               SERIAL PRIMARY KEY,
            ticker           VARCHAR(20)    NOT NULL,
            sector           VARCHAR(50),
            open             NUMERIC(12,4),
            high             NUMERIC(12,4),
            low              NUMERIC(12,4),
            close            NUMERIC(12,4),
            volume           BIGINT,
            ingested_at      TIMESTAMPTZ,
            market_timestamp VARCHAR(50),
            created_at       TIMESTAMPTZ    DEFAULT NOW(),
            UNIQUE (ticker, market_timestamp)
        );
    """
   
    # What each column type means:
    # SERIAL PRIMARY KEY  — auto-incrementing integer, unique ID for every row
    # VARCHAR(20)         — text up to 20 characters (ticker symbols are short)
    # NUMERIC(12,4)       — number with up to 12 digits, 4 after the decimal point
    # BIGINT              — large integer (volume can be in the hundreds of millions)
    # TIMESTAMPTZ         — timestamp with timezone (the Z in ingested_at means UTC)
    # DEFAULT NOW()       — automatically fills in the current time when row is inserted

    with conn.cursor() as cur:
        # cursor is how you send SQL commands to PostgreSQL from Python
        # think of it as picking up a pen to write in the filing cabinet
        cur.execute(create_table_sql)
    conn.commit()
    # commit() saves the changes permanently
    # without commit(), the table creation would be lost when the connection closes
    logger.info("Table bronze_ftse_prices is ready")


def insert_record(conn, record: dict):
    """
    Inserts a single message from Kafka into the bronze_ftse_prices table.

    ON CONFLICT DO NOTHING handles duplicates.
    If we run the producer twice, the same data arrives in Kafka twice.
    Rather than crashing or creating duplicates, we silently skip it.

    But wait — how does PostgreSQL know what counts as a duplicate?
    We use the combination of ticker + market_timestamp.
    If the same ticker has the same market timestamp, it is the same data point.
    """
    insert_sql = """
        INSERT INTO bronze_ftse_prices (
            ticker, sector, open, high, low, close,
            volume, ingested_at, market_timestamp
        )
        VALUES (
            %(ticker)s,
            %(sector)s,
            %(open)s,
            %(high)s,
            %(low)s,
            %(close)s,
            %(volume)s,
            %(ingested_at)s,
            %(market_timestamp)s
        )
        ON CONFLICT DO NOTHING;
    """
    # %(ticker)s is a named placeholder — psycopg2 replaces it with
    # the actual value from our record dict safely.
    # This prevents SQL injection attacks — important in production.

    with conn.cursor() as cur:
        cur.execute(insert_sql, record)
    conn.commit()


def create_consumer() -> KafkaConsumer:
    """
    Creates and returns a KafkaConsumer.

    value_deserializer is the opposite of the producer's value_serializer.
    Producer converted: Python dict → JSON string → bytes
    Consumer converts:  bytes → JSON string → Python dict
    """
    return KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=KAFKA_GROUP_ID,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        # auto_offset_reset="earliest" means:
        # if this consumer has never run before (no bookmark exists),
        # start reading from the very first message ever stored in the topic.
        # If we used "latest" it would only read NEW messages arriving after startup.
        # We use "earliest" so we pick up the 20 messages already sitting in Kafka.
        auto_offset_reset="earliest",
        # enable_auto_commit=True means Kafka automatically saves our bookmark
        # after each message is read. So if we crash and restart, we do not
        # re-process messages we already handled.
        enable_auto_commit=True,
    )


# ── Graceful shutdown ─────────────────────────────────────────────────────────
# The consumer runs forever. When you press Ctrl+C, Python receives a SIGINT signal.
# Without this handler, the consumer would crash mid-message and potentially
# leave a database transaction open.
# With this handler, it finishes the current message and exits cleanly.

running = True  # global flag — when False, the main loop exits

def handle_shutdown(signum, frame):
    global running
    logger.info("Shutdown signal received — finishing current message then stopping")
    running = False

# Register our handler for Ctrl+C (SIGINT) and kill commands (SIGTERM)
signal.signal(signal.SIGINT, handle_shutdown)
signal.signal(signal.SIGTERM, handle_shutdown)


# ── Main loop ─────────────────────────────────────────────────────────────────

def run_consumer():
    """
    Main function. Connects to PostgreSQL and Kafka, then loops forever
    reading messages and writing them to the database.
    """
    logger.info("Starting FTSE consumer")

    # Open database connection and ensure table exists
    conn = get_db_connection()
    create_table(conn)

    # Create Kafka consumer
    consumer = create_consumer()
    logger.info(f"Listening on topic: {KAFKA_TOPIC}")

    message_count = 0

    # This loop runs forever — it blocks on consumer, waiting for messages.
    # When a message arrives, it processes it and loops back to wait again.
    for message in consumer:
        if not running:
            # Ctrl+C was pressed — exit the loop cleanly
            break

        record = message.value
        # message.value is already a Python dict because of value_deserializer
        # message also has .topic, .partition, .offset if we need them

        try:
            insert_record(conn, record)
            message_count += 1
            logger.info(
                f"Stored {record['ticker']} | "
                f"close={record['close']} | "
                f"offset={message.offset} | "
                f"total stored={message_count}"
            )

        except Exception as e:
            logger.error(f"Failed to insert {record.get('ticker', 'unknown')}: {e}")
            # Roll back the failed transaction so the connection stays healthy
            conn.rollback()

    # Clean up when the loop exits
    consumer.close()
    conn.close()
    logger.info(f"Consumer stopped. Total messages stored: {message_count}")


# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    run_consumer()