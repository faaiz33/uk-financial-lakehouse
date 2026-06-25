# ingestion/producers/fx_producer.py

import json
import time
import logging
import os
from datetime import datetime, timezone
import yfinance as yf
from dotenv import load_dotenv
from kafka import KafkaProducer

# ── Load environment variables ───────────────────────────────────────────────
load_dotenv()

# ── Logging setup ────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

# ── Currency pairs we want to track ─────────────────────────────────────────
# Each tuple is (from_currency, to_currency)
# GBP/USD = how many dollars per pound
# GBP/EUR = how many euros per pound
CURRENCY_PAIRS = [
    ("GBP", "USD"),
    ("GBP", "EUR"),
]



# ── Kafka config ─────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = "fx_rates"
# Separate topic from ftse_prices — FX data and stock data never mix


def create_producer() -> KafkaProducer:
    """
    Identical to the FTSE producer.
    Creates a KafkaProducer that serialises Python dicts to JSON bytes.
    """
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        retries=3,
        request_timeout_ms=10000,
    )


def fetch_fx_rate(from_currency: str, to_currency: str) -> dict | None:
    """
    Fetches the current exchange rate using yfinance.
    Yahoo Finance ticker format for FX pairs is "GBPUSD=X", "GBPEUR=X" etc.
    No API key needed. No rate limits.
    """
    try:
        ticker_symbol = f"{from_currency}{to_currency}=X"
        # f-string builds the ticker: "GBP" + "USD" + "=X" = "GBPUSD=X"

        ticker = yf.Ticker(ticker_symbol)
        df = ticker.history(period="1d", interval="1m")

        if df.empty:
            logger.warning(f"No data for {from_currency}/{to_currency} — market may be closed")
            return None

        latest = df.iloc[-1]

        return {
            "from_currency":  from_currency,
            "to_currency":    to_currency,
            "exchange_rate":  round(float(latest["Close"]), 6),
            "high":           round(float(latest["High"]), 6),
            "low":            round(float(latest["Low"]), 6),
            "volume":         int(latest["Volume"]),
            "last_refreshed": str(df.index[-1]),
            "ingested_at":    datetime.now(timezone.utc).isoformat(),
        }

    except Exception as e:
        logger.error(f"Failed to fetch {from_currency}/{to_currency}: {e}")
        return None

def run_producer():
    """
    Main loop. Fetches both currency pairs and sends each to Kafka.
    """
    logger.info("Starting FX producer")

 

    producer = create_producer()
    success_count = 0
    fail_count = 0

    for from_currency, to_currency in CURRENCY_PAIRS:
        logger.info(f"Fetching {from_currency}/{to_currency}")
        data = fetch_fx_rate(from_currency, to_currency)

        if data is None:
            fail_count += 1
            continue

        future = producer.send(KAFKA_TOPIC, value=data)
        try:
            meta = future.get(timeout=10)
            logger.info(
                f"Sent {from_currency}/{to_currency} | "
                f"rate={data['exchange_rate']} | "
                f"topic={meta.topic} | "
                f"offset={meta.offset}"
            )
            success_count += 1
        except Exception as e:
            logger.error(f"Failed to send {from_currency}/{to_currency} to Kafka: {e}")
            fail_count += 1

        # Alpha Vantage free tier allows 25 requests per minute
        # 15 second delay between requests keeps us well within the limit
        time.sleep(1)

    producer.flush()
    producer.close()
    logger.info(f"FX producer done. Success: {success_count} | Failed: {fail_count}")


if __name__ == "__main__":
    run_producer()