# ingestion/producers/ftse_producer.py

import json
import time
import logging
import os
from datetime import datetime, timezone
from dotenv import load_dotenv
import yfinance as yf
from kafka import KafkaProducer

# ── Load environment variables ──────────────────────────────────────────────
# Reads your .env file and makes every variable available via os.environ.get()
# Without this line, all your credentials would return None
load_dotenv()

# ── Logging setup ────────────────────────────────────────────────────────────
# logging gives us timestamps and severity levels automatically
# In production these logs get shipped to tools like Datadog or Splunk
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

# ── FTSE 100 tickers ─────────────────────────────────────────────────────────
# Yahoo Finance uses ".L" suffix for London Stock Exchange stocks
# HSBC on LSE = "HSBA.L", BP on LSE = "BP.L" and so on
# We use 20 stocks for development — production would use all 100
FTSE_TICKERS = [
    "HSBA.L",  # HSBC
    "BP.L",    # BP
    "SHEL.L",  # Shell
    "GSK.L",   # GSK
    "AZN.L",   # AstraZeneca
    "ULVR.L",  # Unilever
    "RIO.L",   # Rio Tinto
    "AAL.L",   # Anglo American
    "BARC.L",  # Barclays
    "LLOY.L",  # Lloyds Banking Group
    "VOD.L",   # Vodafone
    "BT-A.L",  # BT Group
    "TSCO.L",  # Tesco
    "PRU.L",   # Prudential
    "NG.L",    # National Grid
    "SSE.L",   # SSE
    "REL.L",   # RELX
    "CPG.L",   # Compass Group
    "WPP.L",   # WPP
    "STAN.L",  # Standard Chartered
]

# ── Sector map ───────────────────────────────────────────────────────────────
# We tag each stock with its industry sector
# This powers the "Sector Performance" page of our Streamlit dashboard later
SECTOR_MAP = {
    "HSBA.L": "Financials",
    "BP.L": "Energy",
    "SHEL.L": "Energy",
    "GSK.L": "Healthcare",
    "AZN.L": "Healthcare",
    "ULVR.L": "Consumer",
    "RIO.L": "Materials",
    "AAL.L": "Materials",
    "BARC.L": "Financials",
    "LLOY.L": "Financials",
    "VOD.L": "Technology",
    "BT-A.L": "Technology",
    "TSCO.L": "Consumer",
    "PRU.L": "Financials",
    "NG.L": "Utilities",
    "SSE.L": "Utilities",
    "REL.L": "Technology",
    "CPG.L": "Consumer",
    "WPP.L": "Technology",
    "STAN.L": "Financials",
}

# ── Kafka config ─────────────────────────────────────────────────────────────
# Bootstrap servers = the address Kafka is listening on
# We read from .env, and fall back to localhost:9092 if not set
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

# The Kafka topic is a named channel — like a dedicated inbox
# All FTSE price messages go into this one topic
# The consumer will read exclusively from this topic
KAFKA_TOPIC = "ftse_prices"


def create_producer() -> KafkaProducer:
    """
    Creates and returns a KafkaProducer.

    Kafka only understands raw bytes — not Python dicts.
    value_serializer converts our dict automatically:
    Python dict → json.dumps() → JSON string → .encode("utf-8") → bytes
    This happens invisibly every time we call producer.send()
    """
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        retries=3,                  # retry failed sends up to 3 times
        request_timeout_ms=10000,   # wait up to 10 seconds for broker acknowledgement
    )


def fetch_ftse_data(ticker: str) -> dict | None:
    """
    Fetches the latest 1-minute price bar for a single ticker.
    Returns a dict, or None if the fetch fails or market is closed.

    period="1d"   = fetch today's data only
    interval="1m" = in 1-minute bars (granularity)
    iloc[-1]      = take the most recent bar (last row of the DataFrame)
    """
    try:
        stock = yf.Ticker(ticker)
        df = stock.history(period="1d", interval="1m")

        if df.empty:
            # Normal outside LSE hours: Mon-Fri 08:00-16:30 UTC
            logger.warning(f"No data for {ticker} — market may be closed")
            return None

        latest = df.iloc[-1]

        return {
            "ticker": ticker,
            "sector": SECTOR_MAP.get(ticker, "Unknown"),
            "open":   round(float(latest["Open"]),  4),
            "high":   round(float(latest["High"]),  4),
            "low":    round(float(latest["Low"]),   4),
            "close":  round(float(latest["Close"]), 4),
            "volume": int(latest["Volume"]),
            # ingested_at = when our pipeline received this data
            # separate from market_timestamp = when the market generated it
            # this distinction matters for debugging late-arriving data
            "ingested_at":      datetime.now(timezone.utc).isoformat(),
            "market_timestamp": str(df.index[-1]),
        }

    except Exception as e:
        logger.error(f"Failed to fetch {ticker}: {e}")
        return None


def run_producer():
    """
    Main loop — fetches all tickers and sends each to Kafka.
    Designed to run once per invocation.
    Airflow will call this on a schedule later.
    """
    logger.info("Starting FTSE producer")
    producer = create_producer()
    success_count = 0
    fail_count = 0

    for ticker in FTSE_TICKERS:
        data = fetch_ftse_data(ticker)

        if data is None:
            fail_count += 1
            continue

        # producer.send() is non-blocking — it queues the message internally
        # .get(timeout=10) blocks until Kafka confirms it was received
        # Without .get(), the script could exit before messages are actually delivered
        future = producer.send(KAFKA_TOPIC, value=data)
        try:
            meta = future.get(timeout=10)
            logger.info(
                f"Sent {ticker} | "
                f"topic={meta.topic} | "
                f"partition={meta.partition} | "
                f"offset={meta.offset}"
            )
            success_count += 1
        except Exception as e:
            logger.error(f"Failed to send {ticker} to Kafka: {e}")
            fail_count += 1

        # 0.5 second pause between API calls
        # Yahoo Finance will rate-limit or block you if you hammer it
        time.sleep(0.5)

    # flush() pushes any messages still sitting in the internal buffer
    # close() releases the network connection cleanly
    producer.flush()
    producer.close()

    logger.info(f"Producer done. Success: {success_count} | Failed: {fail_count}")


# ── Entry point ───────────────────────────────────────────────────────────────
# Only runs when you execute: python ftse_producer.py
# Does NOT run when Airflow imports this file as a module later
if __name__ == "__main__":
    run_producer()