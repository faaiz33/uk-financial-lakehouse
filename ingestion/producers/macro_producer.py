# ingestion/producers/macro_producer.py

import json
import time
import logging
import os
from datetime import datetime, timezone
from dotenv import load_dotenv
import requests
from kafka import KafkaProducer

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = "macro_indicators"

# Browser headers — required by both BOE and ONS to avoid being blocked
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
}

# ONS timeseries — correct URL pattern is the website URL + /data
# This is the stable, documented way to access ONS time series data
ONS_SERIES = {
    "unemployment_rate": "https://www.ons.gov.uk/employmentandlabourmarket/peoplenotinwork/unemployment/timeseries/mgsx/lms/data",
    "gdp_growth":        "https://www.ons.gov.uk/economy/grossdomesticproductgdp/timeseries/ihyq/pn2/data",
    "inflation_cpi":     "https://www.ons.gov.uk/economy/inflationandpriceindices/timeseries/d7g7/mm23/data",
}

# BOE base rate — fetched from their CSV download endpoint
BOE_BASE_RATE_URL = (
    "https://www.bankofengland.co.uk/boeapps/database/_iadb-fromshowcolumns.asp"
    "?csv.x=yes&SeriesCodes=IUDBEDR&UsingCodes=Y&CSVF=TN&VPD=Y"
)


def create_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        retries=3,
        request_timeout_ms=10000,
    )


def fetch_boe_base_rate() -> dict | None:
    """
    Fetches BOE base rate. Falls back to known value if endpoint blocks us.
    BOE base rate as of June 2026: 3.75% (voted 7-2 to hold, June 2026)
    Source: https://www.bankofengland.co.uk/monetary-policy/the-interest-rate-bank-rate
    This value changes only ~8 times per year at MPC meetings.
    In production, you would update this via a scheduled alert or webhook.
    """
    try:
        response = requests.get(BOE_BASE_RATE_URL, headers=HEADERS, timeout=15)
        response.raise_for_status()
        lines = [l.strip() for l in response.text.strip().split("\n") if l.strip()]
        data_lines = [l for l in lines if not l.startswith('"Date"') and not l.startswith('Date')]

        if data_lines:
            latest = data_lines[-1]
            parts = [p.strip().strip('"') for p in latest.split(",")]
            if len(parts) >= 2 and parts[1]:
                return {
                    "indicator":   "base_rate",
                    "series_code": "IUDBEDR",
                    "source":      "Bank of England",
                    "value":       float(parts[1]),
                    "period":      parts[0],
                    "ingested_at": datetime.now(timezone.utc).isoformat(),
                }

    except Exception as e:
        logger.warning(f"BOE API unavailable ({e}), using known fallback value")

    # Fallback — BOE rate as of June 2026
    logger.info("Using BOE base rate fallback value: 3.75%")
    return {
        "indicator":   "base_rate",
        "series_code": "IUDBEDR",
        "source":      "Bank of England (fallback)",
        "value":       3.75,
        "period":      "Jun 2026",
        "ingested_at": datetime.now(timezone.utc).isoformat(),
    }


def fetch_ons_series(series_name: str, url: str) -> dict | None:
    """
    Fetches latest value from ONS JSON data endpoint.
    ONS website exposes JSON at /timeseries/{code}/{dataset}/data
    """
    try:
        response = requests.get(url, headers=HEADERS, timeout=15)
        response.raise_for_status()
        data = response.json()

        # Try quarters first (GDP), then months (unemployment, CPI)
        if "quarters" in data and data["quarters"]:
            periods = data["quarters"]
        elif "months" in data and data["months"]:
            periods = data["months"]
        else:
            logger.error(f"No period data for {series_name}")
            return None

        latest = periods[-1]

        return {
            "indicator":   series_name,
            "series_code": data.get("description", {}).get("cdid", series_name),
            "source":      "ONS",
            "value":       float(latest["value"]),
            "period":      latest["date"],
            "ingested_at": datetime.now(timezone.utc).isoformat(),
        }

    except Exception as e:
        logger.error(f"Failed to fetch ONS series {series_name}: {e}")
        return None


def run_producer():
    logger.info("Starting macro producer")
    producer = create_producer()
    success_count = 0
    fail_count = 0

    # Fetch BOE base rate
    logger.info("Fetching BOE base rate")
    data = fetch_boe_base_rate()
    if data:
        future = producer.send(KAFKA_TOPIC, value=data)
        try:
            meta = future.get(timeout=10)
            logger.info(f"Sent base_rate | value={data['value']} | period={data['period']} | offset={meta.offset}")
            success_count += 1
        except Exception as e:
            logger.error(f"Failed to send base_rate: {e}")
            fail_count += 1
    else:
        fail_count += 1
    time.sleep(2)

    # Fetch ONS series
    for series_name, url in ONS_SERIES.items():
        logger.info(f"Fetching ONS: {series_name}")
        data = fetch_ons_series(series_name, url)
        if data:
            future = producer.send(KAFKA_TOPIC, value=data)
            try:
                meta = future.get(timeout=10)
                logger.info(f"Sent {series_name} | value={data['value']} | period={data['period']} | offset={meta.offset}")
                success_count += 1
            except Exception as e:
                logger.error(f"Failed to send {series_name}: {e}")
                fail_count += 1
        else:
            fail_count += 1
        time.sleep(2)

    producer.flush()
    producer.close()
    logger.info(f"Macro producer done. Success: {success_count} | Failed: {fail_count}")


if __name__ == "__main__":
    run_producer()