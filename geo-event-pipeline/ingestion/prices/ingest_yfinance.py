"""
Yahoo Finance Commodity Prices Ingestion
Fetches commodity prices from Yahoo Finance and loads into raw_prices.
"""

import duckdb
import pandas as pd
import requests
import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Dict, Any, List
import uuid

# ---------------------------------------------------------------------------
# LOGGING
# ---------------------------------------------------------------------------

logger = logging.getLogger(__name__)

LOG_FORMAT = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"


def setup_logging(level: int = logging.INFO) -> None:
    """Configure root logger."""
    logging.basicConfig(
        level=level,
        format=LOG_FORMAT,
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[logging.StreamHandler(sys.stdout)]
    )
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)


# ---------------------------------------------------------------------------
# EXCEPTIONS
# ---------------------------------------------------------------------------

class PriceIngestionError(Exception):
    """Base exception for price ingestion failures."""


class PriceConnectionError(PriceIngestionError):
    """Cannot reach Yahoo Finance."""


class PriceParseError(PriceIngestionError):
    """Yahoo Finance response format unexpected."""


class DatabaseWriteError(PriceIngestionError):
    """DuckDB insert failed."""


# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------

# Only the 4 commodities from Phase 1
COMMODITIES = {
    "BZ=F":  "brent_oil",
    "GC=F":  "gold",
    "SI=F":  "silver",
    "NG=F":  "natural_gas",
}

# Yahoo Finance API base URL
YAHOO_CHART_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{}"

# Intervals to try, in order (Phase 1 decision)
INTERVAL_FALLBACK = ["1m", "5m", "1h"]

# Headers to mimic a browser
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/125.0.0.0 Safari/537.36"
    )
}

REQUEST_TIMEOUT = 30
MAX_RETRIES = 3
RATE_LIMIT_DELAY = 0.5  # Seconds between API calls to be polite


# ---------------------------------------------------------------------------
# HELPERS
# ---------------------------------------------------------------------------

def get_project_root() -> Path:
    """Return geo-event-pipeline root directory."""
    return Path(__file__).resolve().parent.parent.parent


# ---------------------------------------------------------------------------
# FETCHING
# ---------------------------------------------------------------------------

def fetch_prices(
    ticker: str,
    interval: str = "1m",
    range_str: str = "1d"
) -> Optional[Dict[str, Any]]:
    """
    Fetch price data for a single commodity from Yahoo Finance.

    Args:
        ticker: Yahoo Finance ticker (e.g., "GC=F")
        interval: Data granularity ("1m", "5m", "1h")
        range_str: Time range ("1d", "5d", "7d")

    Returns:
        dict with keys: ticker, interval, timestamps, opens, highs,
                        lows, closes, volumes
        None if the request fails.
    """
    url = YAHOO_CHART_URL.format(ticker)
    params = {"interval": interval, "range": range_str}

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            logger.debug("Fetching %s [%s] (attempt %d/%d)",
                         ticker, interval, attempt, MAX_RETRIES)
            resp = requests.get(
                url, headers=HEADERS, params=params, timeout=REQUEST_TIMEOUT
            )

            if resp.status_code == 429:
                # Rate limited — wait and retry
                wait = 2 ** attempt
                logger.warning("Rate limited for %s. Waiting %ds...", ticker, wait)
                time.sleep(wait)
                continue

            if resp.status_code != 200:
                logger.warning("%s returned HTTP %s", ticker, resp.status_code)
                return None

            data = resp.json()
            result = data["chart"]["result"][0]

            timestamps = result["timestamp"]
            quotes = result["indicators"]["quote"][0]

            return {
                "ticker": ticker,
                "interval": interval,
                "timestamps": timestamps,
                "opens": quotes["open"],
                "highs": quotes["high"],
                "lows": quotes["low"],
                "closes": quotes["close"],
                "volumes": quotes["volume"],
            }

        except requests.exceptions.Timeout:
            logger.warning("Timeout for %s on attempt %d", ticker, attempt)
        except requests.exceptions.RequestException as e:
            logger.warning("Request failed for %s: %s", ticker, e)
        except (KeyError, IndexError, TypeError) as e:
            logger.warning("Unexpected response format for %s: %s", ticker, e)
            return None
        except Exception as e:
            logger.warning("Unexpected error for %s: %s", ticker, e)

    logger.error("All %d attempts failed for %s [%s]", MAX_RETRIES, ticker, interval)
    return None


def fetch_with_fallback(ticker: str, range_str: str = "1d") -> Optional[Dict[str, Any]]:
    """
    Try to fetch prices, falling back to coarser intervals if needed.

    Tries 1m → 5m → 1h in order.
    """
    for interval in INTERVAL_FALLBACK:
        result = fetch_prices(ticker, interval, range_str)
        if result is not None:
            logger.info("Fetched %s at %s interval (%d data points)",
                        ticker, interval, len(result["timestamps"]))
            return result
        logger.info("Interval %s unavailable for %s, trying next...", interval, ticker)

    logger.error("All intervals exhausted for %s", ticker)
    return None


# ---------------------------------------------------------------------------
# TRANSFORM
# ---------------------------------------------------------------------------

def parse_price_data(raw: Dict[str, Any]) -> pd.DataFrame:
    """
    Convert raw Yahoo Finance response into a clean DataFrame.

    Args:
        raw: Dict from fetch_with_fallback()

    Returns:
        DataFrame with columns matching raw_prices table.
    """
    rows = []
    ticker = raw["ticker"]
    interval = raw["interval"]

    for i, ts in enumerate(raw["timestamps"]):
        timestamp = datetime.fromtimestamp(ts, tz=timezone.utc)

        # Round prices to 4 decimal places, handle None
        close = round(raw["closes"][i], 4) if raw["closes"][i] is not None else None
        open_p = round(raw["opens"][i], 4) if raw["opens"][i] is not None else None
        high = round(raw["highs"][i], 4) if raw["highs"][i] is not None else None
        low = round(raw["lows"][i], 4) if raw["lows"][i] is not None else None
        volume = int(raw["volumes"][i]) if raw["volumes"][i] is not None else None

        # Synthetic primary key
        price_id = f"{ticker}_{timestamp.strftime('%Y%m%d%H%M%S')}"

        rows.append({
            "price_id": price_id,
            "commodity_code": ticker,
            "timestamp": timestamp,
            "open": open_p,
            "high": high,
            "low": low,
            "close": close,
            "volume": volume,
            "interval": interval,
        })

    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def ingest_prices(db_path: str = None, range_str: str = "1d") -> Dict[str, Any]:
    """
    Run the full commodity price ingestion pipeline.

    Args:
        db_path: Path to DuckDB file.
        range_str: Time range to fetch ("1d", "5d", etc.)

    Returns:
        dict with run_id, status, results per commodity.
    """
    root = get_project_root()
    if db_path is None:
        db_path = str(root / "data" / "warehouse.duckdb")

    batch_id = str(uuid.uuid4())
    run_id = str(uuid.uuid4())
    start_time = datetime.now(timezone.utc)

    logger.info("=" * 60)
    logger.info("Starting Commodity Price Ingestion")
    logger.info("Run ID: %s | Batch ID: %s | Range: %s", run_id, batch_id, range_str)
    logger.info("=" * 60)

    try:
        conn = duckdb.connect(db_path)
    except Exception as e:
        raise DatabaseWriteError(f"DuckDB connection failed: {e}") from e

    try:
        conn.execute(
            """
            INSERT INTO raw.pipeline_runs
                (run_id, pipeline_name, run_status, start_time, trigger_type)
            VALUES (?, 'prices_ingestion', 'STARTED', ?, 'SCHEDULED')
            """,
            [run_id, start_time]
        )
    except Exception as e:
        conn.close()
        raise DatabaseWriteError(f"Pipeline run insert failed: {e}") from e

    all_results = {}
    total_attempted = 0
    failed_commodities = []

    try:
        for ticker, name in COMMODITIES.items():
            logger.info("Processing %s (%s)...", name, ticker)

            raw = fetch_with_fallback(ticker, range_str)

            if raw is None:
                logger.warning("No data for %s — skipping", name)
                failed_commodities.append(name)
                all_results[name] = {"status": "FAILED", "rows": 0}
                continue

            df = parse_price_data(raw)
            df["ingestion_timestamp"] = datetime.now(timezone.utc)
            df["batch_id"] = batch_id

            logger.info("%s: %s rows at %s interval", name, len(df), raw["interval"])

            conn.register("_temp_prices", df)
            conn.execute(
                "INSERT OR IGNORE INTO raw.raw_prices SELECT * FROM _temp_prices"
            )

            all_results[name] = {
                "status": "SUCCESS",
                "interval": raw["interval"],
                "rows_attempted": len(df),
            }
            total_attempted += len(df)

            time.sleep(RATE_LIMIT_DELAY)

        # Count total inserted for this batch (once, after all inserts)
        total_rows = conn.execute(
            "SELECT COUNT(*) FROM raw.raw_prices WHERE batch_id = ?",
            [batch_id]
        ).fetchone()[0]
        total_duplicates = total_attempted - total_rows

        # Watermark
        watermark = datetime.now(timezone.utc).isoformat()

        # Status
        status = "PARTIAL" if failed_commodities else "SUCCESS"
        end_time = datetime.now(timezone.utc)
        duration = (end_time - start_time).total_seconds()

        error_msg = None
        if failed_commodities:
            error_msg = f"Failed: {', '.join(failed_commodities)}"

        conn.execute(
            """
            UPDATE raw.pipeline_runs
            SET run_status = ?,
                end_time = ?,
                rows_ingested = ?,
                source_watermark = ?,
                error_message = ?
            WHERE run_id = ?
            """,
            [status, end_time, total_rows, watermark, error_msg, run_id]
        )

        logger.info("=" * 60)
        logger.info("Price Ingestion Complete")
        for name, r in all_results.items():
            if r["status"] == "SUCCESS":
                logger.info("  %-15s: %s rows (%s)",
                            name, r["rows_attempted"], r["interval"])
            else:
                logger.info("  %-15s: FAILED", name)
        logger.info("  Total attempted: %s", total_attempted)
        logger.info("  Total inserted:  %s", total_rows)
        logger.info("  Duplicates:      %s", total_duplicates)
        logger.info("  Duration:        %.2f seconds", duration)
        logger.info("=" * 60)

        conn.close()

        return {
            "run_id": run_id,
            "status": status,
            "commodities": all_results,
            "total_rows_ingested": total_rows,
            "total_duplicates": total_duplicates,
            "failed": failed_commodities,
            "duration_seconds": duration,
        }

    except Exception as e:
        logger.exception("Unexpected error during price ingestion")
        _record_failure(conn, run_id, str(e))
        raise PriceIngestionError(f"Unexpected error: {e}") from e


def _record_failure(conn, run_id: str, error_message: str) -> None:
    """Update pipeline run to FAILED. Never raises."""
    try:
        end_time = datetime.now(timezone.utc)
        truncated = error_message[:2000] if len(error_message) > 2000 else error_message
        conn.execute(
            """
            UPDATE raw.pipeline_runs
            SET run_status = 'FAILED', end_time = ?, error_message = ?
            WHERE run_id = ?
            """,
            [end_time, truncated, run_id]
        )
        logger.error("Pipeline run %s marked as FAILED", run_id)
    except Exception as e:
        logger.error("Could not record failure: %s", e)
    finally:
        try:
            conn.close()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    setup_logging(level=logging.INFO)

    try:
        result = ingest_prices(range_str="1d")
        sys.exit(0)
    except PriceConnectionError as e:
        logger.error("Connection error: %s", e)
        sys.exit(1)
    except PriceParseError as e:
        logger.error("Parse error: %s", e)
        sys.exit(1)
    except DatabaseWriteError as e:
        logger.error("Database error: %s", e)
        sys.exit(1)
    except Exception as e:
        logger.exception("Fatal ingestion error")
        sys.exit(1)