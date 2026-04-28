"""
GDELT 2.0 Events Ingestion
Fetches the latest 15-minute GDELT update and loads it into raw_events.
"""

import duckdb
import pandas as pd
import requests
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse
from typing import Optional, Dict, Any
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

class GDeltIngestionError(Exception):
    """Base exception for GDELT ingestion failures."""


class GDeltConnectionError(GDeltIngestionError):
    """Cannot reach GDELT servers."""


class GDeltParseError(GDeltIngestionError):
    """GDELT data format is unexpected."""


class DatabaseWriteError(GDeltIngestionError):
    """DuckDB insert failed."""


# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------

GDELT_LASTUPDATE_URL = "http://data.gdeltproject.org/gdeltv2/lastupdate.txt"
REQUEST_TIMEOUT = 30
MAX_RETRIES = 3

# Full GDELT 2.0 schema — 61 columns, in order
GDELT_COLUMNS = [
    "GLOBALEVENTID", "SQLDATE", "MonthYear", "Year", "FractionDate",
    "Actor1Code", "Actor1Name", "Actor1CountryCode", "Actor1KnownGroupCode",
    "Actor1EthnicCode", "Actor1Religion1Code", "Actor1Religion2Code",
    "Actor1Type1Code", "Actor1Type2Code", "Actor1Type3Code",
    "Actor2Code", "Actor2Name", "Actor2CountryCode", "Actor2KnownGroupCode",
    "Actor2EthnicCode", "Actor2Religion1Code", "Actor2Religion2Code",
    "Actor2Type1Code", "Actor2Type2Code", "Actor2Type3Code",
    "IsRootEvent", "EventCode", "EventBaseCode", "EventRootCode",
    "QuadClass", "GoldsteinScale", "NumMentions", "NumSources",
    "NumArticles", "AvgTone",
    "Actor1Geo_Type", "Actor1Geo_FullName", "Actor1Geo_CountryCode",
    "Actor1Geo_ADM1Code", "Actor1Geo_ADM2Code",
    "Actor1Geo_Lat", "Actor1Geo_Long", "Actor1Geo_FeatureID",
    "Actor2Geo_Type", "Actor2Geo_FullName", "Actor2Geo_CountryCode",
    "Actor2Geo_ADM1Code", "Actor2Geo_ADM2Code",
    "Actor2Geo_Lat", "Actor2Geo_Long", "Actor2Geo_FeatureID",
    "ActionGeo_Type", "ActionGeo_FullName", "ActionGeo_CountryCode",
    "ActionGeo_ADM1Code", "ActionGeo_ADM2Code",
    "ActionGeo_Lat", "ActionGeo_Long", "ActionGeo_FeatureID",
    "DATEADDED", "SOURCEURL"
]

# Columns we keep — 18 GDELT fields
KEEP_COLUMNS = [
    "GLOBALEVENTID", "SQLDATE", "EventCode", "EventBaseCode",
    "EventRootCode", "GoldsteinScale", "NumMentions", "NumArticles",
    "AvgTone", "Actor1Code", "Actor1Name", "Actor2Code", "Actor2Name",
    "ActionGeo_CountryCode", "ActionGeo_Lat", "ActionGeo_Long",
    "DATEADDED", "SOURCEURL"
]

# Map GDELT columns → raw_events columns
# NOTE: SQLDATE and DATEADDED are NOT here — they are transformed into new columns
COLUMN_MAPPING = {
    "GLOBALEVENTID":          "event_id",
    "EventCode":              "event_code",
    "EventBaseCode":          "event_base_code",
    "EventRootCode":          "event_root_code",
    "GoldsteinScale":         "goldstein_scale",
    "NumMentions":            "num_mentions",
    "NumArticles":            "num_articles",
    "AvgTone":                "avg_tone",
    "Actor1Code":             "actor1_code",
    "Actor1Name":             "actor1_name",
    "Actor2Code":             "actor2_code",
    "Actor2Name":             "actor2_name",
    "ActionGeo_CountryCode":  "action_geo_country_code",
    "ActionGeo_Lat":          "action_geo_lat",
    "ActionGeo_Long":         "action_geo_long",
    "SOURCEURL":              "source_url",
}

# Final 21 columns for raw_events — must match the table DDL exactly
INSERT_COLUMNS = [
    "event_id", "event_date", "event_timestamp", "event_code",
    "event_base_code", "event_root_code", "goldstein_scale",
    "num_mentions", "num_articles", "avg_tone",
    "actor1_code", "actor1_name", "actor2_code", "actor2_name",
    "action_geo_country_code", "action_geo_lat", "action_geo_long",
    "source_url", "source_domain", "ingestion_timestamp", "batch_id"
]


# ---------------------------------------------------------------------------
# HELPERS
# ---------------------------------------------------------------------------

def get_project_root() -> Path:
    """Return geo-event-pipeline root directory."""
    return Path(__file__).resolve().parent.parent.parent


def get_last_watermark(conn: duckdb.DuckDBPyConnection) -> Optional[str]:
    """Get last successful watermark from pipeline_runs."""
    try:
        result = conn.execute(
            """
            SELECT source_watermark
            FROM raw.pipeline_runs
            WHERE pipeline_name = 'events_ingestion'
              AND run_status = 'SUCCESS'
            ORDER BY start_time DESC
            LIMIT 1
            """
        ).fetchone()
        return result[0] if result else None
    except Exception as e:
        logger.warning("Could not read last watermark: %s", e)
        return None


# ---------------------------------------------------------------------------
# FETCHING
# ---------------------------------------------------------------------------

def fetch_latest_gdelt_url() -> str:
    """Return the latest GDELT export CSV URL from lastupdate.txt."""
    last_exc = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            logger.info("Fetching lastupdate.txt (attempt %d/%d)", attempt, MAX_RETRIES)
            resp = requests.get(GDELT_LASTUPDATE_URL, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            break
        except requests.exceptions.Timeout as e:
            last_exc = e
            logger.warning("Timeout on attempt %d: %s", attempt, e)
        except requests.exceptions.ConnectionError as e:
            last_exc = e
            logger.warning("Connection error on attempt %d: %s", attempt, e)
        except requests.exceptions.RequestException as e:
            last_exc = e
            logger.warning("Request failed on attempt %d: %s", attempt, e)
    else:
        raise GDeltConnectionError(
            f"Could not reach GDELT after {MAX_RETRIES} attempts"
        ) from last_exc

    lines = resp.text.strip().split("\n")
    if len(lines) < 1:
        raise GDeltParseError("lastupdate.txt returned empty response")

    parts = lines[0].split(" ")
    if len(parts) < 3:
        raise GDeltParseError(
            f"Unexpected format (expected 3 parts): {lines[0][:100]}"
        )

    csv_url = parts[2]
    logger.info("Latest GDELT export: %s (%s bytes)", csv_url, parts[0])
    return csv_url


def download_gdelt_csv(csv_url: str) -> pd.DataFrame:
    """Download and parse GDELT CSV into a DataFrame."""
    last_exc = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            logger.info("Downloading GDELT CSV (attempt %d/%d)", attempt, MAX_RETRIES)
            df = pd.read_csv(
                csv_url,
                sep="\t",
                header=None,
                names=GDELT_COLUMNS,
                dtype=str,
                quoting=1,
                engine="python",
                on_bad_lines="warn",
                encoding="utf-8"
            )
            break
        except requests.exceptions.RequestException as e:
            last_exc = e
            logger.warning("Download failed on attempt %d: %s", attempt, e)
        except pd.errors.ParserError as e:
            raise GDeltParseError(f"Failed to parse CSV: {e}") from e
        except Exception as e:
            last_exc = e
            logger.warning("Unexpected error on attempt %d: %s", attempt, e)
    else:
        raise GDeltConnectionError(
            f"Could not download CSV after {MAX_RETRIES} attempts"
        ) from last_exc

    if len(df.columns) != len(GDELT_COLUMNS):
        raise GDeltParseError(
            f"Column mismatch: expected {len(GDELT_COLUMNS)}, got {len(df.columns)}"
        )

    # Strip stray quotes
    for col in df.columns:
        df[col] = df[col].str.strip('"')

    logger.info("Downloaded %s rows, %s columns (validated)", f"{len(df):,}", len(df.columns))
    return df


# ---------------------------------------------------------------------------
# TRANSFORMATIONS
# ---------------------------------------------------------------------------

def parse_event_timestamp(dateadded) -> Optional[datetime]:
    """Convert DATEADDED integer (YYYYMMDDHHMMSS) to datetime."""
    try:
        ts_str = str(int(float(dateadded)))
        if len(ts_str) != 14:
            return None
        return datetime.strptime(ts_str, "%Y%m%d%H%M%S")
    except (ValueError, TypeError, OverflowError):
        return None


def parse_event_date(sqldate) -> Optional[datetime]:
    """Convert SQLDATE integer (YYYYMMDD) to date."""
    try:
        date_str = str(int(float(sqldate)))
        if len(date_str) != 8:
            return None
        return datetime.strptime(date_str, "%Y%m%d").date()
    except (ValueError, TypeError, OverflowError):
        return None


def extract_domain(url) -> Optional[str]:
    """Extract bare domain from URL."""
    if pd.isna(url) or not url or not isinstance(url, str):
        return None
    try:
        parsed = urlparse(url)
        domain = parsed.netloc or parsed.path.split("/")[0]
        return domain.replace("www.", "").lower()
    except Exception:
        return None


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def ingest_events(db_path: str = None) -> Dict[str, Any]:
    """Run full GDELT events ingestion pipeline."""
    root = get_project_root()
    if db_path is None:
        db_path = str(root / "data" / "warehouse.duckdb")

    batch_id = str(uuid.uuid4())
    run_id = str(uuid.uuid4())
    start_time = datetime.now(timezone.utc)

    logger.info("=" * 60)
    logger.info("Starting GDELT Events Ingestion")
    logger.info("Run ID: %s | Batch ID: %s", run_id, batch_id)
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
            VALUES (?, 'events_ingestion', 'STARTED', ?, 'SCHEDULED')
            """,
            [run_id, start_time]
        )
    except Exception as e:
        conn.close()
        raise DatabaseWriteError(f"Pipeline run insert failed: {e}") from e

    try:
        # Watermark
        last_watermark = get_last_watermark(conn)
        logger.info("Last watermark: %s", last_watermark or "None (first run)")

        # Download
        csv_url = fetch_latest_gdelt_url()
        df = download_gdelt_csv(csv_url)

        if len(df) == 0:
            raise GDeltParseError("Downloaded CSV contains 0 rows")
        if len(df) < 100:
            logger.warning("Low row count: %s rows", len(df))

        # Select columns
        df = df[KEEP_COLUMNS].copy()

        # Transform
        df["event_date"]      = df["SQLDATE"].apply(parse_event_date)
        df["event_timestamp"] = df["DATEADDED"].apply(parse_event_timestamp)
        df["source_domain"]   = df["SOURCEURL"].apply(extract_domain)

        null_ts = df["event_timestamp"].isna().sum()
        if null_ts > 0:
            logger.warning("%s rows (%s%%) have unparseable DATEADDED",
                           null_ts, f"{null_ts / len(df) * 100:.2f}")

        df["ingestion_timestamp"] = datetime.now(timezone.utc)
        df["batch_id"]            = batch_id

        # Rename via mapping (SQLDATE and DATEADDED stay as-is, get dropped later)
        df.rename(columns=COLUMN_MAPPING, inplace=True)

        # Select final columns
        df_insert = df[INSERT_COLUMNS]

        logger.info("Prepared %s rows (%s columns) for insertion",
                    f"{len(df_insert):,}", len(df_insert.columns))

        # Insert
        conn.register("_temp_events", df_insert)
        conn.execute(
            "INSERT OR IGNORE INTO raw.raw_events SELECT * FROM _temp_events"
        )

        actual_inserted = conn.execute(
            "SELECT COUNT(*) FROM raw.raw_events WHERE batch_id = ?",
            [batch_id]
        ).fetchone()[0]

        duplicates = len(df_insert) - actual_inserted
        if duplicates > 0:
            logger.info("Skipped %s duplicate event_ids", f"{duplicates:,}")

        # Results
        rows_ingested = actual_inserted
        max_ts = df_insert["event_timestamp"].max()
        watermark = str(max_ts) if pd.notna(max_ts) else "unknown"

        end_time = datetime.now(timezone.utc)
        duration = (end_time - start_time).total_seconds()

        conn.execute(
            """
            UPDATE raw.pipeline_runs
            SET run_status = 'SUCCESS',
                end_time = ?,
                rows_ingested = ?,
                source_watermark = ?
            WHERE run_id = ?
            """,
            [end_time, rows_ingested, watermark, run_id]
        )

        logger.info("=" * 60)
        logger.info("Ingestion Complete")
        logger.info("  Rows attempted:  %s", f"{len(df_insert):,}")
        logger.info("  Rows inserted:   %s", f"{rows_ingested:,}")
        logger.info("  Duplicates:      %s", f"{duplicates:,}")
        logger.info("  Watermark:       %s", watermark)
        logger.info("  Duration:        %.2f seconds", duration)
        logger.info("=" * 60)

        conn.close()

        return {
            "run_id": run_id,
            "status": "SUCCESS",
            "rows_attempted": len(df_insert),
            "rows_ingested": rows_ingested,
            "duplicates_skipped": duplicates,
            "watermark": watermark,
            "duration_seconds": duration
        }

    except GDeltConnectionError:
        _record_failure(conn, run_id, "GDELT connection failed")
        raise
    except GDeltParseError:
        _record_failure(conn, run_id, "GDELT data parsing failed")
        raise
    except DatabaseWriteError:
        raise
    except Exception as e:
        logger.exception("Unexpected error during ingestion")
        _record_failure(conn, run_id, str(e))
        raise GDeltIngestionError(f"Unexpected error: {e}") from e


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
        result = ingest_events()
        sys.exit(0)
    except GDeltConnectionError as e:
        logger.error("Connection error: %s", e)
        sys.exit(1)
    except GDeltParseError as e:
        logger.error("Data parse error: %s", e)
        sys.exit(1)
    except DatabaseWriteError as e:
        logger.error("Database error: %s", e)
        sys.exit(1)
    except Exception as e:
        logger.exception("Fatal ingestion error")
        sys.exit(1)