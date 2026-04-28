"""
Data Quality Checks for RAW Layer
Runs automated quality checks on raw_events and raw_prices,
writing results to data_quality_metrics table.
"""

import duckdb
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional
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


# ---------------------------------------------------------------------------
# HELPERS
# ---------------------------------------------------------------------------

def get_project_root() -> Path:
    """Return geo-event-pipeline root directory."""
    return Path(__file__).resolve().parent.parent.parent


def insert_metric(
    conn: duckdb.DuckDBPyConnection,
    table_name: str,
    metric_name: str,
    metric_value: float,
    threshold_value: float,
    status: str,
    batch_id: Optional[str] = None
) -> None:
    """Insert a single quality metric row."""
    metric_id = str(uuid.uuid4())
    check_timestamp = datetime.now(timezone.utc)

    conn.execute(
        """
        INSERT INTO raw.data_quality_metrics
            (metric_id, check_timestamp, table_name, metric_name,
             metric_value, threshold_value, status, batch_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [metric_id, check_timestamp, table_name, metric_name,
         metric_value, threshold_value, status, batch_id]
    )


def determine_status(value: float, threshold: float, comparison: str) -> str:
    """
    Determine PASS / WARN / FAIL based on comparison type.

    Args:
        value: Actual metric value.
        threshold: Threshold to compare against.
        comparison: 'gt' (value > threshold is bad),
                    'lt' (value < threshold is bad),
                    'eq' (value == threshold is bad),
                    'lt_warn_gt_fail' (two-tier: WARN at threshold, FAIL at 2x)
    """
    if comparison == "gt":
        return "FAIL" if value > threshold else "PASS"
    elif comparison == "lt":
        return "FAIL" if value < threshold else "PASS"
    elif comparison == "eq":
        return "FAIL" if value == threshold else "PASS"
    elif comparison == "lt_warn_gt_fail":
        if value > threshold * 2:
            return "FAIL"
        elif value > threshold:
            return "WARN"
        else:
            return "PASS"
    return "PASS"


# ---------------------------------------------------------------------------
# CHECKS: RAW_EVENTS
# ---------------------------------------------------------------------------

def check_events_row_count(conn: duckdb.DuckDBPyConnection, batch_id: str = None) -> None:
    """Check that raw_events has rows."""
    result = conn.execute("SELECT COUNT(*) FROM raw.raw_events").fetchone()
    value = float(result[0])
    threshold = 0.0

    status = determine_status(value, threshold, "eq")
    logger.info("  raw_events.row_count: %s (status: %s)", int(value), status)

    insert_metric(conn, "raw_events", "row_count", value, threshold, status, batch_id)


def check_events_null_goldstein(conn: duckdb.DuckDBPyConnection, batch_id: str = None) -> None:
    """Check null percentage in goldstein_scale."""
    result = conn.execute(
        """
        SELECT
            CASE WHEN COUNT(*) = 0 THEN 0
            ELSE SUM(CASE WHEN goldstein_scale IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*)
            END AS null_pct
        FROM raw.raw_events
        """
    ).fetchone()
    value = round(float(result[0]), 2)
    threshold = 10.0  # WARN if > 10%, FAIL if > 20%

    status = determine_status(value, threshold, "lt_warn_gt_fail")
    logger.info("  raw_events.null_pct_goldstein_scale: %s%% (status: %s)", value, status)

    insert_metric(conn, "raw_events", "null_pct_goldstein_scale", value, threshold, status, batch_id)


def check_events_null_timestamp(conn: duckdb.DuckDBPyConnection, batch_id: str = None) -> None:
    """Check null percentage in event_timestamp."""
    result = conn.execute(
        """
        SELECT
            CASE WHEN COUNT(*) = 0 THEN 0
            ELSE SUM(CASE WHEN event_timestamp IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*)
            END AS null_pct
        FROM raw.raw_events
        """
    ).fetchone()
    value = round(float(result[0]), 2)
    threshold = 5.0

    status = determine_status(value, threshold, "lt_warn_gt_fail")
    logger.info("  raw_events.null_pct_event_timestamp: %s%% (status: %s)", value, status)

    insert_metric(conn, "raw_events", "null_pct_event_timestamp", value, threshold, status, batch_id)


def check_events_null_location(conn: duckdb.DuckDBPyConnection, batch_id: str = None) -> None:
    """Check null percentage in action_geo_lat (location data)."""
    result = conn.execute(
        """
        SELECT
            CASE WHEN COUNT(*) = 0 THEN 0
            ELSE SUM(CASE WHEN action_geo_lat IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*)
            END AS null_pct
        FROM raw.raw_events
        """
    ).fetchone()
    value = round(float(result[0]), 2)
    threshold = 30.0  # GDELT often has location gaps, so threshold is higher

    status = determine_status(value, threshold, "gt")
    logger.info("  raw_events.null_pct_action_geo_lat: %s%% (status: %s)", value, status)

    insert_metric(conn, "raw_events", "null_pct_action_geo_lat", value, threshold, status, batch_id)


def check_events_duplicates(conn: duckdb.DuckDBPyConnection, batch_id: str = None) -> None:
    """Check for duplicate event_ids (should never happen with PK)."""
    result = conn.execute(
        """
        SELECT COUNT(*) - COUNT(DISTINCT event_id) AS dupes
        FROM raw.raw_events
        """
    ).fetchone()
    value = float(result[0])
    threshold = 0.0

    status = determine_status(value, threshold, "gt")
    logger.info("  raw_events.duplicate_event_ids: %s (status: %s)", int(value), status)

    insert_metric(conn, "raw_events", "duplicate_event_ids", value, threshold, status, batch_id)


def check_events_freshness(conn: duckdb.DuckDBPyConnection, batch_id: str = None) -> None:
    """Check how many minutes since the latest event_timestamp."""
    result = conn.execute(
        """
        SELECT
            CASE WHEN MAX(event_timestamp) IS NULL THEN 99999
            ELSE DATE_DIFF('minute', MAX(event_timestamp), CURRENT_TIMESTAMP)
            END AS minutes_ago
        FROM raw.raw_events
        """
    ).fetchone()
    value = float(result[0])
    threshold = 60.0  # WARN if > 60 minutes stale

    status = determine_status(value, threshold, "gt")
    logger.info("  raw_events.freshness_minutes: %s (status: %s)", int(value), status)

    insert_metric(conn, "raw_events", "freshness_minutes", value, threshold, status, batch_id)


# ---------------------------------------------------------------------------
# CHECKS: RAW_PRICES
# ---------------------------------------------------------------------------

def check_prices_commodity_count(conn: duckdb.DuckDBPyConnection, batch_id: str = None) -> None:
    """Check that all 4 expected commodities have data."""
    # Get the most recent batch
    latest_batch = conn.execute(
        "SELECT batch_id FROM raw.raw_prices ORDER BY ingestion_timestamp DESC LIMIT 1"
    ).fetchone()
    latest_batch_id = latest_batch[0] if latest_batch else None

    if latest_batch_id:
        result = conn.execute(
            """
            SELECT COUNT(DISTINCT commodity_code)
            FROM raw.raw_prices
            WHERE batch_id = ?
            """,
            [latest_batch_id]
        ).fetchone()
    else:
        result = conn.execute(
            "SELECT COUNT(DISTINCT commodity_code) FROM raw.raw_prices"
        ).fetchone()

    value = float(result[0])
    threshold = 4.0  # We expect exactly 4 commodities

    status = determine_status(value, threshold, "lt")
    logger.info("  raw_prices.commodity_count: %s (status: %s)", int(value), status)

    insert_metric(conn, "raw_prices", "commodity_count", value, threshold, status, batch_id)


def check_prices_null_close(conn: duckdb.DuckDBPyConnection, batch_id: str = None) -> None:
    """Check null percentage in close prices."""
    result = conn.execute(
        """
        SELECT
            CASE WHEN COUNT(*) = 0 THEN 0
            ELSE SUM(CASE WHEN close IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*)
            END AS null_pct
        FROM raw.raw_prices
        """
    ).fetchone()
    value = round(float(result[0]), 2)
    threshold = 5.0  # FAIL if > 5% null close prices

    status = determine_status(value, threshold, "gt")
    logger.info("  raw_prices.null_pct_close: %s%% (status: %s)", value, status)

    insert_metric(conn, "raw_prices", "null_pct_close", value, threshold, status, batch_id)


def check_prices_freshness(conn: duckdb.DuckDBPyConnection, batch_id: str = None) -> None:
    """Check how many minutes since the latest price timestamp."""
    result = conn.execute(
        """
        SELECT
            CASE WHEN MAX(timestamp) IS NULL THEN 99999
            ELSE DATE_DIFF('minute', MAX(timestamp), CURRENT_TIMESTAMP)
            END AS minutes_ago
        FROM raw.raw_prices
        """
    ).fetchone()
    value = float(result[0])
    threshold = 90.0  # WARN if > 90 minutes stale (markets close)

    status = determine_status(value, threshold, "gt")
    logger.info("  raw_prices.freshness_minutes: %s (status: %s)", int(value), status)

    insert_metric(conn, "raw_prices", "freshness_minutes", value, threshold, status, batch_id)


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def run_all_checks(db_path: str = None, batch_id: str = None) -> dict:
    """
    Run all data quality checks and write results.

    Args:
        db_path: Path to DuckDB file.
        batch_id: Optional batch ID to associate checks with.

    Returns:
        dict with summary of PASS/WARN/FAIL counts.
    """
    root = get_project_root()
    if db_path is None:
        db_path = str(root / "data" / "warehouse.duckdb")

    logger.info("=" * 60)
    logger.info("Running Data Quality Checks")
    logger.info("=" * 60)

    conn = duckdb.connect(db_path)

    summary = {"PASS": 0, "WARN": 0, "FAIL": 0}

    # --- raw_events checks ---
    logger.info("raw_events checks:")
    for check_func in [
        check_events_row_count,
        check_events_null_goldstein,
        check_events_null_timestamp,
        check_events_null_location,
        check_events_duplicates,
        check_events_freshness,
    ]:
        try:
            check_func(conn, batch_id)
        except Exception as e:
            logger.error("  Check %s failed with error: %s", check_func.__name__, e)

    # --- raw_prices checks ---
    logger.info("raw_prices checks:")
    for check_func in [
        check_prices_commodity_count,
        check_prices_null_close,
        check_prices_freshness,
    ]:
        try:
            check_func(conn, batch_id)
        except Exception as e:
            logger.error("  Check %s failed with error: %s", check_func.__name__, e)

    # Count results
    results = conn.execute(
        """
        SELECT status, COUNT(*) as cnt
        FROM raw.data_quality_metrics
        WHERE check_timestamp > CURRENT_TIMESTAMP - INTERVAL '1 hour'
        GROUP BY status
        """
    ).fetchall()

    for status, cnt in results:
        summary[status] = cnt

    logger.info("=" * 60)
    logger.info("Quality Check Summary: %s PASS, %s WARN, %s FAIL",
                summary["PASS"], summary["WARN"], summary["FAIL"])
    logger.info("=" * 60)

    conn.close()
    return summary


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    setup_logging(level=logging.INFO)

    try:
        run_all_checks()
        sys.exit(0)
    except Exception as e:
        logger.exception("Data quality checks failed")
        sys.exit(1)