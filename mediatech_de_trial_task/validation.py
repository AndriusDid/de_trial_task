"""Validate trend records for schema consistency, coverage, and anomalies."""

import logging
from datetime import datetime, timezone
from typing import Dict, List

import pandas as pd

from mediatech_de_trial_task.types import TrendRecord, ValidationReport

logger = logging.getLogger(__name__)

_NUMERIC_FIELDS: frozenset = frozenset({"extracted_value"})

EXPECTED_SCHEMA: Dict[str, str] = {
    field: "numeric" if field in _NUMERIC_FIELDS else "string"
    for field in TrendRecord.__annotations__
}


def validate_trends_data(
    records: List[TrendRecord],
    expected_search_terms: List[str],
) -> ValidationReport:
    """Validate trend records and log any anomalies found.

    Runs three categories of checks against the supplied records:

    1. **Schema consistency** -- expected columns present, no extras,
       column dtypes match expectations.
    2. **Search-term coverage** -- every expected term appears for every
       unique timestamp.
    3. **Anomaly detection** -- null/empty values, zero
       ``extracted_value``, and gaps in the timestamp timeline.

    Args:
        records: Flat trend records produced by
            :func:`~mediatech_de_trial_task.normalize.normalize_trends_response`.
        expected_search_terms: Search terms that should appear at every
            timestamp (e.g. ``["vpn", "antivirus", ...]``).

    Returns:
        A report dict with keys ``record_count`` (int),
        ``schema_errors`` (list[str]), ``coverage_gaps`` (list[str]),
        ``anomalies`` (list[str]), and ``valid`` (bool).
    """
    report: ValidationReport = {
        "record_count": len(records),
        "schema_errors": [],
        "coverage_gaps": [],
        "anomalies": [],
        "valid": True,
    }

    if not records:
        logger.warning("Validation: no records to validate.")
        report["valid"] = False
        return report

    df = pd.DataFrame(records)

    _check_schema(df, report)
    _check_search_term_coverage(df, expected_search_terms, report)
    _check_anomalies(df, report)

    if report["valid"]:
        logger.info("Validation passed — %d records, no issues found.",
                    len(df))
    else:
        total = (
            len(report["schema_errors"])
            + len(report["coverage_gaps"])
            + len(report["anomalies"])
        )
        logger.warning("Validation found %d issue(s). "
                       "See report for details.", total)

    return report


def _check_schema(df: pd.DataFrame, report: ValidationReport):
    """Verify column presence and dtype consistency.

    Compares the DataFrame columns against :data:`EXPECTED_SCHEMA` and
    appends error strings to ``report["schema_errors"]`` for any
    mismatches.

    Args:
        df: DataFrame built from the trend records.
        report: Mutable validation report dict to update in-place.
    """
    expected_cols = set(EXPECTED_SCHEMA)
    actual_cols = set(df.columns)

    missing = expected_cols - actual_cols
    extra = actual_cols - expected_cols

    if missing:
        msg = f"Missing columns: {sorted(missing)}"
        logger.warning("Schema: %s", msg)
        report["schema_errors"].append(msg)
        report["valid"] = False

    if extra:
        msg = f"Unexpected columns: {sorted(extra)}"
        logger.warning("Schema: %s", msg)
        report["schema_errors"].append(msg)
        report["valid"] = False

    for col, expected_kind in EXPECTED_SCHEMA.items():
        if col not in actual_cols:
            continue
        if (expected_kind == "numeric" and
                not pd.api.types.is_numeric_dtype(df[col])):
            msg = f"Column '{col}' expected numeric, got {df[col].dtype}"
            logger.warning("Schema: %s", msg)
            report["schema_errors"].append(msg)
            report["valid"] = False
        elif (expected_kind == "string" and
              not pd.api.types.is_string_dtype(df[col])):
            msg = f"Column '{col}' expected string, got {df[col].dtype}"
            logger.warning("Schema: %s", msg)
            report["schema_errors"].append(msg)
            report["valid"] = False


def _check_search_term_coverage(
    df: pd.DataFrame,
    expected_search_terms: List[str],
    report: ValidationReport,
):
    """Ensure every expected search term is present at every date.

    For each unique ``date`` in *df*, checks that all entries in
    *expected_search_terms* appear in the ``query`` column.  Missing
    terms are appended to ``report["coverage_gaps"]``.

    Args:
        df: DataFrame built from the trend records.
        expected_search_terms: Search terms that must appear at each
            date.
        report: Mutable validation report dict to update in-place.
    """
    if "query" not in df.columns or "date" not in df.columns:
        return

    expected = set(expected_search_terms)
    for dt, group in df.groupby("date"):
        present = set(group["query"])
        missing = expected - present
        if missing:
            msg = f"Date {dt}: missing search terms {sorted(missing)}"
            logger.warning("Coverage: %s", msg)
            report["coverage_gaps"].append(msg)
            report["valid"] = False


def _check_anomalies(df: pd.DataFrame, report: ValidationReport):
    """Run all anomaly sub-checks on the DataFrame.

    Delegates to :func:`_check_null_values`,
    :func:`_check_zero_extracted_values`,
    and :func:`_check_empty_date_range_gaps`.

    Args:
        df: DataFrame built from the trend records.
        report: Mutable validation report dict to update in-place.
    """
    _check_null_values(df, report)
    _check_zero_extracted_values(df, report)
    _check_empty_date_range_gaps(df, report)


def _check_null_values(df: pd.DataFrame, report: ValidationReport):
    """Detect null or empty-string values in every column.

    For each column, counts ``NaN`` values plus (for string columns)
    whitespace-only / empty strings.  Any hits are appended to
    ``report["anomalies"]``.

    Args:
        df: DataFrame built from the trend records.
        report: Mutable validation report dict to update in-place.
    """
    for col in df.columns:
        null_count = df[col].isna().sum()
        empty_count = ((df[col].astype(str).str.strip() == "").sum()
                       if pd.api.types.is_string_dtype(df[col]) else 0)
        total = null_count + empty_count
        if total > 0:
            msg = f"Column '{col}' has {total} null/empty value(s)"
            logger.warning("Anomaly: %s", msg)
            report["anomalies"].append(msg)
            report["valid"] = False


def _check_zero_extracted_values(
    df: pd.DataFrame,
    report: ValidationReport,
):
    """Flag records where ``extracted_value`` is zero.

    Zero interest values may be legitimate (very low search volume) so
    this check logs a warning and appends to ``report["anomalies"]``
    but does **not** set ``report["valid"]`` to ``False``.

    Args:
        df: DataFrame built from the trend records.
        report: Mutable validation report dict to update in-place.
    """
    if "extracted_value" not in df.columns:
        return
    zero_count = (df["extracted_value"] == 0).sum()
    if zero_count > 0:
        msg = f"{zero_count} record(s) with zero extracted_value"
        logger.warning("Anomaly: %s", msg)
        report["anomalies"].append(msg)


def _check_empty_date_range_gaps(df: pd.DataFrame, report: ValidationReport) -> None:
    """Detect empty gaps at the edges of the requested date range.

    Parses the ``date_range`` column (format ``"YYYY-MM-DD YYYY-MM-DD"``)
    to determine the requested time window, then checks whether the actual
    data timestamps cover that window.  Flags gaps where the first data
    point starts significantly after the requested start or the last data
    point ends significantly before the requested end.

    Also checks per-query coverage: if any individual search term is
    missing data for a substantial portion of the overall timeline, that
    is flagged as well.

    Args:
        df: DataFrame built from the trend records.
        report: Mutable validation report dict to update in-place.
    """
    if "timestamp" not in df.columns or "date_range" not in df.columns:
        return

    timestamps = pd.to_numeric(df["timestamp"], errors="coerce").dropna()
    if timestamps.empty:
        return

    actual_min = timestamps.min()
    actual_max = timestamps.max()

    # Parse the requested date range from the first non-empty value.
    date_range_vals = df["date_range"].dropna().unique()
    if len(date_range_vals) == 0:
        return

    requested_start_ts = None
    requested_end_ts = None
    for dr_val in date_range_vals:
        parts = str(dr_val).strip().split()
        if len(parts) == 2:
            try:
                start_dt = datetime.strptime(parts[0], "%Y-%m-%d").replace(
                    tzinfo=timezone.utc,
                )
                end_dt = datetime.strptime(parts[1], "%Y-%m-%d").replace(
                    tzinfo=timezone.utc,
                )
                requested_start_ts = start_dt.timestamp()
                requested_end_ts = end_dt.timestamp()
                break
            except ValueError:
                continue

    if requested_start_ts is None or requested_end_ts is None:
        return

    total_range = requested_end_ts - requested_start_ts
    if total_range <= 0:
        return

    # Allow a tolerance of 5% of the total range for edge gaps.
    tolerance = total_range * 0.05

    start_gap = actual_min - requested_start_ts
    if start_gap > tolerance:
        gap_days = int(start_gap / 86400)
        msg = (
            f"Empty data range gap at start: data begins {gap_days} day(s) "
            f"after requested start date"
        )
        logger.warning("Anomaly: %s", msg)
        report["anomalies"].append(msg)
        report["valid"] = False

    end_gap = requested_end_ts - actual_max
    if end_gap > tolerance:
        gap_days = int(end_gap / 86400)
        msg = (
            f"Empty data range gap at end: data ends {gap_days} day(s) "
            f"before requested end date"
        )
        logger.warning("Anomaly: %s", msg)
        report["anomalies"].append(msg)
        report["valid"] = False

    # Per-query coverage check: flag terms missing data for >10% of
    # the overall timeline span.
    if "query" not in df.columns:
        return
