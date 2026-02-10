"""Flatten raw SerpApi Google Trends responses into uniform records."""

from datetime import datetime, timezone
from typing import List

from mediatech_de_trial_task.types import (
    SerpApiResponse,
    TrendRecord,
)

_SERPAPI_FORMAT = "%Y-%m-%d %H:%M:%S %Z"
_OUTPUT_FORMAT = "%Y-%m-%d %H:%M:%S%z"


def normalize_trends_response(response: SerpApiResponse,
                              location: str) -> List[TrendRecord]:
    """Flatten a raw SerpApi Google Trends response into flat records.

    Each returned record contains search metadata and
    one timeline data point with its associated value.

    Args:
        response: Raw JSON response dict from SerpApi's Google Trends
            engine.
        location: a region or location where the search key has been used.
        Values approved by SerpAPI need to be used.

    Returns:
        A list of flat dicts, one per (timeline-point, value) pair.
        Returns an empty list if ``interest_over_time`` or
        ``timeline_data`` is missing from *response*.
    """
    interest = response.get("interest_over_time")
    if not interest:
        return []

    timeline_data = interest.get("timeline_data")
    if not timeline_data:
        return []

    metadata = response.get("search_metadata") or {}
    created_at: str = _format_datetime(metadata.get("created_at", ""))

    records: List[TrendRecord] = []
    for point in timeline_data:
        date: str = _format_timestamp(point.get("timestamp", ""))
        for val in point.get("values", []):
            records.append({
                "query": val.get("query", ""),
                "location": location,
                "date": date,
                "value": val.get("value", ""),
                "extracted_value": val.get("extracted_value", 0),
                "created_at": created_at,
            })

    return records


def _format_timestamp(raw: str) -> str:
    """Convert a Unix timestamp string to ``YYYY-MM-DD HH:MM:SS+TZ`` format.

    Args:
        raw: Unix epoch timestamp as a string.

    Returns:
        Formatted datetime string, or the original value on failure.
    """
    if not raw:
        return ""
    try:
        dt = datetime.fromtimestamp(int(raw), tz=timezone.utc)
        return dt.strftime(_OUTPUT_FORMAT)
    except (ValueError, OSError):
        return raw


def _format_datetime(raw: str) -> str:
    """Parse a datetime string and return it in ``YYYY-MM-DD HH:MM:SS+TZ`` format.
    Falls back to the original string when parsing fails.

    Args:
        raw: Raw datetime string.

    Returns:
        Parsed datetime string.
    """
    if not raw:
        return ""
    try:
        dt = datetime.strptime(raw, _SERPAPI_FORMAT)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.strftime(_OUTPUT_FORMAT)
    except ValueError:
        return raw
