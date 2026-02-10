"""Flatten raw SerpApi Google Trends responses into uniform records."""

from typing import List

from mediatech_de_trial_task.types import (
    SerpApiResponse,
    TrendRecord,
)


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
    created_at: str = metadata.get("created_at", "")

    records: List[TrendRecord] = []
    for point in timeline_data:
        date: str = point.get("date", "")
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