"""Shared type aliases and typed dictionaries for the mediatech_de_trial_task package."""

from typing import Any, Dict, List, TypedDict

Json = Dict[str, Any]
"""A JSON-like dictionary with string keys and arbitrary values."""


class SerpApiRequestParams(TypedDict, total=False):
    """Parameters sent to SerpApi's Google Trends engine."""

    engine: str
    q: str
    geo: str
    date: str
    data_type: str
    api_key: str


class SearchMetadata(TypedDict, total=False):
    """Metadata about the SerpApi search request."""

    created_at: str


class SearchParameters(TypedDict, total=False):
    """Echoed search parameters returned by SerpApi."""

    date: str
    data_type: str


class TimelineValue(TypedDict, total=False):
    """A single value entry inside a timeline data point."""

    query: str
    value: str
    extracted_value: int


class TimelinePoint(TypedDict, total=False):
    """A single timeline data point from Google Trends."""

    date: str
    timestamp: str
    values: List["TimelineValue"]


class InterestOverTime(TypedDict, total=False):
    """The interest_over_time section of a Google Trends response."""

    timeline_data: List["TimelinePoint"]


class SerpApiResponse(TypedDict, total=False):
    """Top-level SerpApi Google Trends response."""

    search_metadata: SearchMetadata
    search_parameters: SearchParameters
    interest_over_time: InterestOverTime
    error: str


class TrendRecord(TypedDict):
    """A single flattened trend data point."""

    query: str
    location: str
    date: str
    value: str
    extracted_value: int
    created_at: str


class ValidationReport(TypedDict):
    """Report produced by validate_trends_data."""

    record_count: int
    schema_errors: List[str]
    coverage_gaps: List[str]
    anomalies: List[str]
    valid: bool
