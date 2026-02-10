"""Retry wrapper for SerpApi calls with exponential backoff.

Provides per-call retry logic for transient API failures (rate limits,
timeouts, network blips) so that a single flaky call doesn't fail the
entire Airflow task.
"""

import logging
import random

from mediatech_de_trial_task.types import SerpApiRequestParams, SerpApiResponse
from requests.exceptions import ChunkedEncodingError, ConnectionError, Timeout
from serpapi import GoogleSearch
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

logger = logging.getLogger(__name__)


class SerpApiTransientError(Exception):
    """Retryable API error (429 rate-limit, 5xx server error)."""


class SerpApiPermanentError(Exception):
    """Non-retryable API error (bad API key, invalid params)."""


# HTTP status codes that are safe to retry.
_TRANSIENT_STATUS_CODES = {429, 500, 502, 503, 504}


def _check_response_for_errors(results: SerpApiResponse) -> None:
    """Inspect a SerpApi response dict and raise on errors.

    The ``google-search-results`` SDK does not always raise exceptions on
    HTTP errors — it may instead return a dict with an ``"error"`` key.
    This function translates those into the appropriate exception type so
    that tenacity can decide whether to retry.

    Args:
        results: The raw response dictionary from SerpApi.

    Raises:
        SerpApiTransientError: If the error is retryable (429, 5xx).
        SerpApiPermanentError: If the error is non-retryable (bad key, invalid params).
    """
    error = results.get("error")
    if error is None:
        return

    error_lower = str(error).lower()

    transient_keywords = [
        "rate limit",
        "too many requests",
        "429",
        "500",
        "502",
        "503",
        "504",
        "server error",
        "internal error",
        "temporarily unavailable",
        "timeout",
        "timed out",
    ]

    if any(kw in error_lower for kw in transient_keywords):
        raise SerpApiTransientError(error)

    raise SerpApiPermanentError(error)


def _add_jitter(retry_state):
    """Compute wait time: exponential backoff (2s..60s, x2) + 0-2s random jitter.

    Args:
        retry_state: The current retry state provided by tenacity.

    Returns:
        Wait time in seconds.
    """
    exp_wait = wait_exponential(multiplier=2, min=2, max=60)
    base = exp_wait(retry_state)
    return base + random.uniform(0, 2)


@retry(
    retry=retry_if_exception_type(
        (ConnectionError, Timeout, ChunkedEncodingError, SerpApiTransientError)
    ),
    stop=stop_after_attempt(5),
    wait=_add_jitter,
    reraise=True,
)
def fetch_with_retry(params: SerpApiRequestParams) -> SerpApiResponse:
    """Call SerpApi with automatic retries on transient failures.

    Args:
        params: Parameters forwarded to ``GoogleSearch(params).get_dict()``.

    Returns:
        The raw response dictionary from SerpApi.

    Raises:
        SerpApiPermanentError: Immediately on non-retryable errors (bad API key,
            invalid params).
        SerpApiTransientError: After all retry attempts are exhausted.
        ConnectionError: After all retry attempts are exhausted.
        Timeout: After all retry attempts are exhausted.
        ChunkedEncodingError: After all retry attempts are exhausted.
    """
    term = params.get("q", "<unknown>")
    logger.info("SerpApi request for '%s' (attempt will auto-retry "
                "on transient errors)", term)

    results = GoogleSearch(params).get_dict()
    _check_response_for_errors(results)

    return results
