"""Dynamic date range utilities for SerpApi query parameters."""

from datetime import date

from dateutil.relativedelta import relativedelta


class DateRange:
    """Dynamic date range computed at access time, not instantiation.

    Stores a ``relativedelta`` offset and recomputes start/end dates
    from ``date.today()`` on every property access, so the range never
    goes stale across long-running processes.

    Args:
        **kwargs: Keyword arguments forwarded to
            :class:`dateutil.relativedelta.relativedelta`
            (e.g. ``months=6``, ``days=30``, ``years=1``).

    Example::

        dr = DateRange(months=6)
        dr.as_serpapi_date_string()  # "2025-08-10 2026-02-10"
    """

    def __init__(self, **kwargs: int) -> None:
        """Initialise with a relativedelta offset.

        Args:
            **kwargs: Keyword arguments forwarded to
                ``relativedelta`` (e.g. ``months=6``).
        """
        self._delta: relativedelta = relativedelta(**kwargs)

    @property
    def end_date(self) -> str:
        """Return today's date formatted as ``YYYY-MM-DD``.

        Returns:
            Today's date string.
        """
        return date.today().strftime("%Y-%m-%d")

    @property
    def start_date(self) -> str:
        """Return the start date (today minus the stored offset).

        Returns:
            Start date string formatted as ``YYYY-MM-DD``.
        """
        return (date.today() - self._delta).strftime("%Y-%m-%d")

    def as_serpapi_date_string(self) -> str:
        """Format the range for the SerpApi ``date`` parameter.

        Returns:
            A string in the form ``"YYYY-MM-DD YYYY-MM-DD"``
            (start date followed by end date, space-separated).
        """
        return f"{self.start_date} {self.end_date}"
