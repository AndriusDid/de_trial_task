"""mediatech_de_trial_task — Google Trends pipeline utilities.

Logging
-------
All package modules log via ``logging.getLogger(__name__)``.  A
:class:`~logging.NullHandler` is attached to the package root logger so
that log messages are silently discarded unless the calling application
(e.g. Airflow, a test runner, or a script) configures its own handlers.

For quick console output outside of Airflow, call :func:`setup_logging`::

    import mediatech_de_trial_task
    mediatech_de_trial_task.setup_logging()          # INFO to stderr
    mediatech_de_trial_task.setup_logging("DEBUG")   # more verbose
"""

import logging

logger: logging.Logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


def setup_logging(level: str = "INFO") -> None:
    """Configure the package logger with a console handler.

    Intended for standalone scripts, notebooks, or test sessions where
    Airflow's logging infrastructure is not available.  In an Airflow
    context the task logger already captures output from the package
    loggers, so calling this function is unnecessary.

    Args:
        level: Logging level name (e.g. ``"DEBUG"``, ``"INFO"``,
            ``"WARNING"``).  Defaults to ``"INFO"``.
    """
    pkg_logger = logging.getLogger(__name__)
    pkg_logger.setLevel(getattr(logging, level.upper(), logging.INFO))

    if not any(
        isinstance(h, logging.StreamHandler)
        and not isinstance(h, logging.NullHandler)
        for h in pkg_logger.handlers
    ):
        handler = logging.StreamHandler()
        handler.setFormatter(
            logging.Formatter(
                fmt="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )
        )
        pkg_logger.addHandler(handler)
