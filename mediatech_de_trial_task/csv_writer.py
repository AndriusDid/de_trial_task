"""Write and deduplicate trend records to CSV files."""

import logging
from pathlib import Path
from typing import List

import pandas as pd

from mediatech_de_trial_task.types import TrendRecord

logger = logging.getLogger(__name__)

DEFAULT_OUTPUT_PATH: Path = Path("/opt/airflow/output/trends.csv")


def write_trends_csv(
    records: List[TrendRecord],
    output_path: Path = DEFAULT_OUTPUT_PATH,
) -> None:
    """Write trend records to CSV with idempotent deduplication.

    If *output_path* already exists the new records are concatenated with
    the existing data and duplicates (keyed on ``query`` + ``date``)
    are dropped, keeping the **last** occurrence.  If the file does not
    exist it is created along with any missing parent directories.

    An empty *records* list is a no-op (a warning is logged).

    Args:
        records: Flat trend record dicts to persist.
        output_path: Destination CSV path.  Defaults to
            ``/opt/airflow/output/trends.csv``.
    """
    if not records:
        logger.warning("No records to write — skipping CSV output.")
        return

    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    new_df = pd.DataFrame(records)

    if output_path.exists():
        existing_df = pd.read_csv(output_path)
        combined = pd.concat([existing_df, new_df], ignore_index=True)
        combined = combined.sort_values("created_at")
        combined = combined.drop_duplicates(subset=["query", "date", "location"], keep="last")
        combined.to_csv(output_path, index=False)
    else:
        new_df.to_csv(output_path, index=False)

    logger.info("Wrote %d records to %s", len(pd.read_csv(output_path)), output_path)
