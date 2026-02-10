import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional

import pandas as pd
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.operators.python import get_current_context

from mediatech_de_trial_task.serpapi_retry import fetch_with_retry, SerpApiPermanentError
from mediatech_de_trial_task.normalize import normalize_trends_response
from mediatech_de_trial_task.date_range import DateRange
from mediatech_de_trial_task.csv_writer import write_trends_csv
from mediatech_de_trial_task.validation import validate_trends_data
from mediatech_de_trial_task.types import (
    Json,
    SerpApiRequestParams,
    SerpApiResponse,
    TrendRecord,
    ValidationReport,
)

logger: logging.Logger = logging.getLogger(__name__)


@dag(
    dag_id="google_trends_dag",
    default_args={
        "owner": "AndriusD",
        "depends_on_past": False,
        "start_date": datetime(2024, 1, 1),
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    description="Fetch Google Trends data for search terms via SerpApi",
    schedule=timedelta(days=1),
    catchup=False,
    tags=["google_trends", "serpapi"],
)
def google_trends_dag() -> None:
    """Google Trends analysis DAG.

    Fetches trend data for security-related search terms via SerpApi,
    validates and persists the results to CSV, and produces a summary
    report.
    """

    @task
    def fetch_google_trends() -> Dict[str, List[TrendRecord]]:
        """Fetch Google Trends data for all search terms using SerpApi.

        Reads ``search_terms``, ``location``, and ``serpapi_key`` from
        Airflow Variables at runtime.

        Returns:
            A mapping of search term to its list of normalised record
            dicts.  Automatically pushed to XCom by TaskFlow.

        Raises:
            ValueError: If the ``serpapi_key`` Airflow Variable is not set.
        """
        search_terms: List[str] = Variable.get(
            "search_terms", deserialize_json=True,
        )
        location: str = Variable.get("location")
        api_key: str = Variable.get("serpapi_key")
        if not api_key:
            raise ValueError("Airflow Variable 'serpapi_key' is not set")

        all_results: Dict[str, List[TrendRecord]] = {}
        date_range: DateRange = DateRange(months=6)

        for search_term in search_terms:

            logger.info("Fetching Google Trends data for: %s", search_term)

            params: SerpApiRequestParams = {
                "engine": "google_trends",
                "q": search_term,
                "geo": location,
                "date": date_range.as_serpapi_date_string(),
                "data_type": "TIMESERIES",
                "api_key": api_key,
            }

            try:
                results: SerpApiResponse = fetch_with_retry(params)
            except SerpApiPermanentError as exc:
                logger.warning("Permanent API error for '%s', skipping: %s",
                               search_term, exc)
                all_results[search_term] = []
                continue

            records: List[TrendRecord] = normalize_trends_response(results, location)
            all_results[search_term] = records
            logger.info("Retrieved %d data points for '%s'",
                        len(records), search_term)

        return all_results

    @task
    def process_trends_data(
        raw_data: Dict[str, List[TrendRecord]],
    ) -> List[TrendRecord]:
        """Process and structure the Google Trends data.

        Flattens the per-term record lists into a single list, runs data
        validation, and writes results to CSV.  The validation report is
        pushed to XCom under the key ``validation_report``.

        Args:
            raw_data: Mapping of search term to normalised records,
                received automatically from the upstream TaskFlow task.

        Returns:
            A flat list of all processed trend record dicts.
            Automatically pushed to XCom by TaskFlow.

        Raises:
            ValueError: If *raw_data* is empty.
        """
        if not raw_data:
            raise ValueError("No raw data received from fetch_google_trends task")

        search_terms: List[str] = Variable.get(
            "search_terms", deserialize_json=True,
        )

        processed_records: List[TrendRecord] = []
        for records in raw_data.values():
            processed_records.extend(records)

        validation_report: ValidationReport = validate_trends_data(
            processed_records, search_terms,
        )
        context = get_current_context()
        context["ti"].xcom_push(key="validation_report", value=validation_report)

        df: pd.DataFrame = pd.DataFrame(processed_records)
        logger.info("Processed %d total records", len(df))
        logger.info("Sample data:\n%s", df.head(10))
        logger.info("Summary statistics by term:\n%s",
                     df.groupby("query")["extracted_value"].describe())

        write_trends_csv(processed_records)

        return processed_records

    @task
    def summarize_results(
        processed_data: List[TrendRecord],
    ) -> Optional[Json]:
        """Generate a summary of the Google Trends analysis.

        Computes aggregate statistics per search term and logs a
        formatted summary report.

        Args:
            processed_data: Flat list of trend record dicts, received
                automatically from the upstream TaskFlow task.

        Returns:
            A dict of summary statistics keyed by aggregation name, or
            ``None`` if *processed_data* is empty.
        """
        if not processed_data:
            logger.warning("No processed data available")
            return None

        search_terms: List[str] = Variable.get(
            "search_terms", deserialize_json=True,
        )
        location: str = Variable.get("location")

        df: pd.DataFrame = pd.DataFrame(processed_data)

        logger.info(
            "\n%s\n"
            "GOOGLE TRENDS ANALYSIS SUMMARY\n"
            "Region: %s\n"
            "Time Period: Last 6 months\n"
            "Search Terms: %s\n"
            "%s",
            "=" * 60, location, ", ".join(search_terms), "=" * 60,
        )

        summary: pd.DataFrame = df.groupby("query")["extracted_value"].agg(
            ["mean", "max", "min"],
        )
        summary = summary.round(2)
        logger.info("Average Search Interest by Term:\n%s", summary)

        avg_interest: pd.Series = (
            df.groupby("query")["extracted_value"]
            .mean()
            .sort_values(ascending=False)
        )
        logger.info("Ranking by Average Interest:\n%s", avg_interest)

        return summary.to_dict()

    raw: Dict[str, List[TrendRecord]] = fetch_google_trends()
    processed: List[TrendRecord] = process_trends_data(raw)
    summarize_results(processed)


google_trends_dag()
