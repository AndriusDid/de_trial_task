import pytest

from mediatech_de_trial_task.validation import (
    EXPECTED_SCHEMA,
    validate_trends_data,
)
from mediatech_de_trial_task.types import TrendRecord


def _record(**overrides) -> TrendRecord:
    """Build a valid TrendRecord with sensible defaults."""
    base = {
        "query": "vpn",
        "location": "US",
        "date": "Jan 1 – 7, 2025",
        "value": "80",
        "extracted_value": 80,
        "created_at": "2025-01-15 12:00:00 +0000",
    }
    base.update(overrides)
    return base


def _records_for_terms(terms, n_dates=3):
    """Create records covering *n_dates* dates for every term in *terms*."""
    recs = []
    for i in range(n_dates):
        for term in terms:
            recs.append(_record(
                query=term,
                date=f"Week {i + 1}",
                value=str(50 + i * 10),
                extracted_value=50 + i * 10,
            ))
    return recs


class TestExpectedSchema:
    def test_schema_keys_match_trend_record_fields(self):
        assert set(EXPECTED_SCHEMA) == set(TrendRecord.__annotations__)

    def test_extracted_value_is_numeric(self):
        assert EXPECTED_SCHEMA["extracted_value"] == "numeric"

    def test_string_fields(self):
        for field in TrendRecord.__annotations__:
            if field != "extracted_value":
                assert EXPECTED_SCHEMA[field] == "string"


class TestValidateEmpty:
    def test_empty_records_invalid(self):
        report = validate_trends_data([], ["vpn"])
        assert report["valid"] is False
        assert report["record_count"] == 0

    def test_empty_records_returns_all_report_keys(self):
        report = validate_trends_data([], [])
        assert "record_count" in report
        assert "schema_errors" in report
        assert "coverage_gaps" in report
        assert "anomalies" in report
        assert "valid" in report


class TestSchemaValidation:
    def test_valid_schema_passes(self):
        terms = ["vpn"]
        records = _records_for_terms(terms)
        report = validate_trends_data(records, terms)
        assert report["schema_errors"] == []

    def test_extra_column_flagged(self):
        records = [_record()]
        records[0]["bonus_col"] = "oops"
        report = validate_trends_data(records, ["vpn"])
        assert report["valid"] is False
        assert any("Unexpected columns" in e for e in report["schema_errors"])

    def test_missing_column_flagged(self):
        records = [{"query": "vpn", "location": "US", "date": "W1"}]
        report = validate_trends_data(records, ["vpn"])
        assert report["valid"] is False
        assert any("Missing columns" in e for e in report["schema_errors"])


class TestSearchTermCoverage:
    def test_full_coverage_no_gaps(self):
        terms = ["vpn", "antivirus"]
        records = _records_for_terms(terms)
        report = validate_trends_data(records, terms)
        assert report["coverage_gaps"] == []

    def test_missing_term_at_some_dates(self):
        terms = ["vpn", "antivirus"]
        records = _records_for_terms(["vpn"], n_dates=2)
        report = validate_trends_data(records, terms)
        assert report["valid"] is False
        assert len(report["coverage_gaps"]) > 0
        assert any("antivirus" in g for g in report["coverage_gaps"])


class TestNullValueAnomalies:
    def test_null_value_flagged(self):
        records = [_record(value=None)]
        report = validate_trends_data(records, ["vpn"])
        assert report["valid"] is False
        assert any("null/empty" in a for a in report["anomalies"])

    def test_empty_string_value_flagged(self):
        records = [_record(query="")]
        report = validate_trends_data(records, ["vpn"])
        assert report["valid"] is False
        assert any("null/empty" in a for a in report["anomalies"])


class TestZeroExtractedValue:
    def test_zero_extracted_value_logged_but_still_valid(self):
        records = [_record(extracted_value=0)]
        # Zero values append an anomaly but do NOT set valid=False.
        report = validate_trends_data(records, ["vpn"])
        assert any("zero extracted_value" in a for a in report["anomalies"])

    def test_nonzero_extracted_value_no_anomaly(self):
        records = [_record(extracted_value=42)]
        report = validate_trends_data(records, ["vpn"])
        zero_msgs = [a for a in report["anomalies"] if "zero extracted_value" in a]
        assert zero_msgs == []


class TestFullyValidData:
    def test_all_checks_pass(self):
        terms = ["vpn", "antivirus", "ad blocker"]
        records = _records_for_terms(terms, n_dates=5)
        report = validate_trends_data(records, terms)
        assert report["valid"] is True
        assert report["schema_errors"] == []
        assert report["coverage_gaps"] == []
        # Only possible anomaly is zero extracted_value (soft warning)
        hard_anomalies = [
            a for a in report["anomalies"]
            if "zero extracted_value" not in a
        ]
        assert hard_anomalies == []

    def test_record_count_matches_input(self):
        terms = ["vpn"]
        records = _records_for_terms(terms, n_dates=4)
        report = validate_trends_data(records, terms)
        assert report["record_count"] == 4


class TestEdgeCases:
    def test_single_record_valid(self):
        records = [_record()]
        report = validate_trends_data(records, ["vpn"])
        assert report["record_count"] == 1

    def test_unknown_expected_term_causes_coverage_gap(self):
        records = [_record(query="vpn")]
        report = validate_trends_data(records, ["vpn", "firewall"])
        assert report["valid"] is False
        assert any("firewall" in g for g in report["coverage_gaps"])

    def test_extra_query_not_in_expected_is_fine(self):
        records = [_record(query="vpn"), _record(query="bonus")]
        report = validate_trends_data(records, ["vpn"])
        # "bonus" is extra but coverage only checks expected terms
        assert report["coverage_gaps"] == []
