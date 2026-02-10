import pytest

from mediatech_de_trial_task.normalize import normalize_trends_response


def _make_response(timeline_data=None, *, interest=True, created_at="2025-01-15 12:00:00 UTC"):
    """Build a minimal SerpApiResponse for testing."""
    resp = {"search_metadata": {"created_at": created_at}}
    if interest:
        iot = {}
        if timeline_data is not None:
            iot["timeline_data"] = timeline_data
        resp["interest_over_time"] = iot
    return resp


def _make_timeline_point(date="Jan 1 – 7, 2025", values=None):
    point = {"date": date}
    if values is not None:
        point["values"] = values
    return point


def _make_value(query="vpn", value="100", extracted_value=100):
    return {"query": query, "value": value, "extracted_value": extracted_value}


class TestNormalizeEmptyInputs:
    def test_empty_response(self):
        assert normalize_trends_response({}, "US") == []

    def test_missing_interest_over_time(self):
        assert normalize_trends_response({"other_key": 1}, "US") == []

    def test_empty_interest_over_time(self):
        resp = {"interest_over_time": {}}
        assert normalize_trends_response(resp, "US") == []

    def test_none_interest_over_time(self):
        resp = {"interest_over_time": None}
        assert normalize_trends_response(resp, "US") == []

    def test_missing_timeline_data(self):
        resp = {"interest_over_time": {"other": "stuff"}}
        assert normalize_trends_response(resp, "US") == []

    def test_empty_timeline_data(self):
        resp = _make_response(timeline_data=[])
        assert normalize_trends_response(resp, "US") == []


class TestNormalizeSingleRecord:
    def test_single_point_single_value(self):
        timeline = [_make_timeline_point(
            date="Jan 1 – 7, 2025",
            values=[_make_value("vpn", "80", 80)],
        )]
        resp = _make_response(timeline)
        records = normalize_trends_response(resp, "US")

        assert len(records) == 1
        rec = records[0]
        assert rec["query"] == "vpn"
        assert rec["location"] == "US"
        assert rec["date"] == "Jan 1 – 7, 2025"
        assert rec["value"] == "80"
        assert rec["extracted_value"] == 80
        assert rec["created_at"] == "2025-01-15 12:00:00 UTC"


class TestNormalizeMultipleValues:
    def test_multiple_values_in_one_point(self):
        timeline = [_make_timeline_point(
            date="Jan 1 – 7, 2025",
            values=[
                _make_value("vpn", "80", 80),
                _make_value("antivirus", "60", 60),
            ],
        )]
        resp = _make_response(timeline)
        records = normalize_trends_response(resp, "US")

        assert len(records) == 2
        queries = [r["query"] for r in records]
        assert "vpn" in queries
        assert "antivirus" in queries


class TestNormalizeMultiplePoints:
    def test_two_points_one_value_each(self):
        timeline = [
            _make_timeline_point("Week 1", [_make_value("vpn", "50", 50)]),
            _make_timeline_point("Week 2", [_make_value("vpn", "70", 70)]),
        ]
        resp = _make_response(timeline)
        records = normalize_trends_response(resp, "DE")

        assert len(records) == 2
        assert records[0]["date"] == "Week 1"
        assert records[1]["date"] == "Week 2"
        assert all(r["location"] == "DE" for r in records)

    def test_multiple_points_multiple_values(self):
        values = [_make_value("a", "1", 1), _make_value("b", "2", 2)]
        timeline = [
            _make_timeline_point("W1", values),
            _make_timeline_point("W2", values),
            _make_timeline_point("W3", values),
        ]
        resp = _make_response(timeline)
        records = normalize_trends_response(resp, "US")

        assert len(records) == 6  # 3 points * 2 values


class TestNormalizeDefaults:
    def test_missing_value_fields_use_defaults(self):
        timeline = [_make_timeline_point(
            date="Jan 1 – 7, 2025",
            values=[{}],
        )]
        resp = _make_response(timeline)
        records = normalize_trends_response(resp, "US")

        assert len(records) == 1
        rec = records[0]
        assert rec["query"] == ""
        assert rec["value"] == ""
        assert rec["extracted_value"] == 0

    def test_missing_date_defaults_to_empty(self):
        point = {"values": [_make_value()]}
        resp = _make_response([point])
        records = normalize_trends_response(resp, "US")

        assert records[0]["date"] == ""

    def test_point_without_values_key_produces_no_records(self):
        timeline = [{"date": "Jan 1 – 7, 2025"}]
        resp = _make_response(timeline)
        records = normalize_trends_response(resp, "US")

        assert records == []


class TestNormalizeLocation:
    @pytest.mark.parametrize("location", ["US", "DE", "United Kingdom", ""])
    def test_location_passed_through(self, location):
        timeline = [_make_timeline_point(values=[_make_value()])]
        resp = _make_response(timeline)
        records = normalize_trends_response(resp, location)

        assert all(r["location"] == location for r in records)
